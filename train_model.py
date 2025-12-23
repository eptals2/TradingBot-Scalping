# train_model.py (FIXED - handle cases with few samples and optional ticks)
import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.model_selection import TimeSeriesSplit
from sklearn.metrics import classification_report, accuracy_score, confusion_matrix, roc_auc_score
import joblib
from datetime import datetime
import os

from utils.sqlite_store import get_all_candles, get_all_ticks, get_db_stats
from features.combined_features import prepare_ml_dataset

def train_scalping_model(
    target_type='classification',
    pips_target=5,
    periods_ahead=1,
    model_type='rf',
    use_ticks=True,
    min_samples=100
):
    """
    Train scalping model
    
    Args:
        target_type: 'classification', 'regression', or 'direction'
        pips_target: minimum pips for profitable trade (classification only)
        periods_ahead: how many periods to predict ahead
        model_type: 'rf' (Random Forest) or 'gb' (Gradient Boosting)
        use_ticks: whether to include tick features (will train without if not enough)
        min_samples: minimum samples needed for training
    """
    
    print("="*60)
    print("XAUUSD SCALPING MODEL TRAINING")
    print("="*60)
    
    # Check data availability
    stats = get_db_stats()
    print(f"\nDatabase Stats:")
    print(f"  Candles: {stats['candles']:,}")
    print(f"  Ticks: {stats['ticks']:,}")
    print(f"  DB Size: {stats['db_size_mb']} MB")
    print(f"  Candle range: {stats['candle_range']}")
    print(f"  Tick range: {stats['tick_range']}")
    
    if stats['candles'] < 100:
        print("\n⚠️  ERROR: Not enough candles for training (need at least 100)")
        print("    Let the data collector run for a few more hours")
        return None, None
    
    # Load data
    print("\nLoading data...")
    candles = get_all_candles()
    
    # Try with ticks first, then without if not enough data
    ticks = None
    if use_ticks and stats['ticks'] > 100:
        ticks = get_all_ticks()
        print(f"Loaded {len(candles)} candles and {len(ticks)} ticks")
    else:
        print(f"Loaded {len(candles)} candles (training WITHOUT tick features)")
        use_ticks = False
    
    # Prepare features
    print("\nPreparing features...")
    try:
        X, y, timestamps, full_df = prepare_ml_dataset(
            candles,
            ticks,
            target_type=target_type,
            pips_target=pips_target,
            periods_ahead=periods_ahead
        )
    except Exception as e:
        print(f"\n⚠️  ERROR preparing features: {e}")
        print("    Retrying WITHOUT tick features...")
        X, y, timestamps, full_df = prepare_ml_dataset(
            candles,
            None,  # No ticks
            target_type=target_type,
            pips_target=pips_target,
            periods_ahead=periods_ahead
        )
        use_ticks = False
    
    print(f"\nDataset prepared:")
    print(f"  Samples: {len(X):,}")
    print(f"  Features: {len(X.columns)}")
    print(f"  Using tick features: {'Yes' if use_ticks else 'No'}")
    print(f"  Date range: {timestamps.min()} to {timestamps.max()}")
    
    if len(X) < min_samples:
        print(f"\n⚠️  WARNING: Only {len(X)} samples (need at least {min_samples})")
        print(f"    Training anyway, but expect poor results.")
        print(f"    Let data collector run for more time.")
        
        if len(X) < 50:
            print(f"\n⚠️  ERROR: Too few samples ({len(X)}) - cannot train")
            return None, None
    
    print(f"\nTarget distribution:")
    print(y.value_counts())
    print(f"Positive class: {(y.sum() / len(y) * 100):.2f}%")
    
    # Adjust n_splits based on sample size
    n_splits = min(5, len(X) // 20)  # At least 20 samples per fold
    if n_splits < 2:
        print(f"\n⚠️  Not enough data for cross-validation. Using simple train/test split.")
        # Simple 80/20 split
        split_idx = int(len(X) * 0.8)
        train_idx = list(range(split_idx))
        test_idx = list(range(split_idx, len(X)))
        splits = [(train_idx, test_idx)]
    else:
        tscv = TimeSeriesSplit(n_splits=n_splits)
        splits = list(tscv.split(X))
    
    print("\n" + "="*60)
    print("TRAINING WITH TIME SERIES CROSS-VALIDATION")
    print("="*60)
    print(f"Number of folds: {len(splits)}")
    
    scores = []
    best_model = None
    best_score = 0
    
    for fold, (train_idx, test_idx) in enumerate(splits):
        print(f"\n--- Fold {fold + 1}/{len(splits)} ---")
        
        X_train, X_test = X.iloc[train_idx], X.iloc[test_idx]
        y_train, y_test = y.iloc[train_idx], y.iloc[test_idx]
        
        print(f"Train: {len(X_train)} samples | Test: {len(X_test)} samples")
        
        # Select model
        if model_type == 'rf':
            model = RandomForestClassifier(
                n_estimators=100,
                max_depth=15,
                min_samples_split=max(2, len(X_train) // 100),
                min_samples_leaf=max(1, len(X_train) // 200),
                max_features='sqrt',
                random_state=42,
                n_jobs=-1,
                class_weight='balanced'
            )
        elif model_type == 'gb':
            model = GradientBoostingClassifier(
                n_estimators=100,
                max_depth=5,
                learning_rate=0.1,
                min_samples_split=max(2, len(X_train) // 100),
                random_state=42
            )
        
        # Train
        model.fit(X_train, y_train)
        
        # Evaluate
        y_pred = model.predict(X_test)
        y_pred_proba = model.predict_proba(X_test)[:, 1] if hasattr(model, 'predict_proba') else None
        
        acc = accuracy_score(y_test, y_pred)
        scores.append(acc)
        
        print(f"Accuracy: {acc:.4f}")
        
        if y_pred_proba is not None and len(np.unique(y_test)) > 1:
            try:
                auc = roc_auc_score(y_test, y_pred_proba)
                print(f"ROC-AUC: {auc:.4f}")
            except:
                pass
        
        # Save best model
        if acc > best_score:
            best_score = acc
            best_model = model
    
    print("\n" + "="*60)
    print("FINAL RESULTS")
    print("="*60)
    print(f"Average Accuracy: {np.mean(scores):.4f} (+/- {np.std(scores):.4f})")
    print(f"Best Fold Accuracy: {best_score:.4f}")
    
    # Final evaluation on last fold
    print("\n--- Test Set Performance (Last Fold) ---")
    y_pred = best_model.predict(X_test)
    print("\nClassification Report:")
    try:
        print(classification_report(y_test, y_pred, target_names=['No Trade', 'Trade']))
    except:
        print(classification_report(y_test, y_pred))
    
    print("\nConfusion Matrix:")
    cm = confusion_matrix(y_test, y_pred)
    print(cm)
    if cm.shape == (2, 2):
        print(f"True Negatives: {cm[0,0]} | False Positives: {cm[0,1]}")
        print(f"False Negatives: {cm[1,0]} | True Positives: {cm[1,1]}")
    
    # Feature importance
    print("\n" + "="*60)
    print("TOP 20 FEATURE IMPORTANCES")
    print("="*60)
    
    feature_importance = pd.DataFrame({
        'feature': X.columns,
        'importance': best_model.feature_importances_
    }).sort_values('importance', ascending=False)
    
    print(feature_importance.head(20).to_string(index=False))
    
    # Save model
    os.makedirs('models', exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    model_filename = f'models/scalping_model_{model_type}_{timestamp}.pkl'
    joblib.dump(best_model, model_filename)
    
    # Save feature columns
    feature_cols_filename = f'models/feature_columns_{timestamp}.pkl'
    joblib.dump(X.columns.tolist(), feature_cols_filename)
    
    print(f"\n✓ Model saved: {model_filename}")
    print(f"✓ Features saved: {feature_cols_filename}")
    
    # Save training metadata
    metadata = {
        'timestamp': timestamp,
        'model_type': model_type,
        'target_type': target_type,
        'pips_target': pips_target,
        'periods_ahead': periods_ahead,
        'use_ticks': use_ticks,
        'n_samples': len(X),
        'n_features': len(X.columns),
        'accuracy': best_score,
        'avg_accuracy': np.mean(scores),
        'std_accuracy': np.std(scores),
        'feature_list': X.columns.tolist()
    }
    
    import json
    metadata_filename = f'models/metadata_{timestamp}.json'
    with open(metadata_filename, 'w') as f:
        json.dump(metadata, f, indent=2)
    
    print(f"✓ Metadata saved: {metadata_filename}")
    
    return best_model, feature_importance


def quick_train():
    """Quick training with default parameters"""
    return train_scalping_model(
        target_type='classification',
        pips_target=5,
        periods_ahead=1,
        model_type='rf',
        use_ticks=True,  # Will automatically fallback to candles-only if needed
        min_samples=50   # Lower threshold for testing
    )


if __name__ == "__main__":
    # Train the model
    result = quick_train()
    
    if result is not None and result[0] is not None:
        model, importance = result
        print("\n" + "="*60)
        print("✓ TRAINING COMPLETE")
        print("="*60)
        print("\nNext steps:")
        print("1. Let data collector run for more hours/days")
        print("2. Retrain with more data for better accuracy")
        print("3. Test model with: python backtest.py")
    else:
        print("\n" + "="*60)
        print("⚠️  TRAINING FAILED - NOT ENOUGH DATA")
        print("="*60)
        print("\nKeep the data collector running and try again in a few hours.")