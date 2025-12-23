# test_features.py (TEST YOUR FEATURES)
from utils.sqlite_store import get_recent_candles, get_recent_ticks
from features.combined_features import prepare_ml_dataset

def test_features():
    print("Loading data...")
    candles = get_recent_candles(n=5000)
    ticks = get_recent_ticks(n=50000)
    
    print(f"Candles: {len(candles)}")
    print(f"Ticks: {len(ticks)}")
    
    print("\nCreating features...")
    X, y, timestamps, full_df = prepare_ml_dataset(
        candles, 
        ticks,
        target_type='classification',
        pips_target=5,
        periods_ahead=1
    )
    
    print(f"\nFeature matrix shape: {X.shape}")
    print(f"Target distribution:\n{y.value_counts()}")
    print(f"\nFeature columns ({len(X.columns)}):")
    for col in X.columns:
        print(f"  - {col}")
    
    print(f"\nSample of features:")
    print(X.head())
    
    print(f"\nFeature statistics:")
    print(X.describe())
    
    # Check for any NaN or inf values
    print(f"\nNaN values: {X.isna().sum().sum()}")
    print(f"Inf values: {np.isinf(X).sum().sum()}")

if __name__ == "__main__":
    import numpy as np
    test_features()