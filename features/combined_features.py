# features/combined_features.py
import pandas as pd
from features.candle_features import create_candle_features, get_candle_feature_columns
from features.tick_features import aggregate_ticks_to_candles, get_tick_feature_columns

def create_all_features(candles_df, ticks_df=None):
    """
    Create all features combining candles and ticks
    """
    # Create candle features
    df = create_candle_features(candles_df)
    
    # Add tick features if available
    if ticks_df is not None and len(ticks_df) > 0:
        df = aggregate_ticks_to_candles(ticks_df, df)
        feature_cols = get_candle_feature_columns() + get_tick_feature_columns()
    else:
        feature_cols = get_candle_feature_columns()
    
    # Drop rows with NaN values
    df = df.dropna()
    
    return df, feature_cols


def create_target(df, target_type='classification', pips_target=5, periods_ahead=1):
    """
    Create target variable
    
    target_type:
        - 'classification': Binary (1 if price up by pips_target, 0 otherwise)
        - 'regression': Actual pip movement
        - 'direction': Simple direction (1 if up, 0 if down)
    """
    df = df.copy()
    
    pip_value = 0.01  # for XAU/USD, 1 pip = 0.01
    df['future_close'] = df['close'].shift(-periods_ahead)
    df['pip_movement'] = (df['future_close'] - df['close']) / pip_value
    
    if target_type == 'classification':
        # Binary: profitable trade or not
        df['target'] = (df['pip_movement'] >= pips_target).astype(int)
    elif target_type == 'regression':
        # Predict actual pip movement
        df['target'] = df['pip_movement']
    elif target_type == 'direction':
        # Simple direction
        df['target'] = (df['pip_movement'] > 0).astype(int)
    
    # Remove last rows without future data
    df = df[:-periods_ahead] if periods_ahead > 0 else df
    
    return df


def prepare_ml_dataset(candles_df, ticks_df=None, target_type='classification', 
                       pips_target=5, periods_ahead=1):
    """
    Complete pipeline: features + target
    """
    # Create features
    df, feature_cols = create_all_features(candles_df, ticks_df)
    
    # Create target
    df = create_target(df, target_type, pips_target, periods_ahead)
    
    # Filter to only include valid feature columns
    available_cols = [col for col in feature_cols if col in df.columns]
    
    X = df[available_cols]
    y = df['target']
    timestamps = df['time']
    
    return X, y, timestamps, df