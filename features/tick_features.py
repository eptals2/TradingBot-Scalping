# features/tick_features.py (FIXED - remove FutureWarning and improve merging)
import pandas as pd
import numpy as np

def create_tick_features(ticks_df, window_seconds=60):
    """
    Create features from tick data (microstructure features)
    Aggregates ticks into rolling windows
    """
    df = ticks_df.copy()
    df['time'] = pd.to_datetime(df['time'])
    df = df.sort_values('time').reset_index(drop=True)
    
    # Calculate mid price and spread
    df['mid'] = (df['bid'] + df['ask']) / 2
    df['tick_spread'] = df['ask'] - df['bid']
    df['tick_spread_pct'] = (df['tick_spread'] / df['mid']) * 100
    
    # Price changes between ticks
    df['price_change'] = df['mid'].diff()
    df['price_change_pct'] = df['mid'].pct_change()
    
    # Trade direction (using last price if available)
    if 'last' in df.columns and df['last'].notna().any():
        df['trade_direction'] = np.where(
            df['last'] >= df['ask'], 1,  # Buy
            np.where(df['last'] <= df['bid'], -1, 0)  # Sell or mid
        )
    
    # Aggregate features over rolling windows
    df['window'] = df['time'].dt.floor(f'{window_seconds}s')
    
    agg_features = df.groupby('window').agg({
        'mid': ['first', 'last', 'min', 'max', 'mean', 'std'],
        'tick_spread': ['mean', 'min', 'max', 'std'],
        'tick_spread_pct': ['mean', 'std'],
        'price_change': ['sum', 'std'],
        'volume': ['sum', 'mean'],
        'time': 'count'  # tick count
    }).reset_index()
    
    # Flatten column names
    agg_features.columns = ['_'.join(col).strip('_') for col in agg_features.columns]
    agg_features = agg_features.rename(columns={'window': 'time', 'time_count': 'tick_count'})
    
    # Calculate additional features
    agg_features['price_range'] = agg_features['mid_max'] - agg_features['mid_min']
    agg_features['price_movement'] = agg_features['mid_last'] - agg_features['mid_first']
    agg_features['price_movement_pct'] = (agg_features['price_movement'] / agg_features['mid_first']) * 100
    
    # Tick intensity (ticks per second)
    agg_features['tick_intensity'] = agg_features['tick_count'] / window_seconds
    
    # Order flow (if we have trade direction)
    if 'trade_direction' in df.columns:
        flow = df.groupby('window')['trade_direction'].agg(['sum', 'mean']).reset_index()
        flow.columns = ['time', 'order_flow_sum', 'order_flow_imbalance']
        agg_features = agg_features.merge(flow, on='time', how='left')
    
    return agg_features


def get_tick_feature_columns():
    """Return list of tick feature columns"""
    base_features = [
        'tick_count', 'tick_intensity',
        'mid_mean', 'mid_std',
        'tick_spread_mean', 'tick_spread_std', 'tick_spread_pct_mean',
        'price_range', 'price_movement', 'price_movement_pct',
        'price_change_sum', 'price_change_std',
        'volume_sum', 'volume_mean'
    ]
    
    # Add order flow if available
    optional_features = ['order_flow_sum', 'order_flow_imbalance']
    
    return base_features + optional_features


def aggregate_ticks_to_candles(ticks_df, candles_df):
    """
    Aggregate tick features to match candle timeframe
    Returns candles_df with tick features added
    """
    # Create tick features at 1-minute intervals
    tick_features = create_tick_features(ticks_df, window_seconds=60)
    
    # Merge with candles
    candles_df = candles_df.copy()
    candles_df['time'] = pd.to_datetime(candles_df['time'])
    tick_features['time'] = pd.to_datetime(tick_features['time'])
    
    # Merge on time (keep candles as base)
    merged = candles_df.merge(tick_features, on='time', how='left', suffixes=('', '_tick'))
    
    # Forward fill missing tick data (for candles without ticks) - FIXED
    tick_cols = get_tick_feature_columns()
    for col in tick_cols:
        if col in merged.columns:
            merged[col] = merged[col].ffill()  # Use ffill() instead of fillna(method='ffill')
    
    return merged