# features/candle_features.py (ADD PARABOLIC SAR)
import pandas as pd
import numpy as np

def calculate_parabolic_sar(df, af_start=0.02, af_increment=0.02, af_max=0.2):
    """Calculate Parabolic SAR"""
    high = df['high'].values
    low = df['low'].values
    close = df['close'].values
    
    sar = np.zeros(len(df))
    ep = np.zeros(len(df))
    af = np.zeros(len(df))
    trend = np.zeros(len(df))  # 1 for uptrend, -1 for downtrend
    
    # Initialize
    sar[0] = low[0]
    ep[0] = high[0]
    af[0] = af_start
    trend[0] = 1
    
    for i in range(1, len(df)):
        # Previous values
        prev_sar = sar[i-1]
        prev_ep = ep[i-1]
        prev_af = af[i-1]
        prev_trend = trend[i-1]
        
        # Calculate new SAR
        sar[i] = prev_sar + prev_af * (prev_ep - prev_sar)
        
        if prev_trend == 1:  # Uptrend
            # Check for reversal
            if low[i] < sar[i]:
                # Switch to downtrend
                trend[i] = -1
                sar[i] = prev_ep
                ep[i] = low[i]
                af[i] = af_start
            else:
                # Continue uptrend
                trend[i] = 1
                if high[i] > prev_ep:
                    ep[i] = high[i]
                    af[i] = min(prev_af + af_increment, af_max)
                else:
                    ep[i] = prev_ep
                    af[i] = prev_af
                
                # SAR must be below last two lows
                sar[i] = min(sar[i], low[i-1], low[i-2] if i > 1 else low[i-1])
        
        else:  # Downtrend
            # Check for reversal
            if high[i] > sar[i]:
                # Switch to uptrend
                trend[i] = 1
                sar[i] = prev_ep
                ep[i] = high[i]
                af[i] = af_start
            else:
                # Continue downtrend
                trend[i] = -1
                if low[i] < prev_ep:
                    ep[i] = low[i]
                    af[i] = min(prev_af + af_increment, af_max)
                else:
                    ep[i] = prev_ep
                    af[i] = prev_af
                
                # SAR must be above last two highs
                sar[i] = max(sar[i], high[i-1], high[i-2] if i > 1 else high[i-1])
    
    return sar, trend


def create_candle_features(df):
    """
    Create features from candle data (context features)
    df must have: 'time', 'open', 'high', 'low', 'close', 'tick_volume', 'spread', 'bid', 'ask'
    """
    df = df.copy()
    df['time'] = pd.to_datetime(df['time'])
    df = df.sort_values('time').reset_index(drop=True)
    
    # ============ SPREAD FEATURES ============
    df['spread_pct'] = (df['spread'] / df['close']) * 100
    df['spread_ma5'] = df['spread'].rolling(window=5).mean()
    df['spread_ratio'] = df['spread'] / (df['spread_ma5'] + 1e-8)
    
    # ============ VOLATILITY FEATURES (ATR) ============
    df['tr'] = np.maximum(
        df['high'] - df['low'],
        np.maximum(
            abs(df['high'] - df['close'].shift(1)),
            abs(df['low'] - df['close'].shift(1))
        )
    )
    df['atr_5'] = df['tr'].rolling(window=5).mean()
    df['atr_14'] = df['tr'].rolling(window=14).mean()
    df['atr_ratio'] = df['atr_5'] / (df['atr_14'] + 1e-8)
    
    # ============ PRICE MOMENTUM ============
    df['return_1'] = df['close'].pct_change(1)
    df['return_3'] = df['close'].pct_change(3)
    df['return_5'] = df['close'].pct_change(5)
    df['return_10'] = df['close'].pct_change(10)
    
    # ============ MOVING AVERAGES ============
    df['ema_9'] = df['close'].ewm(span=9, adjust=False).mean()
    df['ema_20'] = df['close'].ewm(span=20, adjust=False).mean()
    df['ema_50'] = df['close'].ewm(span=50, adjust=False).mean()
    
    df['dist_ema9'] = (df['close'] - df['ema_9']) / df['ema_9'] * 100
    df['dist_ema20'] = (df['close'] - df['ema_20']) / df['ema_20'] * 100
    df['dist_ema50'] = (df['close'] - df['ema_50']) / df['ema_50'] * 100
    
    # EMA crossovers
    df['ema9_above_20'] = (df['ema_9'] > df['ema_20']).astype(int)
    df['ema20_above_50'] = (df['ema_20'] > df['ema_50']).astype(int)
    
    # ============ PARABOLIC SAR ============
    sar, sar_trend = calculate_parabolic_sar(df)
    df['psar'] = sar
    df['psar_trend'] = sar_trend  # 1 = uptrend, -1 = downtrend
    df['psar_bullish'] = (df['psar_trend'] == 1).astype(int)
    df['dist_from_psar'] = (df['close'] - df['psar']) / df['close'] * 100
    
    # ============ VOLUME FEATURES ============
    df['volume_ma5'] = df['tick_volume'].rolling(window=5).mean()
    df['volume_ma20'] = df['tick_volume'].rolling(window=20).mean()
    df['volume_ratio'] = df['tick_volume'] / (df['volume_ma5'] + 1e-8)
    df['volume_trend'] = df['volume_ma5'] / (df['volume_ma20'] + 1e-8)
    
    # ============ RANGE FEATURES ============
    df['high_5'] = df['high'].rolling(window=5).max()
    df['low_5'] = df['low'].rolling(window=5).min()
    df['range_5'] = df['high_5'] - df['low_5']
    
    df['high_20'] = df['high'].rolling(window=20).max()
    df['low_20'] = df['low'].rolling(window=20).min()
    df['range_20'] = df['high_20'] - df['low_20']
    
    df['dist_from_high5'] = (df['high_5'] - df['close']) / df['close'] * 100
    df['dist_from_low5'] = (df['close'] - df['low_5']) / df['close'] * 100
    df['price_position_5'] = (df['close'] - df['low_5']) / (df['range_5'] + 1e-8)
    
    # ============ TIME FEATURES ============
    df['hour'] = df['time'].dt.hour
    df['minute'] = df['time'].dt.minute
    df['day_of_week'] = df['time'].dt.dayofweek
    
    # Trading session indicators
    df['is_asian'] = ((df['hour'] >= 0) & (df['hour'] < 8)).astype(int)
    df['is_london'] = ((df['hour'] >= 8) & (df['hour'] < 13)).astype(int)
    df['is_ny'] = ((df['hour'] >= 13) & (df['hour'] < 22)).astype(int)
    df['is_overlap'] = ((df['hour'] >= 13) & (df['hour'] < 17)).astype(int)
    
    # ============ TECHNICAL INDICATORS ============
    # RSI
    delta = df['close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / (loss + 1e-8)
    df['rsi_14'] = 100 - (100 / (1 + rs))
    
    # Bollinger Bands
    df['bb_middle'] = df['close'].rolling(window=20).mean()
    df['bb_std'] = df['close'].rolling(window=20).std()
    df['bb_upper'] = df['bb_middle'] + (df['bb_std'] * 2)
    df['bb_lower'] = df['bb_middle'] - (df['bb_std'] * 2)
    df['bb_position'] = (df['close'] - df['bb_lower']) / (df['bb_upper'] - df['bb_lower'] + 1e-8)
    
    return df


def get_candle_feature_columns():
    """Return list of candle feature columns"""
    return [
        # Spread
        'spread', 'spread_pct', 'spread_ratio',
        # Volatility
        'atr_5', 'atr_14', 'atr_ratio',
        # Momentum
        'return_1', 'return_3', 'return_5', 'return_10',
        # Moving averages
        'dist_ema9', 'dist_ema20', 'dist_ema50',
        'ema9_above_20', 'ema20_above_50',
        # Parabolic SAR
        'psar_trend', 'psar_bullish', 'dist_from_psar',
        # Volume
        'volume_ratio', 'volume_trend',
        # Range
        'range_5', 'dist_from_high5', 'dist_from_low5', 'price_position_5',
        # Time
        'hour', 'minute', 'day_of_week',
        'is_asian', 'is_london', 'is_ny', 'is_overlap',
        # Indicators
        'rsi_14', 'bb_position'
    ]