# backtest.py (FIXED)
import pandas as pd
import numpy as np
import joblib
from datetime import datetime
import matplotlib.pyplot as plt
import os
import glob

from utils.sqlite_store import get_all_candles, get_all_ticks
from features.combined_features import create_all_features

class ScalpingBacktest:
    def __init__(self, model_path, feature_cols_path, initial_balance=100):
        """
        Initialize backtester
        
        Args:
            model_path: path to trained model
            feature_cols_path: path to feature columns list
            initial_balance: starting balance in USD
        """
        self.model = joblib.load(model_path)
        self.feature_cols = joblib.load(feature_cols_path)
        self.initial_balance = initial_balance
        self.balance = initial_balance
        
        # Trading parameters
        self.position_size = 0.1  # 0.01 lot = 1 oz of gold
        self.pip_value = 0.01  # 1 pip = $0.01 for XAU/USD
        self.spread_cost = 0.5  # average spread in pips
        self.commission = 0  # commission per trade
        
        # Results tracking
        self.trades = []
        self.equity_curve = []
        
    def calculate_position_size(self, risk_percent=1.0, stop_loss_pips=10):
        """Calculate position size based on risk management"""
        risk_amount = self.balance * (risk_percent / 100)
        position_size = risk_amount / (stop_loss_pips * self.pip_value * 100)  # 100 oz per lot
        return min(position_size, 0.1)  # Max 0.1 lot
    
    def execute_trade(self, entry_price, exit_price, signal_time, exit_time, prediction_proba):
        """Execute a trade and record results"""
        
        # Calculate profit/loss in pips
        pip_movement = (exit_price - entry_price) / self.pip_value
        
        # Account for spread cost
        pip_movement -= self.spread_cost
        
        # Calculate P&L in dollars
        pnl = pip_movement * self.pip_value * 100 * self.position_size
        pnl -= self.commission
        
        # Update balance
        self.balance += pnl
        
        # Record trade
        trade = {
            'entry_time': signal_time,
            'exit_time': exit_time,
            'entry_price': entry_price,
            'exit_price': exit_price,
            'pips': pip_movement,
            'pnl': pnl,
            'balance': self.balance,
            'prediction_proba': prediction_proba,
            'position_size': self.position_size
        }
        
        self.trades.append(trade)
        self.equity_curve.append({
            'time': exit_time,
            'balance': self.balance,
            'equity': self.balance
        })
        
        return trade
    
    def run(self, candles_df, ticks_df=None, prediction_threshold=0.5, holding_periods=1):
        """
        Run backtest
        
        Args:
            candles_df: candle data
            ticks_df: tick data (optional)
            prediction_threshold: minimum probability to trade
            holding_periods: how many periods to hold position
        """
        print("="*60)
        print("RUNNING BACKTEST")
        print("="*60)
        
        # Prepare features
        print("\nPreparing features...")
        df, _ = create_all_features(candles_df, ticks_df)
        
        # Ensure we have the right features
        missing_cols = set(self.feature_cols) - set(df.columns)
        if missing_cols:
            print(f"Warning: Missing features: {missing_cols}")
            # Add missing columns with 0
            for col in missing_cols:
                df[col] = 0
        
        X = df[self.feature_cols]
        
        print(f"Data points: {len(X)}")
        
        # Get predictions
        print("\nGenerating predictions...")
        predictions = self.model.predict(X)
        prediction_probas = self.model.predict_proba(X)[:, 1]
        
        # Filter by threshold
        trade_signals = prediction_probas >= prediction_threshold
        
        print(f"Signals generated: {trade_signals.sum()}")
        print(f"Signal rate: {(trade_signals.sum() / len(trade_signals) * 100):.2f}%")
        
        # Simulate trades
        print("\nSimulating trades...")
        i = 0
        while i < len(df) - holding_periods:
            if trade_signals[i]:
                entry_price = df.iloc[i]['close']
                signal_time = df.iloc[i]['time']
                prediction_proba = prediction_probas[i]
                
                # Exit after holding_periods
                exit_idx = i + holding_periods
                exit_price = df.iloc[exit_idx]['close']
                exit_time = df.iloc[exit_idx]['time']
                
                # Execute trade
                trade = self.execute_trade(
                    entry_price, exit_price, 
                    signal_time, exit_time,
                    prediction_proba
                )
                
                # Skip to after trade exit
                i = exit_idx + 1
            else:
                i += 1
        
        return self.get_results()
    
    def get_results(self):
        """Calculate and return backtest results"""
        if len(self.trades) == 0:
            print("\n⚠️  No trades executed!")
            return None
        
        trades_df = pd.DataFrame(self.trades)
        
        # Calculate metrics
        total_trades = len(trades_df)
        winning_trades = len(trades_df[trades_df['pnl'] > 0])
        losing_trades = len(trades_df[trades_df['pnl'] <= 0])
        win_rate = (winning_trades / total_trades) * 100 if total_trades > 0 else 0
        
        total_pnl = trades_df['pnl'].sum()
        total_pips = trades_df['pips'].sum()
        
        avg_win = trades_df[trades_df['pnl'] > 0]['pnl'].mean() if winning_trades > 0 else 0
        avg_loss = trades_df[trades_df['pnl'] <= 0]['pnl'].mean() if losing_trades > 0 else 0
        
        profit_factor = abs(trades_df[trades_df['pnl'] > 0]['pnl'].sum() / 
                           trades_df[trades_df['pnl'] <= 0]['pnl'].sum()) if losing_trades > 0 else float('inf')
        
        max_balance = trades_df['balance'].max()
        max_drawdown = ((max_balance - trades_df['balance'].min()) / max_balance) * 100
        
        roi = ((self.balance - self.initial_balance) / self.initial_balance) * 100
        
        results = {
            'total_trades': total_trades,
            'winning_trades': winning_trades,
            'losing_trades': losing_trades,
            'win_rate': win_rate,
            'total_pnl': total_pnl,
            'total_pips': total_pips,
            'avg_win': avg_win,
            'avg_loss': avg_loss,
            'profit_factor': profit_factor,
            'max_drawdown': max_drawdown,
            'roi': roi,
            'final_balance': self.balance,
            'trades_df': trades_df
        }
        
        return results
    
    def print_results(self, results):
        """Print backtest results"""
        if results is None:
            return
        
        print("\n" + "="*60)
        print("BACKTEST RESULTS")
        print("="*60)
        
        print(f"\n📊 TRADE STATISTICS:")
        print(f"  Total Trades: {results['total_trades']}")
        print(f"  Winning Trades: {results['winning_trades']}")
        print(f"  Losing Trades: {results['losing_trades']}")
        print(f"  Win Rate: {results['win_rate']:.2f}%")
        
        print(f"\n💰 PROFITABILITY:")
        print(f"  Initial Balance: ${self.initial_balance:,.2f}")
        print(f"  Final Balance: ${results['final_balance']:,.2f}")
        print(f"  Total P&L: ${results['total_pnl']:,.2f}")
        print(f"  Total Pips: {results['total_pips']:.2f}")
        print(f"  ROI: {results['roi']:.2f}%")
        
        print(f"\n📈 PERFORMANCE METRICS:")
        print(f"  Average Win: ${results['avg_win']:.2f}")
        print(f"  Average Loss: ${results['avg_loss']:.2f}")
        print(f"  Profit Factor: {results['profit_factor']:.2f}")
        print(f"  Max Drawdown: {results['max_drawdown']:.2f}%")
        
        # Show last 10 trades
        print(f"\n📋 LAST 10 TRADES:")
        trades_df = results['trades_df']
        print(trades_df[['entry_time', 'pips', 'pnl', 'balance']].tail(10).to_string(index=False))
    
    def plot_results(self, results):
        """Plot equity curve and trade distribution"""
        if results is None or len(self.trades) == 0:
            return
        
        fig, axes = plt.subplots(2, 2, figsize=(15, 10))
        
        trades_df = results['trades_df']
        
        # Equity curve
        axes[0, 0].plot(trades_df['exit_time'], trades_df['balance'], 'b-', linewidth=2)
        axes[0, 0].axhline(y=self.initial_balance, color='r', linestyle='--', label='Initial Balance')
        axes[0, 0].set_title('Equity Curve')
        axes[0, 0].set_xlabel('Time')
        axes[0, 0].set_ylabel('Balance ($)')
        axes[0, 0].legend()
        axes[0, 0].grid(True, alpha=0.3)
        
        # P&L distribution
        axes[0, 1].hist(trades_df['pnl'], bins=30, edgecolor='black', alpha=0.7)
        axes[0, 1].axvline(x=0, color='r', linestyle='--', linewidth=2)
        axes[0, 1].set_title('P&L Distribution')
        axes[0, 1].set_xlabel('P&L ($)')
        axes[0, 1].set_ylabel('Frequency')
        axes[0, 1].grid(True, alpha=0.3)
        
        # Pips distribution
        axes[1, 0].hist(trades_df['pips'], bins=30, edgecolor='black', alpha=0.7, color='green')
        axes[1, 0].axvline(x=0, color='r', linestyle='--', linewidth=2)
        axes[1, 0].set_title('Pips Distribution')
        axes[1, 0].set_xlabel('Pips')
        axes[1, 0].set_ylabel('Frequency')
        axes[1, 0].grid(True, alpha=0.3)
        
        # Cumulative P&L
        cumulative_pnl = trades_df['pnl'].cumsum()
        axes[1, 1].plot(trades_df['exit_time'], cumulative_pnl, 'g-', linewidth=2)
        axes[1, 1].axhline(y=0, color='r', linestyle='--')
        axes[1, 1].set_title('Cumulative P&L')
        axes[1, 1].set_xlabel('Time')
        axes[1, 1].set_ylabel('Cumulative P&L ($)')
        axes[1, 1].grid(True, alpha=0.3)
        
        plt.tight_layout()
        plt.savefig('backtest_results.png', dpi=300, bbox_inches='tight')
        print(f"\n✓ Results plot saved: backtest_results.png")
        plt.show()


def run_backtest(model_path, feature_cols_path, prediction_threshold=0.6):
    """Run backtest with latest model"""
    
    # Load data
    print("Loading data...")
    candles = get_all_candles()
    ticks = get_all_ticks()
    
    print(f"Candles: {len(candles)}")
    print(f"Ticks: {len(ticks)}")
    
    # Initialize backtester
    backtester = ScalpingBacktest(model_path, feature_cols_path, initial_balance=100)
    
    # Run backtest
    results = backtester.run(
        candles, 
        ticks,
        prediction_threshold=prediction_threshold,
        holding_periods=1  # Hold for 1 candle (1 minute)
    )
    
    # Print results
    backtester.print_results(results)
    
    # Plot results
    if results and len(backtester.trades) > 0:
        backtester.plot_results(results)
    
    return backtester, results

def run_backtest_small_account(model_path, feature_cols_path, 
                                initial_balance=100,
                                lot_size=0.1,
                                prediction_threshold=0.6):
    """
    Run backtest optimized for small accounts
    """
    
    print(f"\n{'='*60}")
    print(f"SMALL ACCOUNT BACKTEST")
    print(f"{'='*60}")
    print(f"Initial Balance: ${initial_balance:.2f}")  # Show what was passed
    print(f"Lot Size: {lot_size}")
    print(f"Threshold: {prediction_threshold}")
    
    # Load data
    print("\nLoading data...")
    candles = get_all_candles()
    ticks = get_all_ticks()
    
    print(f"Candles: {len(candles)}")
    print(f"Ticks: {len(ticks)}")
    
    # Initialize backtester with CORRECT initial balance
    backtester = ScalpingBacktest(
        model_path, 
        feature_cols_path, 
        initial_balance=initial_balance  # This is the FIX
    )
    
    # Set position size and pip value based on lot size
    backtester.position_size = lot_size
    
    if lot_size == 0.001:
        backtester.pip_value = 0.001  # $0.001 per pip for micro lot
    elif lot_size == 0.01:
        backtester.pip_value = 0.01   # $0.01 per pip for mini lot
    elif lot_size == 0.1:
        backtester.pip_value = 0.1    # $0.10 per pip
    else:
        backtester.pip_value = lot_size * 1.0  # General formula
    
    # Run backtest
    results = backtester.run(
        candles, 
        ticks,
        prediction_threshold=prediction_threshold,
        holding_periods=1
    )
    
    # Print results
    backtester.print_results(results)
    
    if results and len(backtester.trades) > 0:
        # Additional analysis for small accounts
        print(f"\n{'='*60}")
        print("SMALL ACCOUNT ANALYSIS")
        print(f"{'='*60}")
        
        trades_df = results['trades_df']
        
        # Calculate growth
        growth = ((results['final_balance'] - initial_balance) / initial_balance) * 100
        print(f"Account Growth: {growth:+.2f}%")
        
        # Actual profit in dollars
        print(f"Profit: ${results['total_pnl']:.2f}")
        print(f"Starting: ${initial_balance:.2f} → Ending: ${results['final_balance']:.2f}")
        
        # Risk analysis
        max_loss = trades_df['pnl'].min()
        max_win = trades_df['pnl'].max()
        print(f"\nLargest Win: ${max_win:.4f}")
        print(f"Largest Loss: ${max_loss:.4f}")
        print(f"Average Trade: ${trades_df['pnl'].mean():.4f}")
        
        # Time to double account (if profitable)
        if results['roi'] > 0:
            time_span = (trades_df['exit_time'].max() - trades_df['entry_time'].min())
            days = time_span.days if time_span.days > 0 else 1
            
            trades_per_day = len(trades_df) / days
            daily_roi = results['roi'] / days
            days_to_double = 100 / daily_roi if daily_roi > 0 else float('inf')
            
            print(f"\n📊 Projections:")
            print(f"  Trading period: {days} days")
            print(f"  Avg trades/day: {trades_per_day:.1f}")
            print(f"  Daily ROI: {daily_roi:.2f}%")
            
            if days_to_double < 365:
                print(f"  Days to double: {days_to_double:.0f} days")
                print(f"  Months to double: {days_to_double/30:.1f} months")
            else:
                print(f"  Days to double: Not achievable at current rate")
        
        # Monthly projection
        if time_span.days > 0:
            monthly_roi = (results['roi'] / days) * 30
            print(f"  Estimated monthly ROI: {monthly_roi:.2f}%")
        
        # Plot results
        backtester.plot_results(results)
    
    return backtester, results

if __name__ == "__main__":
    import glob
    import argparse

    parser = argparse.ArgumentParser(description='Backtest scalping strategy')
    parser.add_argument('--balance', type=float, default=100, help='Initial balance')
    parser.add_argument('--lot-size', type=float, default=0.1, help='Position size')
    parser.add_argument('--threshold', type=float, default=0.7, help='Prediction threshold')
    args = parser.parse_args()

    # Find latest model
    models = glob.glob('models/scalping_model_*.pkl')
    if not models:
        print("No trained model found! Run train_model.py first.")
        exit(1)
    
    # Sort by timestamp in filename
    latest_model = max(models, key=os.path.getmtime)
    
    # Extract timestamp from model filename
    # Example: models/scalping_model_rf_20251223_171820.pkl
    model_filename = os.path.basename(latest_model)
    timestamp = model_filename.split('_')[-2] + '_' + model_filename.split('_')[-1].replace('.pkl', '')
    
    # Construct feature columns path
    feature_cols = os.path.join('models', f'feature_columns_{timestamp}.pkl')
    
    # Check if feature columns file exists
    if not os.path.exists(feature_cols):
        print(f"ERROR: Feature columns file not found: {feature_cols}")
        print(f"Looking for alternative...")
        
        # Try to find any feature columns file
        feature_files = glob.glob('models/feature_columns_*.pkl')
        if feature_files:
            feature_cols = max(feature_files, key=os.path.getmtime)
            print(f"Using: {feature_cols}")
        else:
            print("No feature columns file found!")
            exit(1)
    
    print(f"Using model: {latest_model}")
    print(f"Using features: {feature_cols}")

    # Run backtest
    backtester, results = run_backtest_small_account(
        latest_model, 
        feature_cols,
        initial_balance=args.balance,
        lot_size=args.lot_size,
        prediction_threshold=args.threshold
    )
    
    # Run backtest with different thresholds
    # print("\n" + "="*60)
    # print("TESTING DIFFERENT PREDICTION THRESHOLDS")
    # print("="*60)
    
    # for threshold in [0.4, 0.5, 0.6, 0.7]:
    #     print(f"\n{'='*60}")
    #     print(f"Threshold: {threshold}")
    #     print(f"{'='*60}")
    #     backtester, results = run_backtest(latest_model, feature_cols, prediction_threshold=threshold)
        
    #     if results:
    #         print(f"\n✓ Threshold {threshold}: ROI = {results['roi']:.2f}%, Win Rate = {results['win_rate']:.2f}%")