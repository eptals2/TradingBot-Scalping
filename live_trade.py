# live_trade.py (ENHANCED WITH BUY/SELL SIGNALS)
import MetaTrader5 as mt5
import pandas as pd
import joblib
import time
from datetime import datetime
import os
import glob

from utils.mt5_connector import connect, shutdown
from utils.sqlite_store import get_recent_candles, get_recent_ticks
from features.combined_features import create_all_features
from config import SYMBOL

class LiveScalpingTrader:
    def __init__(self, model_path, feature_cols_path, demo_mode=True):
        """
        Initialize live trading bot
        """
        self.model = joblib.load(model_path)
        self.feature_cols = joblib.load(feature_cols_path)
        self.demo_mode = demo_mode
        
        # Trading parameters
        self.symbol = SYMBOL
        self.lot_size = 0.01
        self.magic_number = 234000
        self.slippage = 10
        
        # Risk management - SL/TP in pips
        self.max_positions = 1
        self.stop_loss_pips = 10
        self.take_profit_pips = 15
        self.trailing_stop_pips = 8  # Trailing stop
        self.max_daily_loss = 100
        self.max_daily_trades = 200
        
        # Trading signals
        self.prediction_threshold = 0.38
        self.min_data_points = 100
        
        # Signal validation (use Parabolic SAR for confirmation)
        self.use_psar_filter = True  # Only trade with PSAR
        
        # State tracking
        self.daily_pnl = 0
        self.daily_trades = 0
        self.last_trade_time = None
        self.min_time_between_trades = 60
        
        # Performance tracking
        self.trades_log = []

        # Don't set lot_size here, calculate it dynamically
        self.risk_percent = 1.0  # Risk 1% per trade

    def calculate_lot_size(self):
        """Calculate position size based on account balance"""
        account_info = mt5.account_info()
        if account_info is None:
            return 0.01
        
        balance = account_info.balance
        free_margin = account_info.margin_free
        
        # Calculate based on balance
        if balance < 100 and free_margin < 100:
            return 0.01  # Mini lot
        elif balance < 1000 and free_margin < 1000:
            return 0.1   # Standard
        elif balance < 10000 and free_margin < 10000:
            return 0.5  
        else:
            # Risk-based sizing
            risk_amount = balance * (self.risk_percent / 100)
            lot_size = risk_amount / (self.stop_loss_pips * 10)  # 10 = pip value
            return min(max(0.001, lot_size), 1.0)  # Between 0.001 and 1.0
    
    def open_position(self, signal, prediction_proba):
        """Open a trading position with SL/TP"""
        symbol_info = mt5.symbol_info(self.symbol)
        if symbol_info is None:
            return False
        
        if not self.can_trade():
            return False
        
        # Calculate lot size dynamically
        lot_size = self.calculate_lot_size()
        
        # Prepare order
        order_type = mt5.ORDER_TYPE_BUY if signal == 'BUY' else mt5.ORDER_TYPE_SELL
        price = symbol_info.ask if signal == 'BUY' else symbol_info.bid
        
        # Calculate SL/TP
        sl, tp = self.calculate_sl_tp(order_type, price)
        
        # Create order request
        request = {
            "action": mt5.TRADE_ACTION_DEAL,
            "symbol": self.symbol,
            "volume": lot_size,  # Use calculated lot size
            "type": order_type,
            "price": price,
            "sl": sl,
            "tp": tp,
            "deviation": self.slippage,
            "magic": self.magic_number,
            "comment": f"ML_{signal}_{prediction_proba:.2f}",
            "type_time": mt5.ORDER_TIME_GTC,
            "type_filling": mt5.ORDER_FILLING_IOC,
        }
        
        # Get account info for logging
        account_info = mt5.account_info()
        
        print(f"\n{'='*60}")
        print(f"🔔 OPENING {signal} POSITION")
        print(f"{'='*60}")
        print(f"Account Balance: ${account_info.balance:,.2f}")
        print(f"Free Margin: ${account_info.margin_free:,.2f}")
        print(f"Lot Size: {lot_size}")
        print(f"Probability: {prediction_proba:.2%}")
        print(f"Entry: {price:.2f}")
        print(f"Stop Loss: {sl:.2f} ({self.stop_loss_pips} pips)")
        print(f"Take Profit: {tp:.2f} ({self.take_profit_pips} pips)")
        print(f"Risk/Reward: 1:{self.take_profit_pips/self.stop_loss_pips:.1f}")
        
    def connect_mt5(self):
        """Connect to MT5"""
        if not connect():
            print("❌ Failed to connect to MT5")
            return False
        
        account_info = mt5.account_info()
        if account_info is None:
            print("❌ Failed to get account info")
            return False
        
        print(f"\n{'='*60}")
        print(f"MT5 CONNECTION SUCCESSFUL")
        print(f"{'='*60}")
        print(f"Account: {account_info.login}")
        print(f"Server: {account_info.server}")
        print(f"Balance: ${account_info.balance:,.2f}")
        print(f"Equity: ${account_info.equity:,.2f}")
        print(f"Margin Free: ${account_info.margin_free:,.2f}")
        
        if account_info.trade_mode != mt5.ACCOUNT_TRADE_MODE_DEMO:
            print("\n⚠️  WARNING: NOT A DEMO ACCOUNT!")
            print("⚠️  This bot is for DEMO ACCOUNTS ONLY!")
            return False
        
        print(f"✓ Demo Account Confirmed")
        return True
    
    def check_symbol(self):
        """Check if symbol is available"""
        symbol_info = mt5.symbol_info(self.symbol)
        if symbol_info is None:
            print(f"❌ Symbol {self.symbol} not found")
            return False
        
        if not symbol_info.visible:
            if not mt5.symbol_select(self.symbol, True):
                print(f"❌ Failed to enable {self.symbol}")
                return False
        
        print(f"\n✓ Symbol: {self.symbol}")
        print(f"  Bid: {symbol_info.bid}")
        print(f"  Ask: {symbol_info.ask}")
        print(f"  Spread: {symbol_info.spread}")
        
        return True
    
    def get_current_data(self):
        """Get recent candles and ticks for prediction"""
        candles = get_recent_candles(n=500)
        ticks = get_recent_ticks(n=10000)
        
        if len(candles) < self.min_data_points:
            print(f"⚠️  Not enough candles: {len(candles)} (need {self.min_data_points})")
            return None, None
        
        return candles, ticks
    
    def generate_signal(self):
        """
        Generate trading signal from current data
        Returns: signal ('BUY', 'SELL', or 'WAIT'), probability, psar_trend
        """
        candles, ticks = self.get_current_data()
        if candles is None:
            return None, 0.0, None
        
        try:
            df, _ = create_all_features(candles, ticks)
            latest = df.iloc[-1:]
            
            # Get PSAR trend
            psar_trend = latest['psar_trend'].values[0] if 'psar_trend' in latest.columns else 0
            psar_bullish = psar_trend == 1
            
            # Ensure we have all required features
            missing_cols = set(self.feature_cols) - set(latest.columns)
            if missing_cols:
                for col in missing_cols:
                    latest[col] = 0
            
            X = latest[self.feature_cols]
            
            # Get ML prediction
            prediction = self.model.predict(X)[0]
            prediction_proba = self.model.predict_proba(X)[0, 1]
            
            # Determine signal
            if prediction_proba >= self.prediction_threshold:
                # Use PSAR for direction
                if self.use_psar_filter:
                    signal = 'BUY' if psar_bullish else 'SELL'
                else:
                    signal = 'BUY' if prediction == 1 else 'SELL'
            else:
                signal = 'WAIT'
            
            return signal, prediction_proba, psar_trend
            
        except Exception as e:
            print(f"❌ Error generating signal: {e}")
            import traceback
            traceback.print_exc()
            return None, 0.0, None
    
    def get_open_positions(self):
        """Get currently open positions"""
        positions = mt5.positions_get(symbol=self.symbol)
        if positions is None:
            return []
        return [pos for pos in positions if pos.magic == self.magic_number]
    
    def calculate_sl_tp(self, order_type, entry_price):
        """Calculate stop loss and take profit prices"""
        point = mt5.symbol_info(self.symbol).point
        
        if order_type == mt5.ORDER_TYPE_BUY:
            sl = entry_price - self.stop_loss_pips * 10 * point
            tp = entry_price + self.take_profit_pips * 10 * point
        else:  # SELL
            sl = entry_price + self.stop_loss_pips * 10 * point
            tp = entry_price - self.take_profit_pips * 10 * point
        
        return sl, tp
    
    def open_position(self, signal, prediction_proba):
        """Open a trading position with SL/TP"""
        symbol_info = mt5.symbol_info(self.symbol)
        if symbol_info is None:
            return False
        
        if not self.can_trade():
            return False
        
        # Prepare order
        order_type = mt5.ORDER_TYPE_BUY if signal == 'BUY' else mt5.ORDER_TYPE_SELL
        price = symbol_info.ask if signal == 'BUY' else symbol_info.bid
        
        # Calculate SL/TP
        sl, tp = self.calculate_sl_tp(order_type, price)
        
        # Create order request
        request = {
            "action": mt5.TRADE_ACTION_DEAL,
            "symbol": self.symbol,
            "volume": self.lot_size,
            "type": order_type,
            "price": price,
            "sl": sl,
            "tp": tp,
            "deviation": self.slippage,
            "magic": self.magic_number,
            "comment": f"ML_{signal}_{prediction_proba:.2f}",
            "type_time": mt5.ORDER_TIME_GTC,
            "type_filling": mt5.ORDER_FILLING_IOC,
        }
        
        print(f"\n{'='*60}")
        print(f"🔔 OPENING {signal} POSITION")
        print(f"{'='*60}")
        print(f"Probability: {prediction_proba:.2%}")
        print(f"Entry: {price:.2f}")
        print(f"Stop Loss: {sl:.2f} ({self.stop_loss_pips} pips)")
        print(f"Take Profit: {tp:.2f} ({self.take_profit_pips} pips)")
        print(f"Risk/Reward: 1:{self.take_profit_pips/self.stop_loss_pips:.1f}")
        
        result = mt5.order_send(request)
        
        if result is None:
            print(f"❌ Order send failed: {mt5.last_error()}")
            return False
        
        if result.retcode != mt5.TRADE_RETCODE_DONE:
            print(f"❌ Order failed: {result.retcode} - {result.comment}")
            return False
        
        print(f"✅ Position opened successfully!")
        print(f"   Ticket: #{result.order}")
        print(f"   Volume: {self.lot_size} lot")
        
        # Log trade
        self.log_trade({
            'time': datetime.now(),
            'signal': signal,
            'probability': prediction_proba,
            'entry_price': price,
            'sl': sl,
            'tp': tp,
            'ticket': result.order,
            'status': 'OPEN'
        })
        
        self.daily_trades += 1
        self.last_trade_time = time.time()
        
        return True
    
    def can_trade(self):
        """Check if we can open a new trade"""
        open_positions = self.get_open_positions()
        if len(open_positions) >= self.max_positions:
            return False
        
        if self.daily_trades >= self.max_daily_trades:
            return False
        
        if self.daily_pnl <= -self.max_daily_loss:
            return False
        
        if self.last_trade_time is not None:
            time_since_last = time.time() - self.last_trade_time
            if time_since_last < self.min_time_between_trades:
                return False
        
        return True
    
    def monitor_positions(self):
        """Monitor and update open positions"""
        positions = self.get_open_positions()
        
        if len(positions) == 0:
            return
        
        print(f"\n📊 OPEN POSITIONS: {len(positions)}")
        
        for position in positions:
            profit = position.profit
            pips = (position.price_current - position.price_open) / mt5.symbol_info(self.symbol).point / 10
            if position.type == mt5.ORDER_TYPE_SELL:
                pips = -pips
            
            print(f"  Ticket #{position.ticket}:")
            print(f"    Type: {'BUY' if position.type == 0 else 'SELL'}")
            print(f"    Entry: {position.price_open:.2f}")
            print(f"    Current: {position.price_current:.2f}")
            print(f"    Pips: {pips:+.1f}")
            print(f"    P&L: ${profit:+.2f}")
    
    def log_trade(self, trade_info):
        """Log trade to file"""
        self.trades_log.append(trade_info)
        
        os.makedirs('__logs', exist_ok=True)
        df = pd.DataFrame(self.trades_log)
        df.to_csv('./__logs/live_trades.csv', index=False)
    
    def reset_daily_stats(self):
        """Reset daily statistics"""
        current_date = datetime.now().date()
        if not hasattr(self, 'last_reset_date') or self.last_reset_date != current_date:
            print(f"\n{'='*60}")
            print(f"📅 NEW TRADING DAY: {current_date}")
            print(f"{'='*60}")
            self.daily_pnl = 0
            self.daily_trades = 0
            self.last_reset_date = current_date
    
    def run(self, check_interval=60):
        """Main trading loop"""
        print(f"\n{'='*60}")
        print(f"🚀 LIVE SCALPING BOT STARTED")
        print(f"{'='*60}")
        print(f"Symbol: {self.symbol}")
        print(f"Lot Size: {self.lot_size}")
        print(f"Stop Loss: {self.stop_loss_pips} pips")
        print(f"Take Profit: {self.take_profit_pips} pips")
        print(f"Prediction Threshold: {self.prediction_threshold}")
        print(f"PSAR Filter: {'ON' if self.use_psar_filter else 'OFF'}")
        print(f"Check Interval: {check_interval}s")
        print(f"\n⚠️  DEMO MODE: {self.demo_mode}")
        print(f"{'='*60}")
        
        if not self.connect_mt5():
            return
        
        if not self.check_symbol():
            shutdown()
            return
        
        print(f"\n✅ Bot is ready to trade!")
        print(f"🔍 Monitoring for signals every {check_interval} seconds...")
        print(f"⏸️  Press Ctrl+C to stop\n")
        
        try:
            while True:
                self.reset_daily_stats()
                current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                
                # Generate signal
                signal, proba, psar_trend = self.generate_signal()
                
                if signal is not None:
                    psar_indicator = "📈" if psar_trend == 1 else "📉" if psar_trend == -1 else "➡️"
                    print(f"[{current_time}] Signal: {signal} (Prob: {proba:.2%}) {psar_indicator} PSAR")
                    
                    # Open position if signal is BUY or SELL
                    if signal in ['BUY', 'SELL']:
                        self.open_position(signal, proba)
                    
                    # Monitor existing positions
                    self.monitor_positions()
                    
                    # Show daily stats
                    print(f"\n📊 Daily Stats:")
                    print(f"  Trades: {self.daily_trades}/{self.max_daily_trades}")
                    print(f"  P&L: ${self.daily_pnl:.2f}")
                else:
                    print(f"[{current_time}] ⏳ Waiting for data...")
                
                time.sleep(check_interval)
                
        except KeyboardInterrupt:
            print(f"\n\n{'='*60}")
            print("⏹️  BOT STOPPED BY USER")
            print(f"{'='*60}")
            print(f"Total trades today: {self.daily_trades}")
            print(f"Total P&L today: ${self.daily_pnl:.2f}")
            
        finally:
            shutdown()


def main():
    """Main function to start live trading"""
    
    models = glob.glob('models/scalping_model_*.pkl')
    if not models:
        print("❌ No trained model found! Run train_model.py first.")
        return
    
    latest_model = max(models, key=os.path.getmtime)
    model_filename = os.path.basename(latest_model)
    timestamp = model_filename.split('_')[-2] + '_' + model_filename.split('_')[-1].replace('.pkl', '')
    feature_cols = os.path.join('models', f'feature_columns_{timestamp}.pkl')
    
    if not os.path.exists(feature_cols):
        feature_files = glob.glob('models/feature_columns_*.pkl')
        if feature_files:
            feature_cols = max(feature_files, key=os.path.getmtime)
        else:
            print("❌ No feature columns file found!")
            return
    
    print(f"📁 Using model: {latest_model}")
    print(f"📁 Using features: {feature_cols}")
    
    trader = LiveScalpingTrader(
        model_path=latest_model,
        feature_cols_path=feature_cols,
        demo_mode=True
    )
    
    trader.run(check_interval=60)


if __name__ == "__main__":
    main()