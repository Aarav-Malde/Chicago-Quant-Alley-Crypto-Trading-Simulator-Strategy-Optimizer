# Strategy.py

import pandas as pd

class Strategy:
    def __init__(self, simulator):
        self.sim = simulator
        self.entry_time = None
        self.entry_price = None
        self.call_symbol = None
        self.put_symbol = None
        self.call_active = False
        self.put_active = False
        self.pnl = 0
        self.trades = []

    def onMarketData(self, row):
        time = pd.to_datetime(row['time'])
        symbol = row['Symbol']
        price = row['price']

        # --- Entry logic at exactly 1 PM ---
        if self.entry_time is None and time.hour == 13 and time.minute == 0:
            self.entry_time = time
            self.entry_price = price
            futures_price = price

            # Strike range (±2%)
            lower, upper = 0.98 * futures_price, 1.02 * futures_price

            # Get all symbols available at this timestamp
            symbols_now = self.sim.df[self.sim.df["time"] == row["time"]]["Symbol"].unique()
            call_strikes = [s for s in symbols_now if "C-BTC" in s]
            put_strikes = [s for s in symbols_now if "P-BTC" in s]

            def extract_strike(s):
                try:
                    return float(s.split('-')[3])
                except (IndexError, ValueError):
                    print(f"Couldn't extract strike from: {s}")
                    return float('inf')

            # Pick closest ATM strikes
            closest_call = min(call_strikes, key=lambda s: abs(extract_strike(s) - futures_price), default=None)
            closest_put = min(put_strikes, key=lambda s: abs(extract_strike(s) - futures_price), default=None)

            if closest_call and closest_put:
                self.call_symbol = closest_call
                self.put_symbol = closest_put

                # Fallback if price data is missing
                call_price = self.sim.currentPrice.get(self.call_symbol, price)
                put_price = self.sim.currentPrice.get(self.put_symbol, price)

                # Place straddle orders (sell both)
                self.sim.onOrder(self.call_symbol, "sell", 0.1, call_price)
                self.sim.onOrder(self.put_symbol, "sell", 0.1, put_price)
                self.call_active = self.put_active = True
                print(f"Entered straddle at {time}: {self.call_symbol}, {self.put_symbol}")
            else:
                print("No valid call/put symbols found for straddle entry.")

        # --- Exit logic ---
        if self.entry_price is not None and (self.call_active or self.put_active):
            deviation = abs(price - self.entry_price) / self.entry_price

            if deviation > 0.01 or self.pnl > 500 or self.pnl < -500:
                if self.call_active:
                    call_price = self.sim.currentPrice.get(self.call_symbol, price)
                    self.sim.onOrder(self.call_symbol, "buy", 0.1, call_price)
                    self.call_active = False
                if self.put_active:
                    put_price = self.sim.currentPrice.get(self.put_symbol, price)
                    self.sim.onOrder(self.put_symbol, "buy", 0.1, put_price)
                    self.put_active = False
                print(f"Exiting position at {time} | PnL: {self.pnl:.2f}")

    def onTradeConfirmation(self, symbol, side, quantity, price):
        side = side.lower()
        multiplier = 1 if side == "sell" else -1
        trade_value = price * quantity * multiplier
        self.pnl += trade_value
        self.trades.append((symbol, side.upper(), quantity, price))
