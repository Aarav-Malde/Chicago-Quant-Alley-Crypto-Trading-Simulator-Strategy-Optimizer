# Simulator.py

import os
import pandas as pd
from collections import defaultdict
import config
from Strategy import Strategy

class Simulator:
    def __init__(self):
        # Store merged data
        self.df = None

        # Strategy object receives reference to simulator
        self.strategy = Strategy(self)

        # Initialize trading state tracking
        self.currQuantity = defaultdict(float)
        self.buyValue = defaultdict(float)
        self.sellValue = defaultdict(float)
        self.currentPrice = {}

        # Slippage rate (can be tuned)
        self.slippage = 0.0001

    def readData(self):
        all_data = []

        for date in pd.date_range(config.simStartDate, config.simEndDate):
            date_str = date.strftime("%Y%m%d")
            for symbol in config.symbols:
                file_path = os.path.join(config.dataPath, date_str, f"{symbol}.csv")
                if os.path.exists(file_path):
                    df = pd.read_csv(file_path)
                    df["Symbol"] = symbol
                    all_data.append(df)
                else:
                    print(f"⚠️ Missing file: {file_path}")

        if all_data:
            self.df = pd.concat(all_data)
            self.df["time"] = pd.to_datetime(self.df["time"])
            self.df.sort_values("time", inplace=True)
            self.df.reset_index(drop=True, inplace=True)
        else:
            print("❌ No data found. Exiting.")
            exit()

    def onOrder(self, symbol, side, quantity, price):
        # Normalize order side to lowercase
        side = side.lower()

        # Adjust price with slippage
        adjusted_price = price * (1 + self.slippage) if side == "buy" else price * (1 - self.slippage)
        trade_value = adjusted_price * quantity

        # Update position and PnL tracking
        if side == "buy":
            self.currQuantity[symbol] += quantity
            self.buyValue[symbol] += trade_value
        elif side == "sell":
            self.currQuantity[symbol] -= quantity
            self.sellValue[symbol] += trade_value
        else:
            print(f"❌ Unknown order side: {side}")
            return

        # Confirm trade to strategy
        self.strategy.onTradeConfirmation(symbol, side, quantity, adjusted_price)

    def computePnL(self):
        total_pnl = 0
        for sym in self.currQuantity:
            qty = self.currQuantity[sym]
            price = self.currentPrice.get(sym, 0)
            pnl = self.sellValue[sym] - self.buyValue[sym] + qty * price
            total_pnl += pnl
        return total_pnl

    def startSimulation(self):
        pnl_log = []

        for _, row in self.df.iterrows():
            symbol = row["Symbol"]
            price = row["price"]
            time = row["time"]

            # Update market price
            self.currentPrice[symbol] = price

            # Pass row to strategy
            self.strategy.onMarketData(row)

            # Recalculate total PnL after each tick
            total_pnl = self.computePnL()
            pnl_log.append({"time": time, "pnl": total_pnl})

        # Save PnL log to file
        pd.DataFrame(pnl_log).to_csv("output_pnl.csv", index=False)
        print("✅ output_pnl.csv saved successfully.")

    def printPnl(self):
        print("\n--- Final P&L Report ---")
        total_pnl = 0
        for symbol in self.currQuantity:
            qty = self.currQuantity[symbol]
            price = self.currentPrice.get(symbol, 0)
            pnl = self.sellValue[symbol] - self.buyValue[symbol] + qty * price
            total_pnl += pnl
            print(f"{symbol}: {pnl:.2f}")
        print(f"🔹 Total PnL: {total_pnl:.2f}\n")

if __name__ == "__main__":
    sim = Simulator()
    sim.readData()
    sim.startSimulation()
    sim.printPnl()
