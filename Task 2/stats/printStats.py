# stats/printStats.py

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import os

def load_data(file_path="output_pnl.csv"):
    print(f"Loading file: {file_path}")
    try:
        df = pd.read_csv(file_path, parse_dates=["time"])
        if df.empty:
            print("The CSV is empty. No data to analyze.")
            return None
        df.sort_values("time", inplace=True)
        return df
    except Exception as e:
        print(f"Error reading file: {e}")
        return None

def compute_metrics(df):
    df["cumulative_pnl"] = df["pnl"].cumsum()
    df["returns"] = df["pnl"].diff().fillna(0)

    mean_pnl = df["pnl"].mean()
    median_pnl = df["pnl"].median()
    std_pnl = df["pnl"].std()

    returns_std = df["returns"].std()
    sharpe_ratio = 0
    if returns_std != 0:
        sharpe_ratio = df["returns"].mean() / returns_std * np.sqrt(252)

    df["cummax"] = df["cumulative_pnl"].cummax()
    df["drawdown"] = df["cumulative_pnl"] - df["cummax"]
    max_drawdown = df["drawdown"].min()

    var_95 = np.percentile(df["returns"], 5)
    es_95 = df["returns"][df["returns"] <= var_95].mean()

    return {
        "mean": mean_pnl,
        "median": median_pnl,
        "std": std_pnl,
        "sharpe": sharpe_ratio,
        "drawdown": max_drawdown,
        "var_95": var_95,
        "es_95": es_95
    }

def plot_curves(df, output_dir="plots"):
    os.makedirs(output_dir, exist_ok=True)

    plt.figure(figsize=(10, 5))
    plt.plot(df["time"], df["cumulative_pnl"], label="Cumulative PnL", color="blue")
    plt.title("Cumulative PnL")
    plt.xlabel("Time")
    plt.ylabel("PnL")
    plt.grid(True)
    plt.legend()
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "cumulative_pnl.png"))
    plt.show()
    plt.close()

    plt.figure(figsize=(10, 5))
    plt.plot(df["time"], df["drawdown"], label="Drawdown", color="red")
    plt.title("Drawdown Curve")
    plt.xlabel("Time")
    plt.ylabel("Drawdown")
    plt.grid(True)
    plt.legend()
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "drawdown.png"))
    plt.show()  
    plt.close()

def analyze_pnl(file_path="output_pnl.csv"):
    df = load_data(file_path)
    if df is None:
        return

    metrics = compute_metrics(df)

    print("\nPerformance Metrics:")
    print(f"Mean PnL: {metrics['mean']:.2f}")
    print(f"Median PnL: {metrics['median']:.2f}")
    print(f"Std Dev: {metrics['std']:.2f}")
    print(f"Sharpe Ratio: {metrics['sharpe']:.2f}")
    print(f"Max Drawdown: {metrics['drawdown']:.2f}")
    print(f"VaR (95%): {metrics['var_95']:.2f}")
    print(f"Expected Shortfall (95%): {metrics['es_95']:.2f}")

    plot_curves(df)

# Entry point
if __name__ == "__main__":
    analyze_pnl()
