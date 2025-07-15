import os
from datetime import datetime

# Automatically get the path relative to the current file's directory
project_root = os.path.dirname(os.path.abspath(__file__))
dataPath = os.path.join(project_root, "data")

simStartDate = "2024-06-01"
simEndDate = "2024-06-01"

symbols = [
    "MARK-BTCUSDT",
    "MARK-C-BTC-70000-20240601",
    "MARK-P-BTC-65000-20240601"
]
