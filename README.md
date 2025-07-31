Crypto Quant Strategy Simulator & Optimizer  
Summer of Code 2025 – Chicago Quant Alley

## Overview

This repository documents the work completed as part of the Summer of Code 2025, under the Chicago Quant Alley program. The project is centered on building a robust pipeline for developing, simulating, and optimizing algorithmic trading strategies using real-world crypto options data. It consists of three main components:

1. Construction of a historical data pipeline using the Delta Exchange API.
2. Development of a backtesting simulator for options strategies.
3. Implementation and evaluation of Multi-Armed Bandit algorithms for adaptive strategy selection.

The project bridges theoretical foundations in quantitative finance with practical engineering and simulation tools, aiming to replicate key aspects of real-world algorithmic trading systems.

---------------

## Task 1: BTC Options Data Pipeline

The first task involved building a structured data collection pipeline. Using the Delta Exchange API, we fetched BTCUSD options data including all active calls and puts across a range of strikes and expiry dates.

The data was:
- Retrieved through scripted API calls.
- Stored in CSV format, categorized by date and symbol.
- Cleaned and standardized for compatibility with later simulation components.

This task provided us with a foundational dataset for building realistic trading strategies and testing them in a controlled environment.

---------------

## Probability and Stochastic Processes for Finance (NPTEL Courses)

As part of the theoretical grounding for the simulator and optimization tasks, one of the contributors completed the NPTEL courses: *Probability and Stochastic Processes for Finance I and II*.

The coursework covered the following key areas:

- Axiomatic probability theory and classical paradoxes.
- Random variables, expectations, and convergence theorems.
- Conditional expectation and the Radon-Nikodym derivative.
- Brownian motion, quadratic variation, and stochastic calculus.
- Ito’s Lemma, stochastic differential equations, black-scholes equation and martingale theory.
- Risk-neutral pricing using Girsanov’s theorem.
- Derivation and application of the Black-Scholes formula.
- Pricing of derivatives and the martingale representation theorem.

This content provided the mathematical foundation for modeling financial uncertainty and designing simulation logic based on real-world principles.

------------------------

## Task 2: Crypto Options Strategy Simulator

The second task focused on building a modular, event-driven backtesting engine for simulating mid-frequency crypto options strategies using historical data.

Key features of the simulator include:

- Tick-by-tick processing of options and futures price data.
- Trade execution simulation with slippage modeling.
- Real-time PnL tracking and logging.
- Modular strategy integration with callback methods.

A sample strategy was implemented to demonstrate the framework: a short straddle that initiates at 1 PM by selling 0.1 quantity each of the nearest call and put options (within ±2% of spot price). The strategy exits either upon a 1% move in the underlying price or upon reaching a profit/loss threshold of $500.

In a representative simulation:

- The strategy sold options for a combined value of $64.99.
- It bought them back later at $61.06.
- The net realized PnL was approximately $3.93.

While this is a small gain, the result validates the simulator’s correct handling of trade lifecycle, position management, and event-driven strategy execution. The simulation loop was also optimized with NumPy, improving computational performance by approximately 3.6%.

-----------------------------------------

## Task 3: Strategy Optimization via Multi-Armed Bandits

The third task introduced an adaptive decision-making layer to the strategy selection process. We implemented a suite of Multi-Armed Bandit (MAB) algorithms to evaluate and optimize strategy performance under uncertainty.

The following classes of MAB algorithms were developed and tested:

- Stochastic Bandits: Epsilon-Greedy, UCB1, KL-UCB, and Thompson Sampling.
- Adversarial Bandits: Exp3 and Weighted Majority.
- Contextual Bandits: LinUCB with simulated market features.
- Pure Exploration: Halving, LUCB, KL-LUCB, and lil'UCB.

The algorithms were evaluated on simulated 10-arm environments using both Bernoulli and Gaussian reward distributions over 10,000 iterations. The evaluation metric was cumulative regret.

Key observations included:

- Thompson Sampling consistently performed best in static environments.
- Exp3 adapted more effectively in adversarial or changing reward conditions.
- Halving identified the best arm with high accuracy and minimal samples in fixed-budget scenarios.

These results reinforced the importance of algorithm choice in real-time strategy selection and emphasized the trade-off between exploration and exploitation in live systems.

------------------------------

## Repository Structure
├── Task1_DataPipeline/ # Scripts for data fetching and storage
├── Task2_Simulator/ # Simulation engine, strategy logic, configuration files
├── Task3_Bandits/ # MAB algorithm implementations and experiments
├── stats/ # Post-simulation analysis and metrics
├── output_pnl.csv # Sample PnL output from backtesting
├── REPORT_FOR_SOC.pdf # NPTEL course summary and theoretical notes
├── README.md # Project documentation

-------------------------------

## Authors

- Aarav Malde
  - Led the implementation of Multi-Armed Bandit algorithms  
  - Built reward environments and evaluation pipelines  
  - Analyzed performance of stochastic and adversarial bandits  
- Harsh Modak
  - Developed the crypto options simulator  
  - Completed NPTEL coursework in financial mathematics  
  - Designed and implemented the short straddle strategy
