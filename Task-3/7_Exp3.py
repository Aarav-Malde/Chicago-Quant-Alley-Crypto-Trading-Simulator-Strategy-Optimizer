# EXP3 Algorithm for Adversarial Multi-Armed Bandit Problem
import numpy as np
import matplotlib.pyplot as plt

# Set random seed
np.random.seed(42)

# Parameters
K = 10           # Number of arms
T = 10_000       # Horizon
gamma = 0.07     # Exploration parameter (0 < gamma ≤ 1)

# Simulate adversarial rewards (each in [0,1])
# This example just uses fixed random reward matrix for simplicity
true_rewards = np.random.rand(T, K)
best_arm = np.argmax(np.sum(true_rewards, axis=0))
best_arm_cumulative_reward = np.cumsum(true_rewards[:, best_arm])

# Initialize weights and probabilities
weights = np.ones(K)
probs = np.ones(K) / K

# Stats
cumulative_reward = np.zeros(T)
regret = np.zeros(T)
pulls = np.zeros(K)

# EXP3 loop
for t in range(T):
    # Compute probabilities from weights
    probs = (1 - gamma) * (weights / np.sum(weights)) + gamma / K

    # Select an arm according to probabilities
    arm = np.random.choice(K, p=probs)

    # Observe the reward for the selected arm (bandit feedback only)
    reward = true_rewards[t, arm]

    # Estimate the reward for that arm
    estimated_reward = reward / probs[arm]

    # Update weight for that arm
    weights[arm] *= np.exp(gamma * estimated_reward / K)

    # Track stats
    pulls[arm] += 1
    cumulative_reward[t] = cumulative_reward[t - 1] + reward if t > 0 else reward
    regret[t] = regret[t - 1] + (true_rewards[t, best_arm] - reward) if t > 0 else (true_rewards[t, best_arm] - reward)

# Plot results
plt.figure(figsize=(15, 5))

plt.subplot(1, 2, 1)
plt.plot(cumulative_reward, label="EXP3 Cumulative Reward")
plt.plot(best_arm_cumulative_reward, label="Best Arm Reward", linestyle="--")
plt.title("Cumulative Reward")
plt.xlabel("Time")
plt.ylabel("Reward")
plt.legend()
plt.grid(True)

plt.subplot(1, 2, 2)
plt.plot(regret, color='red')
plt.title("Cumulative Regret")
plt.xlabel("Time")
plt.ylabel("Regret")
plt.grid(True)

plt.tight_layout()
plt.show()

# Terminal output
print("\n============== EXP3 Summary ==============")
print(f"Total Time Steps: {T}")
print(f"Number of Arms: {K}")
print(f"Exploration Parameter (γ): {gamma}")
print(f"True Reward Sums: {np.round(np.sum(true_rewards, axis=0), 2)}")
print(f"Best Arm: {best_arm} (Total Reward = {best_arm_cumulative_reward[-1]:.2f})")
print(f"EXP3 Cumulative Reward: {cumulative_reward[-1]:.2f}")
print(f"Final Regret: {regret[-1]:.2f}")
print(f"Arm Pull Counts: {pulls.astype(int)}")
print("==========================================\n")
