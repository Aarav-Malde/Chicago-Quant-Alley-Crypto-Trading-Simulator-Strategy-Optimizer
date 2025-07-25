# Epsilon-Greedy Bandit Algorithm Implementation
import numpy as np
import matplotlib.pyplot as plt

# Set random seed for reproducibility
np.random.seed(42)

# Parameters
K = 10                  # Number of arms
T = 10_000              # Time horizon
epsilon = 0.1           # Exploration probability
reward_type = "bernoulli"  # Options: "bernoulli" or "gaussian"

# Initialize true reward distributions
if reward_type == "bernoulli":
    true_means = np.random.rand(K)
    def pull_arm(i):
        return np.random.binomial(1, true_means[i])
elif reward_type == "gaussian":
    true_means = np.random.normal(0, 1, K)
    def pull_arm(i):
        return np.random.normal(true_means[i], 1.0)

# Initialize estimated rewards and counts
Q = np.zeros(K)    # Estimated mean rewards
N = np.zeros(K)    # Number of times each arm is pulled

# Metrics to track
cumulative_reward = np.zeros(T)
best_arm = np.argmax(true_means)
best_arm_selections = np.zeros(T)
regret = np.zeros(T)

# ε-greedy loop
for t in range(T):
    # Exploration vs exploitation
    if np.random.rand() < epsilon:
        action = np.random.randint(K)  # explore
    else:
        action = np.argmax(Q)          # exploit

    # Pull the selected arm and observe reward
    reward = pull_arm(action)

    # Update estimates
    N[action] += 1
    Q[action] += (reward - Q[action]) / N[action]  # Incremental average

    # Track metrics
    cumulative_reward[t] = cumulative_reward[t - 1] + reward if t > 0 else reward
    best_arm_selections[t] = 1 if action == best_arm else 0
    regret[t] = regret[t - 1] + (true_means[best_arm] - true_means[action]) if t > 0 else (true_means[best_arm] - true_means[action])

# Compute upper confidence bounds (for visualization only)
confidence_bounds = Q + np.sqrt(2 * np.log(np.maximum(1, T)) / np.maximum(1, N))

# ===========================
# Plot the results
# ===========================

plt.figure(figsize=(15, 5))

# Plot cumulative reward
plt.subplot(1, 3, 1)
plt.plot(cumulative_reward, label='Cumulative Reward')
plt.title("Cumulative Reward")
plt.xlabel("Time")
plt.ylabel("Reward")
plt.grid(True)

# Plot frequency of best arm selection
plt.subplot(1, 3, 2)
plt.plot(np.cumsum(best_arm_selections) / (np.arange(T) + 1), label='Best Arm Selection Frequency')
plt.title("Best Arm Selection Frequency")
plt.xlabel("Time")
plt.ylabel("Proportion")
plt.grid(True)

# Plot cumulative regret
plt.subplot(1, 3, 3)
plt.plot(regret, label='Cumulative Regret', color='red')
plt.title("Cumulative Regret")
plt.xlabel("Time")
plt.ylabel("Regret")
plt.grid(True)

plt.tight_layout()
plt.show()


print("\n========== ε-Greedy Bandit Summary ==========")
print(f"Total Time Steps: {T}")
print(f"Number of Arms: {K}")
print(f"Epsilon (Exploration Rate): {epsilon}")
print(f"True Mean Rewards: {np.round(true_means, 3)}")
print(f"Estimated Q-values: {np.round(Q, 3)}")
print(f"Number of Pulls per Arm: {N.astype(int)}")
print(f"Best Arm: {best_arm} (True Mean = {true_means[best_arm]:.3f})")
print(f"Times Best Arm Selected: {int(np.sum(best_arm_selections))}")
print(f"Percentage Best Arm Selected: {100 * np.mean(best_arm_selections):.2f}%")
print(f"Total Cumulative Reward: {cumulative_reward[-1]:.2f}")
print(f"Final Cumulative Regret: {regret[-1]:.2f}")
print("=============================================\n")
