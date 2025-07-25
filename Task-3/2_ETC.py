# Explore-Then-Commit (ETC) Algorithm Implementation
import numpy as np
import matplotlib.pyplot as plt

# Set random seed
np.random.seed(42)

# Parameters
K = 10
T = 10_000
reward_type = "bernoulli"  # Change to "gaussian" if needed
exploration_rounds_per_arm = 200  # Exploration phase: total = K * this

# Define reward distributions
if reward_type == "bernoulli":
    true_means = np.random.rand(K)
    def pull_arm(i):
        return np.random.binomial(1, true_means[i])
elif reward_type == "gaussian":
    true_means = np.random.normal(0, 1, K)
    def pull_arm(i):
        return np.random.normal(true_means[i], 1.0)

# Initialize tracking variables
Q = np.zeros(K)             # Estimated mean reward
N = np.zeros(K)             # Number of pulls per arm
cumulative_reward = np.zeros(T)
best_arm = np.argmax(true_means)
best_arm_selections = np.zeros(T)
regret = np.zeros(T)

# Phase 1: Exploration
exploration_total = K * exploration_rounds_per_arm
t = 0
for arm in range(K):
    for _ in range(exploration_rounds_per_arm):
        reward = pull_arm(arm)
        N[arm] += 1
        Q[arm] += (reward - Q[arm]) / N[arm]
        cumulative_reward[t] = cumulative_reward[t - 1] + reward if t > 0 else reward
        best_arm_selections[t] = 1 if arm == best_arm else 0
        regret[t] = regret[t - 1] + (true_means[best_arm] - true_means[arm]) if t > 0 else (true_means[best_arm] - true_means[arm])
        t += 1

# Phase 2: Commit to best estimated arm
commit_arm = np.argmax(Q)
for t in range(exploration_total, T):
    reward = pull_arm(commit_arm)
    cumulative_reward[t] = cumulative_reward[t - 1] + reward
    best_arm_selections[t] = 1 if commit_arm == best_arm else 0
    regret[t] = regret[t - 1] + (true_means[best_arm] - true_means[commit_arm])

# === Plotting ===
plt.figure(figsize=(15, 5))

plt.subplot(1, 3, 1)
plt.plot(cumulative_reward)
plt.title("Cumulative Reward")
plt.xlabel("Time")
plt.ylabel("Reward")
plt.grid(True)

plt.subplot(1, 3, 2)
plt.plot(np.cumsum(best_arm_selections) / (np.arange(T) + 1))
plt.title("Best Arm Selection Frequency")
plt.xlabel("Time")
plt.ylabel("Proportion")
plt.grid(True)

plt.subplot(1, 3, 3)
plt.plot(regret, color='red')
plt.title("Cumulative Regret")
plt.xlabel("Time")
plt.ylabel("Regret")
plt.grid(True)

plt.tight_layout()
plt.show()

# === Terminal Output ===
print("\n========== Explore-Then-Commit Summary ==========")
print(f"Total Time Steps: {T}")
print(f"Number of Arms: {K}")
print(f"Exploration per Arm: {exploration_rounds_per_arm}")
print(f"Exploration Phase Length: {exploration_total}")
print(f"True Mean Rewards: {np.round(true_means, 3)}")
print(f"Estimated Q-values after Exploration: {np.round(Q, 3)}")
print(f"Number of Pulls per Arm during Exploration: {N.astype(int)}")
print(f"Best True Arm: {best_arm} (True Mean = {true_means[best_arm]:.3f})")
print(f"Committed Arm: {commit_arm} (Estimated Mean = {Q[commit_arm]:.3f})")
print(f"Times Best Arm Selected: {int(np.sum(best_arm_selections))}")
print(f"Percentage Best Arm Selected: {100 * np.mean(best_arm_selections):.2f}%")
print(f"Total Cumulative Reward: {cumulative_reward[-1]:.2f}")
print(f"Final Cumulative Regret: {regret[-1]:.2f}")
print("===============================================\n")
