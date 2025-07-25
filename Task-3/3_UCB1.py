import numpy as np
import matplotlib.pyplot as plt

# Set random seed
np.random.seed(42)

# Parameters
K = 10
T = 10_000
reward_type = "bernoulli"  # or "gaussian"

# Define reward distributions
if reward_type == "bernoulli":
    true_means = np.random.rand(K)
    def pull_arm(i):
        return np.random.binomial(1, true_means[i])
elif reward_type == "gaussian":
    true_means = np.random.normal(0, 1, K)
    def pull_arm(i):
        return np.random.normal(true_means[i], 1.0)

# Initialize
Q = np.zeros(K)           # Estimated mean rewards
N = np.zeros(K)           # Number of pulls per arm
cumulative_reward = np.zeros(T)
regret = np.zeros(T)
best_arm = np.argmax(true_means)
best_arm_selections = np.zeros(T)

# Step 1: Pull each arm once
for i in range(K):
    reward = pull_arm(i)
    Q[i] = reward
    N[i] = 1
    cumulative_reward[i] = cumulative_reward[i - 1] + reward if i > 0 else reward
    best_arm_selections[i] = 1 if i == best_arm else 0
    regret[i] = regret[i - 1] + (true_means[best_arm] - true_means[i]) if i > 0 else (true_means[best_arm] - true_means[i])

# Step 2: UCB1 Algorithm
for t in range(K, T):
    ucb_values = Q + np.sqrt((2 * np.log(t)) / N)
    action = np.argmax(ucb_values)
    reward = pull_arm(action)

    # Update
    N[action] += 1
    Q[action] += (reward - Q[action]) / N[action]

    cumulative_reward[t] = cumulative_reward[t - 1] + reward
    best_arm_selections[t] = 1 if action == best_arm else 0
    regret[t] = regret[t - 1] + (true_means[best_arm] - true_means[action])

# Plotting
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

print("\n========== UCB1 Bandit Summary ==========")
print(f"Total Time Steps: {T}")
print(f"Number of Arms: {K}")
print(f"True Mean Rewards: {np.round(true_means, 3)}")
print(f"Estimated Q-values: {np.round(Q, 3)}")
print(f"Number of Pulls per Arm: {N.astype(int)}")
print(f"Best Arm (True): {best_arm} (True Mean = {true_means[best_arm]:.3f})")
ucb_chosen_arm = np.argmax(Q)
print(f"Most Selected Arm (Estimated Best): {ucb_chosen_arm} (Q = {Q[ucb_chosen_arm]:.3f})")
print(f"Times Best Arm Selected: {int(np.sum(best_arm_selections))}")
print(f"Percentage Best Arm Selected: {100 * np.mean(best_arm_selections):.2f}%")
print(f"Total Cumulative Reward: {cumulative_reward[-1]:.2f}")
print(f"Final Cumulative Regret: {regret[-1]:.2f}")
print("=========================================\n")
