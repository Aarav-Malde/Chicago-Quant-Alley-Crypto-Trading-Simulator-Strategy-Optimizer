# Thompson Sampling for Multi-Armed Bandit Problem

import numpy as np
import matplotlib.pyplot as plt

# Set random seed
np.random.seed(42)

# Parameters
K = 10
T = 10_000

# True mean rewards for each arm (Bernoulli bandit)
true_means = np.random.rand(K)
best_arm = np.argmax(true_means)

# Function to pull an arm (Bernoulli reward)
def pull_arm(i):
    return np.random.binomial(1, true_means[i])

# Initialize prior parameters for Beta distributions
alpha = np.ones(K)  # Successes + 1
beta_param = np.ones(K)  # Failures + 1

# Stats trackers
cumulative_reward = np.zeros(T)
regret = np.zeros(T)
best_arm_selections = np.zeros(T)
pulls = np.zeros(K)

# Thompson Sampling loop
for t in range(T):
    # Sample from the posterior (Beta distribution) for each arm
    theta_samples = np.random.beta(alpha, beta_param)
    action = np.argmax(theta_samples)

    # Pull the arm and observe reward
    reward = pull_arm(action)

    # Update Beta parameters
    alpha[action] += reward
    beta_param[action] += (1 - reward)

    # Update stats
    pulls[action] += 1
    cumulative_reward[t] = cumulative_reward[t - 1] + reward if t > 0 else reward
    best_arm_selections[t] = 1 if action == best_arm else 0
    regret[t] = regret[t - 1] + (true_means[best_arm] - true_means[action]) if t > 0 else (true_means[best_arm] - true_means[action])

# Plot results
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

# Terminal summary
print("\n========== Thompson Sampling Summary ==========")
print(f"Total Time Steps: {T}")
print(f"Number of Arms: {K}")
print(f"True Mean Rewards: {np.round(true_means, 3)}")
print(f"Arm Pull Counts: {pulls.astype(int)}")
print(f"Best Arm (True): {best_arm} (True Mean = {true_means[best_arm]:.3f})")
chosen_arm = np.argmax(alpha / (alpha + beta_param))
print(f"Most Selected Arm (Estimated Best): {chosen_arm} (Estimated Mean ≈ {alpha[chosen_arm] / (alpha[chosen_arm] + beta_param[chosen_arm]):.3f})")
print(f"Times Best Arm Selected: {int(np.sum(best_arm_selections))}")
print(f"Percentage Best Arm Selected: {100 * np.mean(best_arm_selections):.2f}%")
print(f"Total Cumulative Reward: {cumulative_reward[-1]:.2f}")
print(f"Final Cumulative Regret: {regret[-1]:.2f}")
print("===============================================\n")
