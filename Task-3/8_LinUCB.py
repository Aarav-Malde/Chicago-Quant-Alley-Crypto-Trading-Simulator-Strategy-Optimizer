# LinUCB Algorithm for Contextual Multi-Armed Bandit Problem
import numpy as np
import matplotlib.pyplot as plt

# Set random seed
np.random.seed(42)

# Parameters
K = 10               # Number of arms
d = 20               # Context vector dimension
T = 10_000           # Horizon
alpha = 0.1          # Exploration parameter

# Generate fixed weight vector for each arm (true reward model)
true_theta = [np.random.randn(d) for _ in range(K)]

# Initialize per-arm matrices
A = [np.identity(d) for _ in range(K)]         # d x d matrix for each arm
b = [np.zeros(d) for _ in range(K)]            # d-dim vector for each arm

# Stats
cumulative_reward = np.zeros(T)
regret = np.zeros(T)
best_arm_count = np.zeros(K)

# Simulate
for t in range(T):
    # Generate context vector for this round
    x_t = np.random.randn(K, d)  # one context per arm

    # Compute estimated UCB for each arm
    p_t = np.zeros(K)
    for a in range(K):
        A_inv = np.linalg.inv(A[a])
        theta_hat = A_inv @ b[a]
        ucb = alpha * np.sqrt(x_t[a] @ A_inv @ x_t[a])
        p_t[a] = x_t[a] @ theta_hat + ucb

    # Select arm with highest UCB
    arm = np.argmax(p_t)

    # Generate stochastic reward using true θ with noise
    noise = np.random.normal(scale=0.1)
    reward = x_t[arm] @ true_theta[arm] + noise

    # Find the best arm in hindsight
    expected_rewards = [x_t[a] @ true_theta[a] for a in range(K)]
    optimal_reward = max(expected_rewards)
    optimal_arm = np.argmax(expected_rewards)
    best_arm_count[optimal_arm] += 1

    # Update stats
    cumulative_reward[t] = cumulative_reward[t - 1] + reward if t > 0 else reward
    regret[t] = regret[t - 1] + (optimal_reward - reward) if t > 0 else (optimal_reward - reward)

    # Update A and b for chosen arm
    A[arm] += np.outer(x_t[arm], x_t[arm])
    b[arm] += reward * x_t[arm]

# Plot results
plt.figure(figsize=(15, 5))

plt.subplot(1, 2, 1)
plt.plot(cumulative_reward, label="LinUCB Cumulative Reward")
plt.title("Cumulative Reward")
plt.xlabel("Time")
plt.ylabel("Reward")
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
print("\n============== LinUCB Summary ==============")
print(f"Total Time Steps: {T}")
print(f"Number of Arms: {K}")
print(f"Context Dimension: {d}")
print(f"Exploration Parameter (alpha): {alpha}")
print(f"Final Cumulative Reward: {cumulative_reward[-1]:.2f}")
print(f"Final Regret: {regret[-1]:.2f}")
print(f"Most Optimal Arm Selection Count: {int(best_arm_count.max())} times (Arm {best_arm_count.argmax()})")
print("============================================\n")
