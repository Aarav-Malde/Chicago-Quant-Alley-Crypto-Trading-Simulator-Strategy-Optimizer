#Weighted Majority Algorithm for Multi-Armed Bandit Problem
import numpy as np
import matplotlib.pyplot as plt

# Set random seed
np.random.seed(42)

# Parameters
K = 10           # Number of experts/arms
T = 10_000       # Horizon
eta = 0.5        # Learning rate (0 < eta <= 1)

# Simulate adversarial losses: random loss between 0 and 1 for each arm
true_losses = np.random.rand(T, K)  # shape (T, K)

# Initialize weights
weights = np.ones(K)
cumulative_loss = np.zeros(T)
regret = np.zeros(T)
best_arm = np.argmin(np.sum(true_losses, axis=0))
best_arm_losses = np.cumsum(true_losses[:, best_arm])
wm_losses = []

# Weighted Majority Algorithm loop
for t in range(T):
    # Normalize weights to form a probability distribution
    probs = weights / np.sum(weights)

    # Choose an arm based on the probabilities
    action = np.random.choice(K, p=probs)

    # Observe full losses for all arms (full information setting)
    losses = true_losses[t]

    # Update weights (multiplicative update rule)
    weights *= (1 - eta) ** losses

    # Update stats
    loss_t = losses[action]
    wm_losses.append(loss_t)
    cumulative_loss[t] = cumulative_loss[t - 1] + loss_t if t > 0 else loss_t
    regret[t] = cumulative_loss[t] - best_arm_losses[t]

# Plot results
plt.figure(figsize=(15, 5))

plt.subplot(1, 2, 1)
plt.plot(cumulative_loss, label="WM Cumulative Loss")
plt.plot(best_arm_losses, label="Best Arm Loss", linestyle="--")
plt.title("Cumulative Loss")
plt.xlabel("Time")
plt.ylabel("Loss")
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
print("\n========== Weighted Majority Summary ==========")
print(f"Total Time Steps: {T}")
print(f"Number of Arms (Experts): {K}")
print(f"Learning Rate (η): {eta}")
print(f"Best Arm (Least Total Loss): {best_arm} (Total Loss = {best_arm_losses[-1]:.2f})")
print(f"Final Cumulative Loss (WM): {cumulative_loss[-1]:.2f}")
print(f"Final Regret: {regret[-1]:.2f}")
print(f"Average Loss per Round: {np.mean(wm_losses):.4f}")
print("===============================================\n")
