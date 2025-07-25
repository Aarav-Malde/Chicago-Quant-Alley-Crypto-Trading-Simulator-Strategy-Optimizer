import numpy as np
import matplotlib.pyplot as plt

np.random.seed(42)

# ==== Problem Setup ====
K = 10  # number of arms
T = 10000  # horizon cap for safety
delta = 0.1

# True reward means (unknown to agent)
true_means = np.random.rand(K)
best_arm = np.argmax(true_means)

# Tracking
counts = np.ones(K)  # pull each arm once
sums = np.array([np.random.binomial(1, p) for p in true_means])
means = sums / counts
history = []

# LIL-style confidence radius
def beta(n, delta=0.1):
    return np.sqrt((1.5 / n) * np.log(np.log(n + 1) + 1) + np.log(1 / delta))

# Initial pulls done, start main loop
total_pulls = K
while total_pulls < T:
    # Compute lil’UCB values
    bonuses = beta(counts, delta)
    lilucb_values = means + bonuses

    # Select arm with highest lil’UCB value
    arm = np.argmax(lilucb_values)

    # Pull arm
    reward = np.random.binomial(1, true_means[arm])
    counts[arm] += 1
    sums[arm] += reward
    means[arm] = sums[arm] / counts[arm]
    total_pulls += 1

    # Track for graph
    history.append((means.copy(), counts.copy(), arm))

# Final arm selection
final_arm = np.argmax(means)

# ==== Terminal Output ====
print("\n========== lil'UCB Algorithm Summary ==========")
print(f"True Means: {np.round(true_means, 3)}")
print(f"True Best Arm: {best_arm} (μ = {true_means[best_arm]:.3f})")
print(f"Arm Selected by lil'UCB: {final_arm} (μ = {true_means[final_arm]:.3f})")
print(f"Correct Selection: {'Yes' if final_arm == best_arm else 'No'}")
print(f"Total Pulls: {total_pulls}")
print("Pulls per Arm:", counts.astype(int))
print("==============================================\n")

# ==== Plot ====
plt.figure(figsize=(10, 4))
plt.bar(range(K), true_means, alpha=0.6, label="True Means")
plt.bar(range(K), means, alpha=0.4, label="Estimated Means")
plt.axvline(final_arm, color='green', linestyle='--', label="Selected Arm")
plt.axvline(best_arm, color='red', linestyle='--', label="True Best Arm")
plt.xlabel("Arm")
plt.ylabel("Mean Reward")
plt.title("lil'UCB: True vs Estimated Means")
plt.legend()
plt.grid(True)
plt.tight_layout()
plt.show()
