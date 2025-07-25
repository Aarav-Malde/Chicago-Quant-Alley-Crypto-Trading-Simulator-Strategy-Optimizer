import numpy as np
import matplotlib.pyplot as plt

# Parameters
np.random.seed(42)
K = 10                 # number of arms
T = 5000               # total budget (pulls)
rounds = int(np.log2(K)) + 1
arms = list(range(K))

# Simulate true means (unknown to agent)
true_means = np.random.rand(K)
best_arm = np.argmax(true_means)

# Store results
pulls_per_round = []
estimated_means = np.zeros(K)
pull_counts = np.zeros(K)

# Halving Loop
remaining_arms = arms.copy()
for r in range(rounds):
    n_arms = len(remaining_arms)
    pulls_per_arm = T // (n_arms * rounds)
    pulls_per_round.append((r+1, n_arms, pulls_per_arm))

    for arm in remaining_arms:
        rewards = np.random.binomial(1, true_means[arm], pulls_per_arm)
        estimated_means[arm] = np.mean(rewards)
        pull_counts[arm] += pulls_per_arm

    # Eliminate half the arms with lowest estimated reward
    sorted_arms = sorted(remaining_arms, key=lambda x: estimated_means[x], reverse=True)
    remaining_arms = sorted_arms[:max(1, len(sorted_arms)//2)]


# Final selected arm
final_arm = remaining_arms[0]

# Terminal Output
print("\n========== Halving Algorithm Summary ==========")
print(f"Total Arms: {K}")
print(f"Total Budget (T): {T}")
print(f"True Means: {np.round(true_means, 3)}")
print(f"True Best Arm: {best_arm} (μ = {true_means[best_arm]:.3f})")
print(f"Arm Selected by Halving: {final_arm} (μ = {true_means[final_arm]:.3f})")
print(f"Correct Selection: {'Yes' if final_arm == best_arm else 'No'}")
print("Pulls per Round:")
for r, n_arms, pulls in pulls_per_round:
    print(f"  Round {r}: {n_arms} arms × {pulls} pulls")
print("===============================================\n")

# Plot 1: True means vs estimated
plt.figure(figsize=(10, 4))
plt.bar(range(K), true_means, alpha=0.6, label='True Mean')
plt.bar(range(K), estimated_means, alpha=0.4, label='Estimated Mean')
plt.axvline(final_arm, color='green', linestyle='--', label='Selected Arm')
plt.axvline(best_arm, color='red', linestyle='--', label='True Best Arm')
plt.title("Halving Algorithm: True vs Estimated Means")
plt.xlabel("Arm")
plt.ylabel("Mean Reward")
plt.legend()
plt.grid(True)
plt.tight_layout()
plt.show()


