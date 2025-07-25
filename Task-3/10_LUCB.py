import numpy as np
import matplotlib.pyplot as plt

np.random.seed(42)

# Problem setup
K = 10                      # number of arms
delta = 0.1                 # confidence level (1 - δ)
epsilon = 0.05              # accuracy
max_pulls = 10000           # safety cap

# Simulated true means
true_means = np.random.rand(K)
best_arm = np.argmax(true_means)

# Tracking
counts = np.zeros(K)
sums = np.zeros(K)
means = np.zeros(K)
rounds = 0
history = []

# Pull function
def pull(arm):
    return np.random.binomial(1, true_means[arm])

# Confidence radius
def beta(n):
    return np.sqrt((1 / (2 * n)) * np.log(3 * np.log(max(n, 2)) / delta))

# Initialize: pull each arm once
for arm in range(K):
    reward = pull(arm)
    counts[arm] += 1
    sums[arm] += reward
    means[arm] = sums[arm] / counts[arm]

# LUCB loop
while True:
    rounds += 1

    # Calculate confidence bounds
    ucbs = means + beta(counts)
    lcbs = means - beta(counts)

    # Top arm by mean
    i_t = np.argmax(means)
    
    # Best competitor: max UCB gap w.r.t i_t
    gaps = ucbs - lcbs[i_t]
    gaps[i_t] = -np.inf  # exclude top arm
    j_t = np.argmax(gaps)

    # Pull both arms
    for arm in [i_t, j_t]:
        reward = pull(arm)
        counts[arm] += 1
        sums[arm] += reward
        means[arm] = sums[arm] / counts[arm]

    # Track
    history.append((i_t, j_t, means.copy(), counts.copy()))

    # Stopping condition
    if ucbs[j_t] - lcbs[i_t] < epsilon or np.sum(counts) > max_pulls:
        break

# Final selection
selected_arm = i_t

# ==== Terminal Output ====
print("\n========== LUCB Algorithm Summary ==========")
print(f"True Means: {np.round(true_means, 3)}")
print(f"True Best Arm: {best_arm} (μ = {true_means[best_arm]:.3f})")
print(f"Arm Selected by LUCB: {selected_arm} (μ = {true_means[selected_arm]:.3f})")
print(f"Correct Selection: {'Yes' if selected_arm == best_arm else 'No'}")
print(f"Total Pulls: {int(np.sum(counts))}")
print(f"Rounds until termination: {rounds}")
print("============================================\n")

# ==== Plotting ====
plt.figure(figsize=(10, 4))
plt.bar(range(K), true_means, alpha=0.6, label="True Means")
plt.bar(range(K), means, alpha=0.4, label="Estimated Means")
plt.axvline(selected_arm, color='green', linestyle='--', label="Selected Arm")
plt.axvline(best_arm, color='red', linestyle='--', label="True Best Arm")
plt.xlabel("Arm")
plt.ylabel("Mean Reward")
plt.title("LUCB: True vs Estimated Means")
plt.legend()
plt.grid(True)
plt.tight_layout()
plt.show()
