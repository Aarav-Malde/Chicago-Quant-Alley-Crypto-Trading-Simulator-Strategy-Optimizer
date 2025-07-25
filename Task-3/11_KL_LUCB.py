import numpy as np
import matplotlib.pyplot as plt
from scipy.optimize import bisect
from math import log

np.random.seed(42)

# ----- KL divergence for Bernoulli -----
def kl_bernoulli(p, q):
    p = min(max(p, 1e-15), 1 - 1e-15)
    q = min(max(q, 1e-15), 1 - 1e-15)
    return p * log(p / q) + (1 - p) * log((1 - p) / (1 - q))

# ----- KL-UCB upper bound solver -----
def kl_ucb(mean, n, t, delta):
    """Returns max q such that KL(mean || q) <= beta"""
    beta = log(t) + 3 * log(log(t)) + log(1 / delta)
    def func(q): return kl_bernoulli(mean, q) - beta / n
    try:
        return bisect(func, mean, 1 - 1e-10, maxiter=50)
    except ValueError:
        return 1.0

# ----- KL-LCB lower bound solver -----
def kl_lcb(mean, n, t, delta):
    """Returns min q such that KL(mean || q) <= beta"""
    beta = log(t) + 3 * log(log(t)) + log(1 / delta)
    def func(q): return kl_bernoulli(mean, q) - beta / n
    try:
        return bisect(func, 1e-10, mean, maxiter=50)
    except ValueError:
        return 0.0

# ----- Setup -----
K = 10
epsilon = 0.05
delta = 0.1
max_rounds = 10000

true_means = np.random.rand(K)
best_arm = np.argmax(true_means)

counts = np.zeros(K)
sums = np.zeros(K)
means = np.zeros(K)

# Pull each arm once
for arm in range(K):
    r = np.random.binomial(1, true_means[arm])
    counts[arm] += 1
    sums[arm] += r
    means[arm] = sums[arm] / counts[arm]

t = K  # total pulls
history = []

# Main loop
while True:
    t += 1
    ucbs = np.zeros(K)
    lcbs = np.zeros(K)

    for arm in range(K):
        ucbs[arm] = kl_ucb(means[arm], counts[arm], t, delta)
        lcbs[arm] = kl_lcb(means[arm], counts[arm], t, delta)

    i_t = np.argmax(means)
    gaps = ucbs - lcbs[i_t]
    gaps[i_t] = -np.inf
    j_t = np.argmax(gaps)

    for arm in [i_t, j_t]:
        reward = np.random.binomial(1, true_means[arm])
        counts[arm] += 1
        sums[arm] += reward
        means[arm] = sums[arm] / counts[arm]

    history.append((i_t, j_t, means.copy(), counts.copy()))

    if ucbs[j_t] - lcbs[i_t] < epsilon or np.sum(counts) > max_rounds:
        break

selected_arm = i_t

# ===== Terminal Output =====
print("\n========== KL-LUCB Summary ==========")
print(f"True Means: {np.round(true_means, 3)}")
print(f"True Best Arm: {best_arm} (μ = {true_means[best_arm]:.3f})")
print(f"Arm Selected by KL-LUCB: {selected_arm} (μ = {true_means[selected_arm]:.3f})")
print(f"Correct Selection: {'Yes' if selected_arm == best_arm else 'No'}")
print(f"Total Pulls: {int(np.sum(counts))}")
print(f"Rounds until termination: {len(history)}")
print("======================================\n")

# ===== Plotting =====
plt.figure(figsize=(10, 4))
plt.bar(range(K), true_means, alpha=0.6, label="True Means")
plt.bar(range(K), means, alpha=0.4, label="Estimated Means")
plt.axvline(selected_arm, color='green', linestyle='--', label="Selected Arm")
plt.axvline(best_arm, color='red', linestyle='--', label="True Best Arm")
plt.xlabel("Arm")
plt.ylabel("Mean Reward")
plt.title("KL-LUCB: True vs Estimated Means")
plt.legend()
plt.grid(True)
plt.tight_layout()
plt.show()
