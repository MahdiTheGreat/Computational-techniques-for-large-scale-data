from assignment5_problem1_skeleton import murmur3_32
import matplotlib.pyplot as plt
import numpy as np

if __name__ == '__main__':

    seed = 0xee418b6c16 & 0xFFFFFFFF  # ensure 32-bit if needed
    m = 128
    hash_counts = [0] * m

    with open("/data/courses/2025_dat470_dit066/words", "rb") as f:
        words = [line.strip() for line in f]

    n = len(words)

    # Hash and count 7 LSBs
    for word in words:
        h = murmur3_32(word, seed=seed)
        bucket = h & (m - 1)  # Extract 7 LSBs
        hash_counts[bucket] += 1

    # Plot histogram
    plt.bar(range(m), hash_counts)
    plt.xlabel("Bucket (7 LSBs of hash)")
    plt.ylabel("Frequency")
    plt.title("Hash Value Distribution (7-bit)")
    plt.show()
    plt.savefig("hash_distribution.png")

    # Statistics
    mean = np.mean(hash_counts)
    std_dev = np.std(hash_counts)

    # Collision probability
    collisions = sum(f * (f - 1) // 2 for f in hash_counts)
    total_pairs = n * (n - 1) // 2
    collision_prob = collisions / total_pairs

    print(f"Total words: {n}")
    print(f"Mean: {mean:.4f}")
    print(f"Standard Deviation: {std_dev:.4f}")
    print(f"Collisions: {collisions}")
    print(f"Estimated Collision Probability: {collision_prob:.6e}")
