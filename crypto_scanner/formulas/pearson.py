import numpy as np


def calculate_pearson_correlation(A, B, a_key, b_key, cache):
    """Calculate Pearson's correlation coefficient between two lists x and y."""

    if len(A) == 0 or len(B) == 0:
        print("LENGTH IS 0")
        print(len(A), a_key)
        print(len(B), b_key)

    mean_a = cache.setdefault(f"mean_{a_key}", np.mean(A))
    mean_b = cache.setdefault(f"mean_{b_key}", np.mean(B))

    centered_a = cache.setdefault(f"centered_{a_key}", A - mean_a)
    centered_b = cache.setdefault(f"centered_{b_key}", B - mean_b)

    sum_sq_diffs_a = cache.setdefault(
        f"sum_sq_diffs{a_key}", np.sum(centered_a * centered_a)
    )
    sum_sq_diffs_b = cache.setdefault(
        f"sum_sq_diffs{b_key}", np.sum(centered_b * centered_b)
    )

    sum_diffs = np.dot(centered_a, centered_b)

    pearson_correlation = sum_diffs / np.sqrt(sum_sq_diffs_a * sum_sq_diffs_b)

    return 0 if np.isnan(pearson_correlation) else pearson_correlation
