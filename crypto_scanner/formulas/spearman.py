import numpy as np
from scipy.stats import rankdata


def calculate_spearman_correlation(A, B, key_a, key_b, rank_cache=None):
    if rank_cache is None:
        rank_cache = {}

    A = np.asarray(A)
    B = np.asarray(B)

    ranked_a = rank_cache.setdefault(A.tobytes(), rankdata(A, method="average"))
    ranked_b = rank_cache.setdefault(B.tobytes(), rankdata(B, method="average"))

    std_dev_rank_a = rank_cache.setdefault(f"std_dev_{key_a}", np.std(ranked_a))
    std_dev_rank_b = rank_cache.setdefault(f"std_dev_{key_b}", np.std(ranked_b))

    if std_dev_rank_a == 0 or std_dev_rank_b == 0:
        return 0

    rho = np.corrcoef(ranked_a, ranked_b)[0, 1]

    return 0 if np.isnan(rho) else rho
