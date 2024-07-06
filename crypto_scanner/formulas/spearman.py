import numpy as np


def rank_data(data):
    """Assign ranks to data values, handling ties by giving them sequential ranks."""
    data = np.array(data)
    temp = data.argsort()
    ranks = np.empty_like(temp)
    ranks[temp] = np.arange(len(data))
    return ranks + 1


def calculate_spearman_correlation(x, y, rank_cache=None):
    if tuple(x) not in rank_cache:
        ranked_x = rank_data(x)
        rank_cache[tuple(x)] = ranked_x
    else:
        ranked_x = rank_cache[tuple(x)]

    if tuple(y) not in rank_cache:
        ranked_y = rank_data(y)
        rank_cache[tuple(y)] = ranked_y
    else:
        ranked_y = rank_cache[tuple(y)]

    std_dev_rank_x = np.std(ranked_x)
    std_dev_rank_y = np.std(ranked_y)

    if std_dev_rank_x == 0 or std_dev_rank_y == 0:
        return 0

    covariance = np.cov(ranked_x, ranked_y)[0, 1]

    rho = covariance / (std_dev_rank_x * std_dev_rank_y)

    return 0 if np.isnan(rho) else rho
