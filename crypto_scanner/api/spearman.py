def calculate_mean(values):
    """Calculate the mean of a list of values."""
    return sum(values) / len(values)


def rank_data(data):
    """Assign ranks to data values, handling ties by giving them sequential ranks."""
    sorted_data = sorted((value, idx) for idx, value in enumerate(data))
    ranks = {}
    same_value_count = 0

    for i in range(len(sorted_data)):
        if i > 0 and sorted_data[i][0] == sorted_data[i - 1][0]:
            same_value_count += 1
        else:
            same_value_count = 0

        rank = i - same_value_count + 1
        ranks[sorted_data[i][1]] = rank

    return [ranks[idx] for idx in range(len(data))]


def calculate_spearman_correlation(x, y, rank_cache=None):
    """Calculate Spearman's correlation coefficient between two lists x and y."""
    n = len(x)

    if rank_cache is None:
        rank_cache = {}

    # Calculate ranks for x and y if not already cached
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

    # Calculate means of ranks
    mean_rank_x = calculate_mean(ranked_x)
    mean_rank_y = calculate_mean(ranked_y)

    # Calculate sum of (XRa - Mx) * (YRa - My)
    sum_diffs = sum(
        (ranked_x[i] - mean_rank_x) * (ranked_y[i] - mean_rank_y) for i in range(n)
    )

    # Calculate standard deviations of ranked X and Y values
    std_dev_rank_x = (
        sum((ranked_x[i] - mean_rank_x) ** 2 for i in range(n)) / n
    ) ** 0.5
    std_dev_rank_y = (
        sum((ranked_y[i] - mean_rank_y) ** 2 for i in range(n)) / n
    ) ** 0.5

    if std_dev_rank_x == 0 or std_dev_rank_y == 0:
        return 0  # If standard deviation is zero, return 0 to avoid division by zero

    # Calculate covariance
    covariance = sum_diffs / n

    # Calculate Spearman's correlation coefficient
    rho = covariance / (std_dev_rank_x * std_dev_rank_y)
    return rho


# def calculate_all_correlations(crypto_prices):
#     """Calculate Spearman's correlation coefficients for all pairs of cryptocurrencies."""
#     symbols = list(crypto_prices.keys())
#     correlations = {}
#     rank_cache = {}
#
#     for i in range(len(symbols)):
#         for j in range(i + 1, len(symbols)):
#             symbol1 = symbols[i]
#             symbol2 = symbols[j]
#             prices1 = crypto_prices[symbol1]
#             prices2 = crypto_prices[symbol2]
#
#             # Calculate Spearman's correlation coefficient between symbol1 and symbol2
#             correlation = calculate_spearman_correlation(prices1, prices2, rank_cache)
#             correlations[(symbol1, symbol2)] = correlation
#
#     return correlations
