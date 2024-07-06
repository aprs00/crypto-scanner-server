import numpy as np


def calculate_pearson_correlation(x, y, x_symbol, y_symbol, cache=None):
    """Calculate Pearson's correlation coefficient between two lists x and y."""

    if f"mean_{x_symbol}" not in cache:
        cache[f"mean_{x_symbol}"] = np.mean(x)

    if f"mean_{y_symbol}" not in cache:
        cache[f"mean_{y_symbol}"] = np.mean(y)

    mean_x = cache[f"mean_{x_symbol}"]
    mean_y = cache[f"mean_{y_symbol}"]

    # Calculate sum of (X - Mx)^2 and (Y - My)^2
    if f"sum_sq_diffs_x_{x_symbol}" not in cache:
        cache[f"sum_sq_diffs_x_{x_symbol}"] = np.sum((x - mean_x) ** 2)

    if f"sum_sq_diffs_y_{y_symbol}" not in cache:
        cache[f"sum_sq_diffs_y_{y_symbol}"] = np.sum((y - mean_y) ** 2)

    sum_sq_diffs_x = cache[f"sum_sq_diffs_x_{x_symbol}"]
    sum_sq_diffs_y = cache[f"sum_sq_diffs_y_{y_symbol}"]

    # Calculate sum of (X - Mx)(Y - My)
    sum_diffs = np.dot(x - mean_x, y - mean_y)

    pearson_correlation = sum_diffs / (sum_sq_diffs_x * sum_sq_diffs_y) ** 0.5

    return 0 if np.isnan(pearson_correlation) else pearson_correlation
