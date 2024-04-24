import numpy as np


def calculate_pearson_correlation(x, y, x_symbol, y_symbol, cache=None):
    """Calculate Pearson's correlation coefficient between two lists x and y."""

    # Calculate means of x and y
    if f"mean_{x_symbol}" not in cache:
        cache[f"mean_{x_symbol}"] = np.mean(x)

    if f"mean_{y_symbol}" not in cache:
        cache[f"mean_{y_symbol}"] = np.mean(y)

    mean_x = cache[f"mean_{x_symbol}"]
    mean_y = cache[f"mean_{y_symbol}"]

    # sum_diffs = sum((x[i] - mean_x) * (y[i] - mean_y) for i in range(n))
    # sum_diffs = sum(
    #     (x[i] - cache[f"mean_{x_symbol}"]) * (y[i] - cache[f"mean_{y_symbol}"])
    #     for i in range(n)
    # )
    sum_diffs = np.dot(x - mean_x, y - mean_y)

    # Calculate sum of (X - Mx)^2 and (Y - My)^2
    if f"sum_sq_diffs_x_{x_symbol}" not in cache:
        # cache[f"sum_sq_diffs_x_{x_symbol}"] = sum(
        #     (x[i] - cache[f"mean_{x_symbol}"]) ** 2 for i in range(n)
        # )
        cache[f"sum_sq_diffs_x_{x_symbol}"] = np.sum((x - mean_x) ** 2)

    if f"sum_sq_diffs_y_{y_symbol}" not in cache:
        # cache[f"sum_sq_diffs_y_{y_symbol}"] = sum(
        #     (y[i] - cache[f"mean_{y_symbol}"]) ** 2 for i in range(n)
        # )
        cache[f"sum_sq_diffs_y_{y_symbol}"] = np.sum((y - mean_y) ** 2)

    # Calculate Pearson correlation coefficient
    sum_sq_diffs_x = cache[f"sum_sq_diffs_x_{x_symbol}"]
    sum_sq_diffs_y = cache[f"sum_sq_diffs_y_{y_symbol}"]

    return sum_diffs / (sum_sq_diffs_x * sum_sq_diffs_y) ** 0.5
