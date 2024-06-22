import numpy as np


def calculate_current_z_score(data):
    mean = np.mean(data)
    std_dev = np.std(data)
    z_score = (data[-1] - mean) / std_dev

    if np.isnan(z_score):
        return 0

    return z_score


def calculate_z_scores(values):
    mean_value = np.mean(values)
    std_dev_value = np.std(values)
    return [(item - mean_value) / std_dev_value for item in values]
