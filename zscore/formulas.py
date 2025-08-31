import numpy as np


def calculate_z_scores(values):
    mean_value = np.mean(values)
    std_dev_value = np.std(values)
    return [(item - mean_value) / std_dev_value for item in values]
