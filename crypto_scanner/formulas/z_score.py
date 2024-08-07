import numpy as np


def calculate_current_z_score(data):
    data = np.array(data, dtype=np.float64)
    data = data[~np.isnan(data)]

    mean = np.mean(data)
    std_dev = np.std(data)

    z_score = (data[-1] - mean) / std_dev

    return 0 if np.isnan(z_score) else z_score


def calculate_z_scores(values):
    mean_value = np.mean(values)
    std_dev_value = np.std(values)
    return [(item - mean_value) / std_dev_value for item in values]
