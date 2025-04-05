import numpy as np
from collections import deque


class IncrementalPearsonCorrelation:
    """
    Calculates Pearson correlation coefficient over a incremental window of data points.
    """

    def __init__(self, window_size, x_initial=None, y_initial=None):
        self.window_size = window_size
        self.x_values = deque(maxlen=window_size)
        self.y_values = deque(maxlen=window_size)

        self.sum_x = 0
        self.sum_y = 0
        self.sum_xx = 0
        self.sum_yy = 0
        self.sum_xy = 0
        self.count = 0

        # Initialize with arrays if provided
        if x_initial is not None and y_initial is not None:
            # Make sure we only take up to window_size elements
            x_initial = x_initial[-window_size:]
            y_initial = y_initial[-window_size:]

            # Make sure the arrays have the same length
            min_length = min(len(x_initial), len(y_initial))
            x_initial = x_initial[:min_length]
            y_initial = y_initial[:min_length]

            # Add values to deques
            self.x_values.extend(x_initial)
            self.y_values.extend(y_initial)

            # Calculate sums
            self.sum_x = sum(x_initial)
            self.sum_y = sum(y_initial)
            self.sum_xx = sum(x * x for x in x_initial)
            self.sum_yy = sum(y * y for y in y_initial)
            self.sum_xy = sum(x * y for x, y in zip(x_initial, y_initial))
            self.count = min_length

    def add_data_point(self, x, y):
        if self.count == self.window_size:
            old_x = self.x_values[0]
            old_y = self.y_values[0]

            self.sum_x -= old_x
            self.sum_y -= old_y
            self.sum_xx -= old_x * old_x
            self.sum_yy -= old_y * old_y
            self.sum_xy -= old_x * old_y
            self.count -= 1

        self.x_values.append(x)
        self.y_values.append(y)

        self.sum_x += x
        self.sum_y += y
        self.sum_xx += x * x
        self.sum_yy += y * y
        self.sum_xy += x * y
        self.count = min(self.count + 1, self.window_size)

    def get_correlation(self):
        var_x = self.count * self.sum_xx - self.sum_x * self.sum_x
        var_y = self.count * self.sum_yy - self.sum_y * self.sum_y

        if var_x <= 0 or var_y <= 0:
            return 0

        numerator = (self.count * self.sum_xy) - (self.sum_x * self.sum_y)
        denominator = np.sqrt(var_x * var_y)

        return numerator / denominator
