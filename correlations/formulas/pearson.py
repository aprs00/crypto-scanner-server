import numpy as np
from collections import deque
from typing import Optional


class IncrementalPearsonCorrelation:
    """
    Calculates Pearson correlation coefficient over a incremental window of data points.
    """

    def __init__(self, window_size, x_initial=None, y_initial=None):
        self.window_size = window_size

        self.sum_x = 0
        self.sum_y = 0
        self.sum_xx = 0
        self.sum_yy = 0
        self.sum_xy = 0
        self.count = 0

        if x_initial is not None and y_initial is not None:
            x_initial = x_initial[-window_size:]
            y_initial = y_initial[-window_size:]

            min_length = min(len(x_initial), len(y_initial))
            x_initial = x_initial[:min_length]
            y_initial = y_initial[:min_length]

            self.sum_x = sum(x_initial)
            self.sum_y = sum(y_initial)
            self.sum_xx = sum(x * x for x in x_initial)
            self.sum_yy = sum(y * y for y in y_initial)
            self.sum_xy = sum(x * y for x, y in zip(x_initial, y_initial))
            self.count = min_length

    def add_data_point(
        self,
        x_new: float,
        y_new: float,
        x_old: Optional[float] = None,
        y_old: Optional[float] = None,
    ):
        if self.count == self.window_size:
            if x_old is not None and y_old is not None:
                self.sum_x -= x_old
                self.sum_y -= y_old
                self.sum_xx -= x_old * x_old
                self.sum_yy -= y_old * y_old
                self.sum_xy -= x_old * y_old
                self.count -= 1

        self.sum_x += x_new
        self.sum_y += y_new
        self.sum_xx += x_new * x_new
        self.sum_yy += y_new * y_new
        self.sum_xy += x_new * y_new
        self.count = min(self.count + 1, self.window_size)

    def get_correlation(self):
        var_x = self.count * self.sum_xx - self.sum_x * self.sum_x
        var_y = self.count * self.sum_yy - self.sum_y * self.sum_y

        if var_x <= 0 or var_y <= 0:
            return 0

        numerator = (self.count * self.sum_xy) - (self.sum_x * self.sum_y)
        denominator = np.sqrt(var_x * var_y)

        return numerator / denominator
