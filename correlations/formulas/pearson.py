import numpy as np
from typing import Optional


class IncrementalPearsonCorrelation:
    """
    Calculates Pearson correlation coefficient over an incremental window of data points.
    Optimized with NumPy for better performance.
    """

    def __init__(self, window_size: int, x_initial=None, y_initial=None):
        self.window_size = window_size

        self.sum_x = np.float64(0)
        self.sum_y = np.float64(0)
        self.sum_xx = np.float64(0)
        self.sum_yy = np.float64(0)
        self.sum_xy = np.float64(0)
        self.count = 0

        if x_initial is not None and y_initial is not None:
            x_initial = np.asarray(x_initial, dtype=np.float64)
            y_initial = np.asarray(y_initial, dtype=np.float64)

            min_length = min(len(x_initial), len(y_initial))
            if min_length > 0:
                x_data = x_initial[:min_length]
                y_data = y_initial[:min_length]

                self.sum_x = np.sum(x_data)
                self.sum_y = np.sum(y_data)
                self.sum_xx = np.sum(x_data * x_data)
                self.sum_yy = np.sum(y_data * y_data)
                self.sum_xy = np.sum(x_data * y_data)
                self.count = min_length

    def add_data_point(
        self,
        x_new: float,
        y_new: float,
        x_old: Optional[float] = None,
        y_old: Optional[float] = None,
    ):
        x_new = np.float64(x_new)
        y_new = np.float64(y_new)

        if self.count == self.window_size and x_old is not None and y_old is not None:
            x_old = np.float64(x_old)
            y_old = np.float64(y_old)

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

    def get_correlation(self) -> float:
        if self.count <= 1:
            return 0.0

        n = np.float64(self.count)

        var_x = n * self.sum_xx - self.sum_x * self.sum_x
        var_y = n * self.sum_yy - self.sum_y * self.sum_y

        if var_x <= np.finfo(np.float64).eps or var_y <= np.finfo(np.float64).eps:
            return 0.0

        numerator = (n * self.sum_xy) - (self.sum_x * self.sum_y)
        denominator = np.sqrt(var_x * var_y)

        return float(numerator / denominator)
