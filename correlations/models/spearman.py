from collections import deque
from sortedcontainers import SortedList


class RollingSpearmanCorrelation:
    """
    Calculates Spearman correlation coefficient over a rolling window of data points.
    """

    def __init__(self, window_size):
        self.window_size = window_size
        self.x_values = deque(maxlen=window_size)
        self.y_values = deque(maxlen=window_size)

        self.sorted_x = SortedList()
        self.sorted_y = SortedList()

        self.x_ranks = deque(maxlen=window_size)
        self.y_ranks = deque(maxlen=window_size)

        self.sum_d_squared = 0

    def _get_rank(self, sorted_list, value):
        """
        Get the rank of a value in a sorted list, handling ties.
        """
        left = sorted_list.bisect_left(value)
        right = sorted_list.bisect_right(value)
        return (left + right) / 2 + 1

    def _update_ranks(self):
        """
        Update ranks for all values after adding/removing points.
        """
        new_x_ranks = deque(maxlen=self.window_size)
        new_y_ranks = deque(maxlen=self.window_size)

        for i in range(len(self.x_values)):
            new_x_ranks.append(self._get_rank(self.sorted_x, self.x_values[i]))
            new_y_ranks.append(self._get_rank(self.sorted_y, self.y_values[i]))

        self.x_ranks = new_x_ranks
        self.y_ranks = new_y_ranks

        self.sum_d_squared = 0
        for i in range(len(self.x_ranks)):
            diff = self.x_ranks[i] - self.y_ranks[i]
            self.sum_d_squared += diff * diff

    def add_data_point(self, x, y):
        if len(self.x_values) == self.window_size:
            old_x = self.x_values[0]
            old_y = self.y_values[0]
            self.sorted_x.remove(old_x)
            self.sorted_y.remove(old_y)

        self.x_values.append(x)
        self.y_values.append(y)
        self.sorted_x.add(x)
        self.sorted_y.add(y)

        self._update_ranks()

    def get_correlation(self):
        n = len(self.x_values)

        if n < 2:
            return 0

        return 1 - (6 * self.sum_d_squared) / (n * (n * n - 1))
