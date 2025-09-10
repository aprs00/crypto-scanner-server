import numpy as np
from typing import Optional, Dict, Tuple, Sequence
import math


class SumCache:
    """
    Cache for sum_x and sum_xx values to avoid recalculation across different correlation pairs.
    Indexed by (symbol, data_type, hours).
    """

    def __init__(self):
        self._cache: Dict[Tuple[str, str, int], Dict[str, float]] = {}

    def get(
        self, symbol: str, data_type: str, hours: int
    ) -> Optional[Dict[str, float]]:
        """Get cached sum_x and sum_xx values for a symbol."""
        key = (symbol, data_type, hours)
        return self._cache.get(key)

    def set(
        self,
        symbol: str,
        data_type: str,
        hours: int,
        sum_x: np.float64,
        sum_xx: np.float64,
        count: int,
    ):
        """Cache sum_x and sum_xx values for a symbol."""
        key = (symbol, data_type, hours)
        self._cache[key] = {
            "sum_x": float(sum_x),
            "sum_xx": float(sum_xx),
            "count": count,
        }

    def clear(self):
        """Clear all cached values."""
        self._cache.clear()


class IncrementalPearsonCorrelation:
    """
    Calculates Pearson correlation coefficient over an incremental window of data points.
    Optimized with NumPy for better performance.

    Now supports caching of sum_x and sum_xx values to avoid recalculation across correlation pairs.
    """

    def __init__(
        self,
        window_size: int,
        x_initial: Sequence[float],
        y_initial: Sequence[float],
        sum_cache: SumCache,
        x_symbol: str,
        y_symbol: str,
        data_type: str,
        hours: int,
    ):
        self.window_size = window_size
        self.sum_cache = sum_cache
        self.x_symbol = x_symbol
        self.y_symbol = y_symbol
        self.data_type = data_type
        self.hours = hours

        self.sum_x = np.float64(0)
        self.sum_y = np.float64(0)
        self.sum_xx = np.float64(0)
        self.sum_yy = np.float64(0)
        self.sum_xy = np.float64(0)
        self.count = 0

        if x_initial is not None and y_initial is not None:
            x_array = np.asarray(x_initial, dtype=np.float64)
            y_array = np.asarray(y_initial, dtype=np.float64)

            min_length = min(len(x_array), len(y_array))
            x_data = x_array[:min_length]
            y_data = y_array[:min_length]

            self.sum_x, self.sum_xx = self._get_or_compute_sums(
                x_data, self.x_symbol, min_length
            )
            self.sum_y, self.sum_yy = self._get_or_compute_sums(
                y_data, self.y_symbol, min_length
            )

            self.sum_xy = np.sum(x_data * y_data)
            self.count = min_length

    def _get_or_compute_sums(
        self, data: np.ndarray, symbol: str, data_length: int
    ) -> Tuple[np.float64, np.float64]:
        """Get sums from cache or compute them, updating cache if needed."""
        if self.sum_cache and symbol and self.data_type and self.hours:
            cache_entry = self.sum_cache.get(symbol, self.data_type, self.hours)
            if cache_entry and cache_entry["count"] == data_length:
                return np.float64(cache_entry["sum_x"]), np.float64(
                    cache_entry["sum_xx"]
                )

        sum_val = np.sum(data)
        sum_squared = np.sum(data * data)

        if self.sum_cache and symbol and self.data_type and self.hours:
            self.sum_cache.set(
                symbol, self.data_type, self.hours, sum_val, sum_squared, data_length
            )

        return sum_val, sum_squared

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
        var_x = self.count * self.sum_xx - self.sum_x * self.sum_x
        var_y = self.count * self.sum_yy - self.sum_y * self.sum_y

        if var_x <= 0 or var_y <= 0:
            return 0.0

        numerator = (self.count * self.sum_xy) - (self.sum_x * self.sum_y)
        denominator = math.sqrt(var_x * var_y)

        if denominator == 0.0:
            return 0.0

        return float(numerator / denominator)
