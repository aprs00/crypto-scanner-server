import numpy as np
import time
import json

from crypto_scanner.formulas.pearson import (
    calculate_pearson_correlation,
)
from crypto_scanner.formulas.spearman import (
    calculate_spearman_correlation,
)

# A = [7, 8, 1, 28, 9, 1, -2, 8, 2]
# B = [9, 0, 100, 29, 18, 8, 12, 88, 9]

A = np.random.rand(10000000)
B = np.random.rand(10000000)

cache = {}
cache2 = {}

print(calculate_pearson_correlation(A, B, "BTC", "ETH", cache))
print(np.corrcoef(A, B)[0][1])


def calculate_correlations(data, symbols, type):
    correlations = {}
    rank_cache = {}
    pearson_cache = {}

    for symbol1 in symbols:
        for symbol2 in symbols:
            if type == "pearson":

                correlations[f"{symbol1} - {symbol2}"] = calculate_pearson_correlation(
                    data[symbol1], data[symbol2], symbol1, symbol2, pearson_cache
                )
            elif type == "spearman":
                correlations[f"{symbol1} - {symbol2}"] = calculate_spearman_correlation(
                    data[symbol1], data[symbol2], rank_cache
                )

    return correlations


def run_performance_test(num_iterations, num_symbols, data_size, corr_type):
    """
    Run performance test for correlation calculations
    """
    # Generate random symbols
    symbols = [f"SYMBOL_{i}" for i in range(num_symbols)]

    # Generate random data dictionary
    data = {}
    for symbol in symbols:
        data[symbol] = np.random.rand(data_size)

    # Warm-up run (to initialize any caches)
    calculate_correlations(data, symbols, corr_type)

    # Actual test run
    start_time = time.time()

    for i in range(num_iterations):
        # Generate new random data for each iteration
        for symbol in symbols:
            data[symbol] = np.random.rand(data_size)

        calculate_correlations(data, symbols, corr_type)

    end_time = time.time()
    execution_time = end_time - start_time

    # Calculate statistics
    total_calculations = num_iterations * num_symbols * num_symbols
    avg_time_per_iteration = execution_time / num_iterations
    avg_time_per_calculation = execution_time / total_calculations

    return {
        "correlation_type": corr_type,
        "total_execution_time": f"{execution_time:.4f} seconds",
        "avg_time_per_iteration": f"{avg_time_per_iteration:.4f} seconds",
        "avg_time_per_calculation": f"{avg_time_per_calculation:.6f} seconds",
    }


if __name__ == "__main__":
    # Test configuration
    configs = [
        {"iterations": 1, "symbols": 120, "data_size": 180000, "type": "pearson"},
        # {"iterations": 1000, "symbols": 5, "data_size": 10000, "type": "spearman"},
    ]

    results = []

    print("Starting performance tests...")

    for config in configs:
        print(
            f"Running test: {config['type']} correlation with {config['symbols']} symbols, "
            f"{config['data_size']} data points, {config['iterations']} iterations..."
        )

        result = run_performance_test(
            config["iterations"], config["symbols"], config["data_size"], config["type"]
        )

        results.append(result)
        print(f"Test completed: {result['total_execution_time']}")
        print(f"Average time per calculation: {result['avg_time_per_calculation']}")
        print("-" * 50)

    print("\nPerformance Test Summary:")
    print(json.dumps(results, indent=2))
