def convert_timeframe_to_seconds(timeframe):
    """
    Convert a timeframe string (e.g., '5m', '1h') to seconds.

    Args:
        timeframe (str): A string representing a time duration (e.g., '5m', '15m', '1h', '4h')

    Returns:
        int: Duration in seconds

    Raises:
        ValueError: If the timeframe format is unsupported
    """
    if timeframe.endswith("m"):
        return int(timeframe[:-1]) * 60  # Convert minutes to seconds
    elif timeframe.endswith("h"):
        return int(timeframe[:-1]) * 3600  # Convert hours to seconds
    else:
        raise ValueError(f"Unsupported timeframe format: {timeframe}")
