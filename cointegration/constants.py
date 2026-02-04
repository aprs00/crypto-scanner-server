COINTEGRATION_LIVE_TABLE_CONFIG = {
    "window_options": {
        "1d": 1440,
    },
    "limit_options": [50, 100, 250],
    "sort_key_to_column_id": {
        "abs_z": "spread_z",
        "adf_t": "adf_t",
        "half_life": "half_life",
        "updated": "updated_at",
    },
    "column_id_to_sort_key": {
        "spread_z": "abs_z",
        "adf_t": "adf_t",
        "half_life": "half_life",
        "updated_at": "updated",
    },
    "right_aligned_columns": [
        "spread_z",
        "half_life",
        "adf_t",
        "hedge_ratio",
        "spread_std",
        "updated_at",
    ],
    "sortable_column_ids": ["spread_z", "half_life", "adf_t", "updated_at"],
    "default_sort_desc": {
        "adf_t": False,
        "half_life": False,
        "spread_z": True,
        "updated_at": True,
    },
}
