def format_options(options, type="dict", label_to_upper=False):
    if type == "dict":
        return [
            {"value": k, "label": k.capitalize() if label_to_upper else k}
            for k, _ in options.items()
        ]
    elif type == "list":
        return [
            {"value": i, "label": i.capitalize() if label_to_upper else i}
            for i in options
        ]
