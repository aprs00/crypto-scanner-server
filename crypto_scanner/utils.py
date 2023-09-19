def format_options(options, type="dict"):
    if type == "dict":
        return [{"value": k, "label": k} for k, _ in options.items()]
    elif type == "list":
        return [{"value": i, "label": i} for i in options]
