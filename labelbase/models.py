def create_model_run_with_name(model, model_run_name="0.0.1"):
    """ Creates a model run with a unique name given the existing names in the model run
    Args:
        model                       :   Required (labelbox.Model) - Labelbox Model
        model_run_name              :   Optional (str) - Desired Model Run Name  
    Returns:
        model_run
    """
    current_runs = [model_run.name for model_run in model.model_runs()]
    while model_run_name in current_runs:
        # Model run name syntax is #.#.##
        v1, v2, v3 = model_run_name.split(".")
        # Increase v3 by one, 99 being the max value
        if int(v3) == 99:
            v2 = int(v2) + 1
            v3str = "00"
        else:
            v2 = int(v2)
            v3 = int(v3) + 1
            v3str = str(v3) if len(str(v3)) >= 2 else f"0{str(v3)}"
        # Increase v2 by one, 9 being the max value
        if v2 > 9:
            v1 = int(v1) + 1
            v2 = 0
        # Construct model run name
        model_run_name = f"{str(v1)}.{str(v2)}.{v3str}"
    # Create model run
    model_run = model.create_model_run(name=model_run_name)
    return model_run
