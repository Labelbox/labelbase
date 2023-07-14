from labelbox import Client as labelboxClient
from labelbox.schema.data_row_metadata import DataRowMetadataKind
from labelbase.metadata import _refresh_metadata_ontology

def validate_columns(client:labelboxClient, table, get_columns_function, get_unique_values_function, 
                     divider:str="///", verbose:bool=False, extra_client=None, creating_data_rows:bool=True):
    """ Given a table of columns with the right naming formats, does the following:
    
    1. Identifies if there are "row_data", "global_key", "external_id", "dataset_id", "model_id", and "model_run_id" columns
    
    2. Creates a metadata_index, attachment_index, annotation_index, and prediction_index - validates that the column names are in the right format:
    
    - Metadata columns must be `metadata{divider}metadata_type{divider}metadata_field_name`
        - metadata_type must be one of |"enum", "string", "datetime", "number"|
        
    - Attachment columns must be `attachment{divider}attachment_type{divider}column_name` (column_name is not relevant for attachment columns
        - metadata_type must be one of |"IMAGE", "VIDEO", "RAW_TEXT", "HTML", "TEXT_URL"|
        
    - Annotation columns must be `annotation{divider}annotation_type{divider}top_level_class_name`
        - annotation_type must be one of |"bbox", "polygon", "point", "mask", "line", "named-entity", "radio", "checklist", "text"|
        
    - Prediction columns must be `prediction{divider}annotation_type{divider}top_level_class_name`
        - annotation_type must be one of |"bbox", "polygon", "point", "mask", "line", "named-entity", "radio", "checklist", "text"|        
        
    Args:
        client                      :   Required (labelbox.client.Client) - Labelbox Client object    
        table                       :   Required - Input user table    
        get_columns_function        :   Required (function) - Function that can get the column names from the table provided, returns list of strings
        get_unique_values_function  :   Required (function) - Function that grabs all unique values from a column, returns list of strings
        verbose                     :   Optional (bool) - If True, prints information about code execution
        extra_client                :   Optional - If relevant, the input get_columns_function / get_unique_values_function required other client object
        creating_data_rows          :   Optional (bool) - If True, performs logic necessary for creating data rows
    Returns a dictionary with the following keys:
    {
        row_data_col                :   Column representing asset URL, raw text, or path to local asset file
        global_key_col              :   Defaults to row_data_col
        external_id_col             :   Defaults to global_key_col
        project_id_col              :   Either `project_id` or "" - overridden by arg for project_id        
        dataset_id_col              :   Either `dataset_id` or "" - overridden by arg for dataset_id
        model_id_col                :   Either `model_id` or "" - overridden by arg for model_id and any args for model_run_id
        model_run_id_col            :   Either `model_run_id` or "" - overridden by arg for model_run_id        
        metadata_index              :   Dictonary where {key=metadata_field_name : value=metadata_type} or {} if not uploading metadata
        attachment_index            :   Dictonary where {key=column_name : value=attachment_type} or {} if not uploading attachments
        annotation_index            :   Dictonary where {key=column_name : value=top_level_class_name} or {} if not uploading annotations
        prediction_index            :   Dictonary where {key=column_name : value=top_level_class_name} or {}  if not uploading predictions
    }
    """
    # Default values
    cols = ["row_data", "global_key", "external_id", "dataset_id", "project_id", "model_id", "model_run_id"]
    x = {f"{c}_col" : "" for c in cols} # Default for cols is "" 
    indexes = ["metadata_index", "attachment_index", "annotation_index", "prediction_index"]    
    x.update({i : {} for i in indexes}) # Default for indexes is {}
    # Accepted values
    accepted_metadata_types = ["enum", "string", "datetime", "number"]
    accepted_attachment_types = ["IMAGE", "VIDEO", "RAW_TEXT", "HTML", "TEXT_URL"]
    accepted_annotation_types = ["bbox", "polygon", "point", "mask", "line", "named-entity", "radio", "checklist", "text", "geo_bbox", "geo_polygon", "geo_point", "geo_line"]
    # Get table column names
    column_names = get_columns_function(table=table, extra_client=extra_client)
    # column names should be input_type///
    for column_name in column_names:
        res = column_name.split(divider)
        if (type(res) == list) and (len(res) == 3):
            input_type = column_name.split(divider)[0]
            # Metadata columns --> metadata///metadata_type///metadata_field_name
            if input_type.lower() == "metadata":
                metadata_type, metadata_field_name = column_name.split(divider)[1:]
                if metadata_type.lower() not in accepted_metadata_types:
                    raise ValueError(f"Invalid value in metadata column name {column_name} - must be `metadata{divider}` followed by one of the following: |{accepted_metadata_types}| followed by `{divider}metadata_field_name`")
                x["metadata_index"][metadata_field_name] = metadata_type.lower()
            # Attachment columns --> attachment///attachment_type///attachment_name          
            elif input_type.lower() == "attachment":
                attachment_type, attachment_name = column_name.split(divider)[1:]
                if attachment_type.upper() not in accepted_attachment_types:
                    raise ValueError(f"Invalid value in attachment column name {column_name} - must be `attachment{divider}` followed by one of the following: |{accepted_attachment_types}| followed by `{divider}column_name`")
                x["attachment_index"][column_name] = attachment_type.upper()
            # Annotation columns --> annotation///annotation_type///top_level_class_name   
            elif input_type.lower() == "annotation":
                annotation_type, top_level_class_name = column_name.split(divider)[1:]
                if annotation_type.lower() not in accepted_annotation_types:
                    raise ValueError(f"Invalid value in annotation column name {column_name} - must be `annotation{divider}` followed by one of the following: |{accepted_annotation_types}| followed by `{divider}top_level_feature_name`")
                x["annotation_index"][column_name] = top_level_class_name                  
            # Prediction columns --> prediction///annotation_type///top_level_class_name
            elif input_type.lower() == "prediction":
                annotation_type, top_level_class_name = column_name.split(divider)[1:]
                if annotation_type.lower() not in accepted_annotation_types:
                    raise ValueError(f"Invalid value in prediction column name {column_name} - must be `prediction{divider}` followed by one of the following: |{accepted_annotation_types}| followed by `{divider}top_level_feature_name`")                    
                x["prediction_index"][column_name] = top_level_class_name   
            else:
                continue
        else:
            # Confirm id column
            if column_name in cols:
                x[f"{column_name}_col"] = column_name                    
            else:
                continue
    # If we're attempting to create data rows but don't have a row_data_col, we cannot upload                
    if creating_data_rows and not x["row_data_col"]: 
        raise ValueError(f"No `row_data` column found - please provide a column of row data URls with the colunn name `row_data`")
    # If we're attempting to do anything on Labelbox without a row_data_col or a global_key_col, we cannot fetch data rows
    if not x["row_data_col"] and not x["global_key_col"]:
        raise ValueError(f"Must provide either a 'global_key' column or a 'data_row_id' column")
    # global_key defaults to row_data        
    x["global_key_col"] = x["global_key_col"] if x["global_key_col"] else x["row_data_col"]
    # external_id defaults to global_key     
    x["external_id_col"] = x["external_id_col"] if x["external_id_col"] else x["global_key_col"]
    # Here, we sync the desired metadata to upload with the existing metadata index
    lb_mdo, lb_metadata_names = _refresh_metadata_ontology(client)
    metadata_types = {
        "enum" : DataRowMetadataKind.enum, "string" : DataRowMetadataKind.string, 
        "datetime" : DataRowMetadataKind.datetime, "number" : DataRowMetadataKind.number
    }
    # If a metadata field name was passed in that doesn't exist, create it in Labelbox
    if x["metadata_index"]:
        for metadata_field_name in x["metadata_index"].keys():
            metadata_string_type = x["metadata_index"][metadata_field_name]
            if metadata_field_name not in lb_metadata_names:
                enum_options = get_unique_values_function(table=table, col=f"metadata{divider}{metadata_string_type}{divider}{metadata_field_name}", extra_client=extra_client) if metadata_string_type == "enum" else []
                if verbose:
                    print(f"Creating Labelbox metadata field with name {metadata_field_name} of type {metadata_string_type}")
                lb_mdo.create_schema(name=metadata_field_name, kind=metadata_types[metadata_string_type], options=enum_options)
    if "lb_integration_source" not in lb_metadata_names:
        lb_mdo.create_schema(name="lb_integration_source", kind=metadata_types["string"])
    return x

def determine_actions(
    row_data_col:str, dataset_id:str, dataset_id_col:str, project_id:str, project_id_col:str, 
    model_id:str, model_id_col:str, model_run_id:str, model_run_id_col:str,
    upload_method:str, metadata_index:dict, attachment_index:dict, annotation_index:dict, prediction_index:dict):
    """ Determines if this upload action can batch data rows to projects - does so by checking if a project ID string or column has been provided
    Args:
        row_data_col                :   Required (str) Column representing asset URL, raw text, or path to local asset file
        dataset_id                  :   Required (str) - Labelbox Dataset ID
        dataset_id_col              :   Required (str) - Column name pertaining to dataset_id
        project_id                  :   Required (str) - Labelbox Project ID
        project_id_col              :   Required (str) - Column name pertaining to project_id
        model_id                    :   Required (str) - Labelbox Model ID ID (supercedes model_id_col)
        model_id_col                :   Required (str) - Column name pertaining to model_id
        model_run_id                :   Required (str) - Labelbox Model Run ID (supercedes model_run_id_col)
        model_run_id_col            :   Required (str) - Column name pertaining to model_run_id (supercedes model_id)
        upload_method               :   Required (bool) - Either "mal", "import", "ground-truth" or ""
        metadata_index              :   Required (dict) - Dictonary where {key=metadata_field_name : value=metadata_type} or {} if not uploading metadata
        attachment_index            :   Required (dict) - Dictonary where {key=column_name : value=attachment_type} or {} if not uploading attachments
        annotation_index            :   Required (dict) - Dictonary where {key=column_name : value=top_level_class_name} or {} if not uploading annotations
        prediction_index            :   Required (dict) - Dictonary where {key=column_name : value=top_level_class_name} or {}  if not uploading predictions
    Returns:
        create_action               :   True dataset_id or dataset_id_col exists
        batch_action                :   True if project_id or project_id_col exists
        annotate_action             :   If upload_method is "mal", "import", or "ground-truth", annotations are in the table, batch_action = True
                                        - If no model / model ID informtion is provided, will default "ground-truth" to "import"
        predictions_action          :   Dictionary that determines how to select a model run, if uploading predictions to a model run, else = False
    """
    # Determine if we're creating data rows
    create_action = False if (dataset_id == dataset_id_col == "") or (row_data_col == "") else True
    # Metadata and Attachments actions are only performed if **not** creating data rows (done with the data row upload otherwise)
    metadata_action = True if metadata_index and not create_action else False
    attachments_action = True if attachment_index and not create_action else False    
    # Determine if we're batching data rows
    batch_action = False if (project_id == project_id_col == "") else True
    # Determine the upload_method if we're batching to projects
    annotate_action = upload_method if (upload_method in ["mal", "import", "ground-truth"]) and annotation_index and batch_action else ""      
    # "ground-truth" defaults to "import" if no model informtion is given
    if (model_id_col==model_id==model_run_id_col==model_run_id=="") and (annotate_action=="ground-truth"):
        print(f"Warning - attempted ground-truth upload attempted, but no model run / model information was provided - uploading data as submitted labels")
        annotate_action = "import" 
    # Determine what kind of predictions action we're taking, if any
    predictions_action = False if not prediction_index or (model_id_col==model_id==model_run_id_col==model_run_id=="") else True
    return {
      "create" : create_action, "batch" : batch_action, "metadata" : metadata_action, 
      "attachments" : attachments_action, "annotate" : annotate_action, "predictions" : predictions_action
    }
