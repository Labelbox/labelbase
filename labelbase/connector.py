from labelbox import Client as labelboxClient
from labelbox.schema.data_row_metadata import DataRowMetadataKind
from labelbase.metadata import _refresh_metadata_ontology

def determine_actions(dataset_id:str, dataset_id_col:str, project_id:str, project_id_col:str, upload_method:str, annotation_index:dict):
    """ Determines if this upload action can batch data rows to projects - does so by checking if a project ID string or column has been provided
    Args:
        dataset_id                  :   Required (str) - Labelbox Dataset ID
        dataset_id_col              :   Required (str) - Column name pertaining to dataset_id        
        project_id                  :   Required (str) - Labelbox Project ID
        project_id_col              :   Required (str) - Column name pertaining to project_id
        upload_method               :   Required (bool) - Either "mal", "import", or ""
        annotation_index            :   Required (dict) - Dictonary where {key=column_name : value=top_level_feature_name}        
    Returns:
        batch_action                :   Required (bool) - True if batching to projects, False if not
        annotate_action             :   Required (bool) - True if uploading annotations to projects, False if not
    """
    if (dataset_id_col=="") and (dataset_id==""):
        raise ValueError(f"To create data rows, please provide either a `dataset_id` column or a Labelbox dataset ID to argument `dataset_id`")    
    if (project_id == "") and (project_id_col == ""):
        batch_action = False
    else:
        batch_action = True
    if upload_method: # If there's an upload method, the user at least wants to upload annotations
        if upload_method in ["mal", "import"]:
            if not batch_action:
                raise ValueError(f"Upload method was provided, but data rows have not been configured to-be-batched to projects")
            elif annotation_index == {}:
                raise ValueError(f"Upload method was provided, but no columns have been identified as columns containing annotation values")
            else: # There's a batch_action and there's an annotation_index
                annotate_action = True
        else:
            raise ValueError(f"Upload method was provided, but must be one of `mal` or `import` - received `{upload_method}`")
    else:
        annotate_action = False
    return batch_action, annotate_action  

def validate_columns(client:labelboxClient, table, get_columns_function, get_unique_values_function, 
                     divider:str="///", verbose:bool=False, extra_client=None):
    """ Given a table of columns with the right naming formats, does the following:
    
    1. Identifies a `row_data_col` name, `global_key_col` name, and an `external_id_col` name
    
    - `row_data_col` must be identified as the column with the name `row_data`
    - `global_key_col` defaults to `row_data_col` if not identified
    - `external_id_col` defaults to `global_key_col` if not identified
    
    2. Creates a metadata_index, attachment_index, and annotation_index - validates that the column names are in the right format:
    
    - Metadata columns must be `metadata{divider}metadata_type{divider}metadata_field_name`
        - metadata_type must be one of |"enum", "string", "datetime", "number"| (not case sensitive)
        
    - Attachment columns must be `attachment{divider}attachment_type{divider}column_name` (column_name is not relevant for attachment columns
        - metadata_type must be one of |"IMAGE", "VIDEO", "RAW_TEXT", "HTML", "TEXT_URL"| (not case sensitive)
        
    - Metadata columns must be `annotation{divider}annotation_type{divider}top_level_name`
        - annotation_type must be one of |"bbox", "polygon", "point", "mask", "line", "named-entity", "radio", "checklist", "text"| (not case sensitive)
        
    Args:
        client                      :   Required (labelbox.client.Client) - Labelbox Client object    
        table                       :   Required - Input user table    
        get_columns_function        :   Required (function) - Function that can get the column names from the table provided, returns list of strings
        get_unique_values_function  :   Required (function) - Function that grabs all unique values from a column, returns list of strings
        verbose                     :   Optional (bool) - If True, prints information about code execution
        extra_client                :   Optional - If relevant, the input get_columns_function / get_unique_values_function required other client object
    Returns:
        row_data_col                :   Raises an error if no `row_data` column is provided
        global_key_col              :   Defaults to row_data_col
        external_id_col             :   Defaults to global_key_col
        project_id_col              :   Returns "" if no `project_id` column is provided
        dataset_id_col              :   Returns "" if no `dataset_id` column is provided
        metadata_index              :   Dictonary where {key=metadata_field_name : value=metadata_type}
        attachment_index            :   Dictonary where {key=column_name : value=attachment_type}
        annotation_index            :   Dictonary where {key=column_name : value=annotation_type}
    """
    metadata_index = {}
    attachment_index = {}
    annotation_index = {}
    row_data_col = ""
    global_key_col = ""
    external_id_col = ""
    project_id_col = ""
    dataset_id_col = ""
    accepted_metadata_types = ["enum", "string", "datetime", "number"]
    accepted_attachment_types = ["IMAGE", "VIDEO", "RAW_TEXT", "HTML", "TEXT_URL"]
    accepted_annotation_types = ["bbox", "polygon", "point", "mask", "line", "named-entity", "radio", "checklist", "text"]
    column_names = get_columns_function(table=table, extra_client=extra_client)
    for column_name in column_names:
        if divider in column_name:
            input_type, column_type, header = column_name.split(divider)
            if input_type.lower() == "metadata":
                if column_type.lower() not in accepted_metadata_types:
                    raise ValueError(f"Invalid value in metadata column name {column_name} - must be `metadata{divider}` followed by one of the following: |{accepted_metadata_types}| followed by `{divider}metadata_field_name`")
                metadata_index[header] = column_type.lower()
            elif input_type.lower() == "attachment":
                if column_type.upper() not in accepted_attachment_types:
                    raise ValueError(f"Invalid value in attachment column name {column_name} - must be `attachment{divider}` followed by one of the following: |{accepted_attachment_types}| followed by `{divider}column_name`")
                attachment_index[column_name] = column_type.upper()
            elif input_type.lower() == "annotation":
                if column_type.lower() not in accepted_annotation_types:
                    raise ValueError(f"Invalid value in annotation column name {column_name} - must be `annotation{divider}` followed by one of the following: |{accepted_annotation_types}| followed by `{divider}top_level_feature_name`")
                annotation_index[column_name] = header
        else:
            if column_name == "row_data":
                row_data_col = "row_data"
            if column_name == "global_key":
                global_key_col = "global_key"
            if column_name == "external_id":
                external_id_col = "external_id"   
            if column_name == "project_id":
                project_id_col = "project_id"   
            if column_name == "dataset_id":
                dataset_id_col = "dataset_id" 
    if not row_data_col:
        raise ValueError(f"No `row_data` column found - please provide a column of row data URls with the colunn name `row_data`")
    global_key_col = global_key_col if global_key_col else row_data_col
    external_id_col = external_id_col if external_id_col else global_key_col
    lb_mdo, lb_metadata_names = _refresh_metadata_ontology(client)
    metadata_types = {
      "enum" : DataRowMetadataKind.enum, 
      "string" : DataRowMetadataKind.string, 
      "datetime" : DataRowMetadataKind.datetime, 
      "number" : DataRowMetadataKind.number
    }
    # If a metadata field name was passed in that doesn't exist, create it in Labelbox
    if metadata_index:
        for metadata_field_name in metadata_index.keys():
            metadata_string_type = metadata_index[metadata_field_name]
            if metadata_field_name not in lb_metadata_names:
                enum_options = get_unique_values_function(table=table, col=f"metadata{divider}{metadata_string_type}{divider}{metadata_field_name}", extra_client=extra_client) if metadata_string_type == "enum" else []
                if verbose:
                    print(f"Creating Labelbox metadata field with name {metadata_field_name} of type {metadata_string_type}")
                lb_mdo.create_schema(name=metadata_field_name, kind=metadata_types[metadata_string_type], options=enum_options)
    if "lb_integration_source" not in lb_metadata_names:
        lb_mdo.create_schema(name="lb_integration_source", kind=metadata_types["string"])
    return row_data_col, global_key_col, external_id_col, project_id_col, dataset_id_col, metadata_index, attachment_index, annotation_index
  
def validate_column_name_change(old_col_name:str, new_col_name:str, existing_col_names:list):
    """ Validates that the rename aligns with LabelPandas column name specifications
    Args:
        old_col_name                :   Required (str) - Original column name
        new_col_name                :   Required (str) - Desired new name
        existing_col_names          :   Required (list) - List of existing column names
    Returns:
        Nothing - 
        - Will raise an error if the old column name isn't in the passed in DataFrame
        - Will also raise an error if the new column name isn't what LabelPandas is expecting
    """ 
    if old_col_name not in existing_col_names:
        raise ValueError(f"Argument `rename_dict` requires a dictionary where:\n            \n        `old_column_name` : `new_column_name`,\n        `old_column_name` : `new_column_name`\n    \nReceived key `{old_col_name}` which is not an existing column name")    
    if new_col_name in ["row_data", "external_id", "global_key", "file_path"]:
        valid_column = True
    elif new_col_name.startswith("metadata"):
        valid_column = True
    elif new_col_name.startswith("attachment"):
        valid_column = True
    elif new_col_name.startswith("annotation"):
        valid_column = True   
    else:
        valid_column = False
    if not valid_column:
        raise ValueError(f"New name assignment invalid for LabelPandas - colmn name must be one of `row_data`, `external_id` or `global_key` or start with `metadata`, `attachment` or `annotation` -- received new column name `{new_col_name}`")  
