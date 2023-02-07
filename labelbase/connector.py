from labelbox import Client as labelboxClient
from labelbox.schema.data_row_metadata import DataRowMetadataKind
from labelbase.metadata import _refresh_metadata_ontology

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
                enum_options = get_unique_values_function(table=table, column_name=f"metadata{divider}{metadata_string_type}{divider}{metadata_field_name}", extra_client=extra_client) if metadata_string_type == "enum" else []
                if verbose:
                    print(f"Creating Labelbox metadata field with name {metadata_field_name} of type {metadata_string_type}")
                lb_mdo.create_schema(name=metadata_field_name, kind=metadata_types[metadata_string_type], options=enum_options)
    if "lb_integration_source" not in lb_metadata_names:
        lb_mdo.create_schema(name="lb_integration_source", kind=metadata_types["string"])
    return row_data_col, global_key_col, external_id_col, project_id_col, dataset_id_col, metadata_index, attachment_index, annotation_index
