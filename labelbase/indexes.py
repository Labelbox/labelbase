from labelbox import Client as labelboxClient
from labelbox.schema.data_row_metadata import DataRowMetadataKind
from labelbase.ontology import _refresh_metadata_ontology

def validate_indexes(client:labelboxClient, table, get_columns_function, get_unique_values_function, divider:str="///" verbose:bool=False, extra_client=None):
    """ Given a table of columns with the right naming formats, creates a metadata_index, attachment_index, and annotation_index
    Args:
        client                      :   Required (labelbox.client.Client) - Labelbox Client object    
        table                       :   Required - Input user table    
        get_columns_function        :   Required (function) - Function that can get the column names from the table provided, returns list of strings
        get_unique_values_function  :   Required (function) - Function that grabs all unique values from a column, returns list of strings
        verbose                     :   Optional (bool) - If True, prints information about code execution
        extra_client                :   Optional - If relevant, the input get_columns_function / get_unique_values_function required other client object
    Returns:
        metadata_index              :   Dictonary where {key=column_name : value=metadata_type}
        attachment_index            :   Dictonary where {key=column_name : value=attachment_type}
        annotation_index            :   Dictonary where {key=column_name : value=annotation_type}
    """
    metadata_index = {}
    attachment_index = {}
    annotation_index = {}
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
                attachment_index[header] = column_type.upper()
            elif input_type.lower() == "annotation":
                if column_type.lower() not in accepted_annotation_types:
                    raise ValueError(f"Invalid value in annotation column name {column_name} - must be `annotation{divider}` followed by one of the following: |{accepted_annotation_types}| followed by `{divider}top_level_feature_name`")
                annotation_index[header] = column_type.lower()
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
                enum_options = get_unique_values_function(table=table, column_name=metadata_field_name, extra_client=extra_client) if metadata_string_type == "enum" else []
                if verbose:
                    print(f"Creating Labelbox metadata field with name {metadata_field_name} of type {metadata_string_type}")
                lb_mdo.create_schema(name=metadata_field_name, kind=conversion[metadata_type], options=enum_options)
    if "lb_integration_source" not in lb_metadata_names:
        lb_mdo.create_schema(name="lb_integration_source", kind=metadata_types["string"])
    return metadata_index, attachment_index, annotation_index
