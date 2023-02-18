from labelbox import Client as labelboxClient
from labelbox.schema.data_row_metadata import DataRowMetadataKind
from datetime import datetime
from dateutil import parser
import pytz

def get_metadata_schema_to_type(client:labelboxClient, lb_mdo=False, invert:bool=False):
    """ Creates a dictionary where {key=metadata_schema_id: value=metadata_type} 
    - metadata_type either "string" "enum" "datetime" or "number"
    Args:
        client              :   Required (labelbox.client.Client) - Labelbox Client object    
        lb_mdo              :   Optional (labelbox.schema.data_row_metadata.DataRowMetadataOntology) - Labelbox metadata ontology
        divider             :   Optional (str) - String separating parent and enum option metadata values
        invert              :   Optional (bool) - If True, inverts the dictionary to be where {key=metadata_name_key: value=metadata_schema_id}
    Returns:
        Dictionary where {key=metadata_schema_id: value=metadata_type} - or the inverse
    """    
    metadata_schema_to_type = {}
    lb_mdo = client.get_data_row_metadata_ontology() if not lb_mdo else lb_mdo
    for field in lb_mdo._get_ontology():
        metadata_type = ""
        if "enum" in field["kind"].lower():
            metadata_type = "enum"
        if "string" in field["kind"].lower():
            metadata_type = "string"
        if "datetime" in field["kind"].lower():
            metadata_type = "datetime"
        if "number" in field["kind"].lower():
            metadata_type = "number"
        if metadata_type:
            metadata_schema_to_type[field["id"]] = metadata_type
    return_value = metadata_schema_to_type if not invert else {v:k for k,v in metadata_schema_to_type.items()}
    return return_value

def get_metadata_schema_to_name_key(client:labelboxClient, lb_mdo=False, divider="///", invert:bool=False):
    """ Creates a dictionary where {key=metadata_schema_id: value=metadata_name_key} 
    - name_key is name for all metadata fields, and for enum options, it is "parent_name{divider}child_name"
    Args:
        client              :   Required (labelbox.client.Client) - Labelbox Client object    
        lb_mdo              :   Optional (labelbox.schema.data_row_metadata.DataRowMetadataOntology) - Labelbox metadata ontology
        divider             :   Optional (str) - String separating parent and enum option metadata values
        invert              :   Optional (bool) - If True, inverts the dictionary to be where {key=metadata_name_key: value=metadata_schema_id}
    Returns:
        Dictionary where {key=metadata_schema_id: value=metadata_name_key} - or the inverse
    """
    lb_mdo = client.get_data_row_metadata_ontology() if not lb_mdo else lb_mdo
    lb_metadata_dict = lb_mdo.reserved_by_name
    lb_metadata_dict.update(lb_mdo.custom_by_name)
    metadata_schema_to_name_key = {}
    for metadata_field_name_key in lb_metadata_dict:
        if type(lb_metadata_dict[metadata_field_name_key]) == dict:
            metadata_schema_to_name_key[lb_metadata_dict[metadata_field_name_key][next(iter(lb_metadata_dict[metadata_field_name_key]))].parent] = str(metadata_field_name_key)
            for enum_option in lb_metadata_dict[metadata_field_name_key]:
                metadata_schema_to_name_key[lb_metadata_dict[metadata_field_name_key][enum_option].uid] = f"{str(metadata_field_name_key)}{str(divider)}{str(enum_option)}"
        else:
            metadata_schema_to_name_key[lb_metadata_dict[metadata_field_name_key].uid] = str(metadata_field_name_key)
    return_value = metadata_schema_to_name_key if not invert else {v:k for k,v in metadata_schema_to_name_key.items()}
    return return_value  

def _refresh_metadata_ontology(client:labelboxClient):
    """ Refreshes a Labelbox Metadata Ontology
    Args:
        client              :   Required (labelbox.client.Client) - Labelbox Client object    
    Returns:
        lb_mdo              :   labelbox.schema.data_row_metadata.DataRowMetadataOntology
        lb_metadata_names   :   List of metadata field names from a Labelbox metadata ontology
    """
    lb_mdo = client.get_data_row_metadata_ontology()
    lb_metadata_names = [field['name'] for field in lb_mdo._get_ontology()]
    return lb_mdo, lb_metadata_names

def sync_metadata_fields(client:labelboxClient, table, get_columns_function, add_column_function, get_unique_values_function, metadata_index:dict={}, verbose:bool=False, extra_client=None):
    """ Ensures Labelbox's Metadata Ontology and your input have all necessary metadata fields / columns given a metadata_index
    Args:
        client                      :   Required (labelbox.client.Client) - Labelbox Client object    
        table                       :   Required - Input user table
        get_columns_function        :   Required (function) - Function that can get the column names from the table provided, returns list of strings
        add_column_function         :   Required (function) - Function that can add an empty column to the table provided, returns table
        get_unique_values_function  :   Required (function) - Function that grabs all unique values from a column, returns list of strings
        metadata_index              :   Optional (dict) - Dictionary where {key=column_name : value=metadata_type} - metadata_type must be one of "enum", "string", "datetime" or "number"
        verbose                     :   Optional (bool) - If True, prints information about code execution
        extra_client                :   Optional - If relevant, the input functions' required other client object
    Returns:
        Updated table if successful, False if not
    """
    # Get your metadata ontology, grab all the metadata field names
    lb_mdo, lb_metadata_names = _refresh_metadata_ontology(client)
    # Convert your meatdata_index values from strings into labelbox.schema.data_row_metadata.DataRowMetadataKind types
    conversion = {"enum" : DataRowMetadataKind.enum, "string" : DataRowMetadataKind.string, "datetime" : DataRowMetadataKind.datetime, "number" : DataRowMetadataKind.number}
    # Check to make sure the value in your metadata index is one of the accepted values        
    _enforce_metadata_index(metadata_index, verbose)
    # If your table doesn't have columns for all your metadata_field_names, make columns for them
    if type(table) != bool:
        if metadata_index:
            column_names = get_columns_function(table, extra_client=extra_client)
            for metadata_field_name in metadata_index.keys():
                if metadata_field_name not in column_names:
                    table = add_column_function(table, column_name=metadata_field_name, default_value=None, extra_client=extra_client)
    # If Labelbox doesn't have metadata for all your metadata_field_names, make Labelbox metadata fields
    for metadata_field_name in metadata_index.keys():
        metadata_type = metadata_index[metadata_field_name]
        # Check to see if a metadata index input is a metadata field in Labelbox. If not, create the metadata field in Labelbox. 
        if metadata_field_name not in lb_metadata_names:
            enum_options = get_unique_values_function(table, metadata_field_name, extra_client=extra_client) if metadata_type == "enum" else []
            lb_mdo.create_schema(name=metadata_field_name, kind=conversion[metadata_type], options=enum_options)
            lb_mdo, lb_metadata_names = _refresh_metadata_ontology(client)
    if 'lb_integration_source' not in lb_metadata_names:
        lb_mdo.create_schema(name='lb_integration_source', kind=conversion["string"])
    return table  

def process_metadata_value(metadata_value, metadata_type:str, parent_name:str, metadata_name_key_to_schema:dict, divider:str="///"):
    """ Processes inbound values to ensure only valid values are added as metadata to Labelbox given the metadata type. Returns None if invalid or None
    Args:
        metadata_value              :   Required (any) - Value to-be-screeened and inserted as a proper metadata value to-be-uploaded to Labelbox
        metadata_type               :   Required (str) - Either "string", "datetime", "enum", or "number"
        parent_name                 :   Required (str) - Parent metadata field name
        metadata_name_key_to_schema :   Required (dict) - Dictionary where {key=metadata_field_name_key : value=metadata_schema_id}
        divider                     :   Required (str) - String delimiter for all name keys generated
    Returns:
        The proper data type given the metadata type for the input value. None if the value is invalud - should be skipped
    """
    if not metadata_value: # Catch empty values
        return_value = None
    if str(metadata_value) == "nan": # Catch NaN values
        return_value = None
    # By metadata type
    if metadata_type == "enum": # For enums, it must be a schema ID - if we can't match it, we have to skip it
        name_key = f"{parent_name}{divider}{str(metadata_value)}"
        if name_key in metadata_name_key_to_schema.keys():
            return_value = metadata_name_key_to_schema[name_key]
        else:
            return_value = None                  
    elif metadata_type == "number": # For numbers, it's ints as strings
        try:
            return_value = str(int(metadata_value))
        except:
            return_value = None                  
    elif metadata_type == "string": 
        return_value = str(metadata_value)
    else: # For datetime, it's an isoformat string
        if type(metadata_value) == str:
            return_value = parser.parse(metadata_value).astimezone(pytz.utc).replace(tzinfo=None)
        elif type(metadata_value) == datetime:
            return_value = metadata_value.astimezone(pytz.utc).replace(tzinfo=None)
        else:
            return_value = None       
    return return_value    
