from labelbox import Client as labelboxClient
from datetime import datetime
from dateutil import parser
import pytz

def check_global_keys(client:labelboxClient, global_keys:list):
    """ Checks if data rows exist for a set of global keys
    Args:
        client                  : Required (labelbox.client.Client) : Labelbox Client object
        global_keys             : Required (list(str)) : List of global key strings
    Returns:
        True if global keys are available, False if not
    """
    query_keys = [str(x) for x in global_keys]
    # Create a query job to get data row IDs given global keys
    query_str_1 = """query get_datarow_with_global_key($global_keys:[ID!]!){dataRowsForGlobalKeys(where:{ids:$global_keys}){jobId}}"""
    query_str_2 = """query get_job_result($job_id:ID!){dataRowsForGlobalKeysResult(jobId:{id:$job_id}){data{
                    accessDeniedGlobalKeys\ndeletedDataRowGlobalKeys\nfetchedDataRows{id}\nnotFoundGlobalKeys}jobStatus}}"""        
    res = None
    while not res:
        query_job_id = client.execute(query_str_1, {"global_keys":global_keys})['dataRowsForGlobalKeys']['jobId']
        res = client.execute(query_str_2, {"job_id":query_job_id})['dataRowsForGlobalKeysResult']['data']
    return res  

def create_dataset_with_integration(client:labelboxClient, name:str, integration:str="DEFAULT", verbose:bool=False):
    """ Creates a Labelbox dataset given a desired dataset name and a desired delegated access integration name
    Args:
        client              :   Required (labelbox.client.Client) : Labelbox Client object
        name                :   Required (str) - Desired dataset name
        integration         :   Optional (str) - Existing Labelbox delegated access setting for new dataset
        verbose             :   Optional (bool) - If True, prints information about code execution
    Returns:
        labelbox.shema.dataset.Dataset object
    """
    # Gets all iam_integration DB objects and find if any names match the input dataset_integration
    for iam_integration in client.get_organization().get_iam_integrations(): 
        if iam_integration.name == integration: # If the names match, reassign the iam_integration input value
            integration = iam_integration
            if verbose:
                print(f'Creating a Labelbox dataset with name "{name}" and delegated access integration name {integration.name}')
            break
    # If none match, use the default setting
    if (type(integration)==str) and (verbose==True):
        print(f'Creating a Labelbox dataset with name "{name}" and the default delegated access integration setting')
    # Create the Labelbox dataset 
    dataset = client.create_dataset(name=name, iam_integration=integration) 
    return dataset   

def refresh_metadata_ontology(client:labelboxClient):
    """ Refreshes a Labelbox Metadata Ontology
    Args:
        client              :   Required (labelbox.client.Client) : Labelbox Client object    
    Returns:
        lb_mdo              :   labelbox.schema.data_row_metadata.DataRowMetadataOntology
        lb_metadata_names   :   List of metadata field names from a Labelbox metadata ontology
    """
    lb_mdo = client.get_data_row_metadata_ontology()
    lb_metadata_names = [field['name'] for field in lb_mdo._get_ontology()]
    return lb_mdo, lb_metadata_names

def enforce_metadata_index(metadata_index:dict, verbose:bool=False):
    """ Ensure your metadata_index is in the proper format. Returns True if it is, and False if it is not
    Args:
        metadata_index      :   Required (dict) - Dictionary where {key=metadata_field_name : value=metadata_type}
        verbose             :   Required (bool) - If True, prints information about code execution
    Returns:
        True if the metadata_index is valid, False if not
    """
    if metadata_index:
        for metadata_field_name in metadata_index:
            if metadata_index[metadata_field_name] not in ["enum", "string", "datetime", "number"]:
                raise ValueError(f"Invalid value in metadata_index for key {metadata_field_name} - must be `enum`, `string`, `datetime`, or `number`")
        if verbose:
            print(f"Valid metadata_index")
    else:
        if verbose:
            print(f"No metadata_index provided")
    return

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
            return_value = str(int(row_value))
        except:
            return_value = None                  
    elif metadata_type == "string": 
        return_value = str(metadata_value)
    else: # For datetime, it's an isoformat string
        if type(metadata_value) == str:
            return_value = parser.parse(metadata_value).astimezone(pytz.utc).replace(tzinfo=None)
        elif type(metadata_value) == datetime.datetime:
            return_value = metadata_value.astimezone(pytz.utc).replace(tzinfo=None)
        else:
            return_value = None       
    return return_value   
