from labelbox import Client as labelboxClient

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
