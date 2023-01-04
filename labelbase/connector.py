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

def create_dataset_with_integration(client:labelboxClient, dataset_name:str, dataset_integration:str="DEFAULT", verbose:bool=False):
    """ Creates a Labelbox dataset given a desired dataset name and a desired delegated access integration name
    Args:
        client              :   Required (labelbox.client.Client) : Labelbox Client object with an api key
        dataset_name        :   Required (str) - Desired dataset name
        dataset_integration :   Optional (str) - Existing Labelbox delegated access setting for new dataset
        verbose             :   Optional (bool) - If True, prints information about code execution
    Returns:
        labelbox.shema.dataset.Dataset object
    """
    # Gets all iam_integration DB objects and find if any names match the input dataset_integration
    for iam_integration in client.get_organization().get_iam_integrations(): 
        if iam_integration.name == dataset_integration: # If the names match, reassign the iam_integration input value
            dataset_integration = iam_integration
            if verbose:
                print(f"Creating a Labelbox dataset with name {dataset_name} and delegated access integration name {dataset_integration.name}")
            break
    # If none match, use the default setting
    if (type(dataset_integration)==str) and (verbose==True):
        print(f"Creating a Labelbox dataset with name {dataset_name} and the default delegated access integration setting")
    # Create the Labelbox dataset 
    lb_dataset = client.create_dataset(name=dataset_name, iam_integration=dataset_integration) 
    return lb_dataset   
