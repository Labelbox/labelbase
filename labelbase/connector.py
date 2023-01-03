from labelbox import Client as labelboxClient
from google.api_core import retry
  
@retry.Retry(predicate=retry.if_exception_type(Exception), deadline=240.)
def upload_local_file(lb_client:labelboxClient, file_path:str):
    """ Wraps client.upload_file() in retry logic
    Args:
        lb_client   :   Required (labelbox.client.Cient) - Labelbox Client object
        file_path   :   Required (str) - Data row file path
    Returns:
        URL corresponding to the uploaded asset
    """ 
    return lb_client.upload_file(file_path)    

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
