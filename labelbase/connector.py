from labelbox import Client
from google.api_core import retry
  
@retry.Retry(predicate=retry.if_exception_type(Exception), deadline=240.)
def upload_local_file(lb_client:Client, file_path:str):
    """ Wraps client.upload_file() in retry logic
    Args:
        lb_client   :   Required (labelbox.client.Cient) - Labelbox Client object
        file_path   :   Required (str) - Data row file path
    Returns:
        URL corresponding to the uploaded asset
    """ 
    return file_path, lb_client.upload_file(file_path)    
