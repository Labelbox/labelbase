from labelbox import Client as labelboxClient
from labelbox.schema.dataset import Dataset as labelboxDataset

class Client:
    """ A Labelbase Client, containing a Labelbox Client, that can perform a plethora of helper functions
    Args:
        lb_api_key                  :   Required (str) - Labelbox API Key
        lb_endpoint                 :   Optinoal (bool) - Labelbox GraphQL endpoint
        lb_enable_experimental      :   Optional (bool) - If `True` enables experimental Labelbox SDK features
        lb_app_url                  :   Optional (str) - Labelbox web app URL
    Attributes:
        lb_client                   :   labelbox.Client object
    Key Functions:
    """
    def __init__(self, lb_api_key:str=None, lb_endpoint:str='https://api.labelbox.com/graphql', lb_enable_experimental:bool=False, lb_app_url:str="https://app.labelbox.com"):  
        self.lb_client = labelboxClient(lb_api_key, endpoint=lb_endpoint, enable_experimental=lb_enable_experimental, app_url=lb_app_url)

    def check_global_keys(self, global_keys:list):
        """ Checks if data rows exist for a set of global keys
        Args:
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
            query_job_id = self.lb_client.execute(query_str_1, {"global_keys":global_keys})['dataRowsForGlobalKeys']['jobId']
            res = self.lb_client.execute(query_str_2, {"job_id":query_job_id})['dataRowsForGlobalKeysResult']['data']
        return res               
    
    def get_or_create_dataset(self, name:str, integration:str="DEFAULT", verbose:bool=False):
        """ Gets or creates a Labelbox dataset given a dataset name and a deleagted access integration name
        Args:
            name                :   Required (str) - Desired dataset name
            integration         :   Optional (str) - Existing Labelbox delegated access setting for new dataset
            verbose             :   Optional (bool) - If True, prints information about code execution
        Returns:
            labelbox.schema.dataset.Dataset object
        """
        try: 
            dataset = next(self.lb_client.get_datasets(where=(labelboxDataset.name==name)))
            if verbose:
                print(f'Got existing dataset with ID {dataset.uid}')
        except:
            for iam_integration in self.lb_client.get_organization().get_iam_integrations(): 
                if iam_integration.name == integration: # If the names match, reassign the iam_integration input value
                    integration = iam_integration
                    if verbose:
                        print(f'Creating a Labelbox dataset with name "{name}" and delegated access integration name {integration.name}')
                    break
            # If none match, use the default setting
            if (type(integration)==str) and (verbose==True):
                print(f'Creating a Labelbox dataset with name "{name}" and the default delegated access integration setting')
            # Create the Labelbox dataset 
            dataset = self.lb_client.create_dataset(name=name, iam_integration=integration)             
            if verbose:
                print(f'Created a new dataset with ID {dataset.uid}') 
        return dataset
       
