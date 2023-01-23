from labelbox import Client as labelboxClient
from labelbox.schema.dataset import Dataset as labelboxDataset

def get_or_create_dataset(client:labelboxClient, name:str, integration:str="DEFAULT", verbose:bool=False):
    """ Gets or creates a Labelbox dataset given a dataset name and a deleagted access integration name
    Args:
        name                :   Required (str) - Desired dataset name
        integration         :   Optional (str) - Existing Labelbox delegated access setting for new dataset
        verbose             :   Optional (bool) - If True, prints information about code execution
    Returns:
        labelbox.schema.dataset.Dataset object
    """
    try: 
        dataset = next(client.get_datasets(where=(labelboxDataset.name==name)))
        if verbose:
            print(f'Got existing dataset with ID {dataset.uid}')
    except:
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
        if verbose:
            print(f'Created a new dataset with ID {dataset.uid}') 
    return dataset
