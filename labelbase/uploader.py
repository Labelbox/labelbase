from labelbox import Client as labelboxClient
from labelbox import Dataset as labelboxDataset
from labelbox import Project as labelboxProject
import uuid
import math

def create_global_key_to_data_row_dict(client:labelboxClient, global_keys:list):
    """ Creates a dictionary where {key=global_key : value=data_row_id}
    Args:
        client          :   Required (labelbox.client.Client) - Labelbox Client object    
        global_keys     :   Required (list(str)) - List of global key strings
    Returns:
        Dictionary where {key=global_key : value=data_row_id}
    """
    res = client.get_data_row_ids_for_global_keys(global_keys, timeout_seconds=240)
    if res['errors']:
        raise ValueError(f"{res}")
    global_key_to_data_row_dict = {global_keys[i] : res['results'][i] for i in range(0, len(global_keys))}
    return global_key_to_data_row_dict

def check_global_keys(client:labelboxClient, global_keys:list):
    """ Checks if data rows exist for a set of global keys
    Args:
        client                  :   Required (labelbox.client.Client) - Labelbox Client object    
        global_keys             :   Required (list(str)) - List of global key strings
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

def batch_create_data_rows(client:labelboxClient, dataset:labelboxDataset, global_key_to_upload_dict:dict, 
                           skip_duplicates:bool=True, divider:str="___", batch_size:int=20000, verbose:bool=False):
    """ Uploads data rows, skipping duplocate global keys or auto-generating new unique ones
    Args:
        client                      :   Required (labelbox.client.Client) - Labelbox Client object
        dataset                     :   Required (labelbox.schema.dataset.Dataset) - Labelbox Dataset object
        global_key_to_upload_dict   :   Required (dict) - Dictionary where {key=global_key : value=data_row_dict to-be-uploaded to Labelbox}
        skip_duplicates             :   Optional (bool) - If True, will skip duplicate global_keys, otherwise will generate a unique global_key with a suffix "_1", "_2" and so on
        divider                     :   Optional (str) - If skip_duplicates=False, uploader will auto-add a suffix to global keys to create unique ones, where new_global_key=old_global_key+divider+clone_counter
        batch_size                  :   Optional (int) - Upload batch size, 20,000 is recommended
        verbose                     :   Optional (bool) - If True, prints information about code execution
    Returns:
        Upload errors
    """
    global_keys_list = list(global_key_to_upload_dict.keys())
    payload = check_global_keys(client, global_keys_list)
    if payload:
        loop_counter = 0
        while len(payload['notFoundGlobalKeys']) != len(global_keys_list):
            # If global keys are taken by deleted data rows, clearn global keys from deleted data rows
            if payload['deletedDataRowGlobalKeys']:
                if verbose:
                    print(f"Warning: Global keys in this upload are in use by deleted data rows, clearing all global keys from deleted data rows")
                client.clear_global_keys(payload['deletedDataRowGlobalKeys'])
                payload = check_global_keys(client, global_keys_list)
                continue
            # If global keys are taken by existing data rows, either skip them on upload or update the global key to have a "_{loop_counter}" suffix
            if payload['fetchedDataRows']:
                loop_counter += 1                    
                if verbose and skip_duplicates:
                    print(f"Warning: Global keys in this upload are in use by active data rows, skipping the upload of data rows affected")         
                elif verbose:
                    print(f"Warning: Global keys in this upload are in use by active data rows, attempting to add the following suffix to affected data rows: '{divider}{loop_counter}'")   
                for i in range(0, len(payload['fetchedDataRows'])):
                    current_global_key = str(global_keys_list[i])
                    # Add (or replace) a suffix to your working global_key
                    new_global_key = f"{current_global_key[:-len(divider)]}{divider}{loop_counter}" if current_global_key[-len(divider):-1] == divider else f"{current_global_key}{divider}{loop_counter}"
                    if payload['fetchedDataRows'][i] != "":                            
                        if skip_duplicates:
                            del global_key_to_upload_dict[current_global_key] # Delete this data_row_upload_dict from your upload_dict
                        else:
                            new_upload_dict = global_key_to_upload_dict[current_global_key] # Grab the existing data_row_upload_dict
                            del global_key_to_upload_dict[current_global_key] # Delete this data_row_upload_dict from your upload_dict
                            new_upload_dict['global_key'] = new_global_key # Put new global key values in this data_row_upload_dict
                            global_key_to_upload_dict[new_global_key] = new_upload_dict # Add your new data_row_upload_dict to your upload_dict                               
                global_keys_list = list(global_key_to_upload_dict.keys())
                payload = check_global_keys(client, global_keys_list) 
    upload_list = list(global_key_to_upload_dict.values())
    if verbose:
        print(f'Beginning data row upload: uploading {len(upload_list)} data rows')
    batch_number = 0
    for i in range(0,len(upload_list),batch_size):
        batch_number += 1
        batch = upload_list[i:] if i + batch_size >= len(upload_list) else upload_list[i:i+batch_size]
        if verbose:
            print(f'Batch #{batch_number}: {len(batch)} data rows')
        task = dataset.create_data_rows(batch)
        task.wait_till_done()
        errors = task.errors
        if errors:
            if verbose: 
                print(f'Error: upload batch number {batch_number} unsuccessful')
            return errors
        else:
            if verbose: 
                print(f'Success: upload batch number {batch_number} complete')  
    if verbose:
        print(f'Upload complete')
    return []

def batch_upload_annotations(client:labelboxClient, project_id_to_upload_dict:dict, import_name:str=str(uuid.uuid4()), 
                             how:str="import", batch_size:int=20000, verbose=False):
    """ Batch imports labels given a batch size via MAL or LabelImport
    Args:
        client                      :   Required (labelbox.client.Client) - Labelbox Client object
        project_id_to_upload_dict   :   Required (dict) Dictionary where {key=project_id - value=annotation_upload_list} - annotations must be in ndjson format
        import_name                 :   Optional (str) - Name to give to import jobs - will have a batch number suffix
        how                         :   Optional (str) - Upload method - options are "mal" and "import" - defaults to "import"
        batch_size                  :   Optional (int) - Desired batch upload size - this size is determined by annotation counts, not by data row count
        verbose                     :   Optional (bool) - If True, prints information about code execution
    Returns: 
        A list of errors if there is one, True if upload failed, False if successful
    """
    if how.lower() == "mal":
        from labelbox import MALPredictionImport as upload_protocol
        if verbose:
            print(f"Uploading {len(annotations)} annotations non-submitted pre-labels (MAL)")            
    elif how.lower() == "import":
        from labelbox import LabelImport as upload_protocol
        if verbose:
            print(f"Uploading {len(annotations)} annotations as submitted labels (Label Import)")
    else:
        raise ValueError(f"Import method must be wither 'mal' or 'import' - received value {how}")
    batch_number = 0        
    for project_id in project_id_to_upload_dict:
        data_row_id_to_upload_dict = {}
        for annotation in project_id_to_upload_dict[project_id]:
            data_row_id = annotation['dataRow']['id']
            if data_row_id not in data_row_id_to_upload_dict:
                data_row_id_to_upload_dict[data_row_id] = [annotation]
            else:
                data_row_id_to_upload_dict[data_row_id].append(annotation)
        data_row_list = len(list(data_row_id_to_upload_dict.keys()))
        for i in range(0, data_row_list, batch_size):
            data_row_batch = data_row_list[i:] if i+batch_size >= len(data_row_list) else data_row_list[i:i+batch_size]
            upload = []
            for annotation in project_id_to_upload_dict[project_id]:
                if annotation['dataRow']['id'] in data_row_batch:
                    upload.append(annotation)
            batch_number += 1
            import_request = upload_protocol.create_from_objects(client, project_id, f"{import_name}-{batch_number}", upload)
            errors = import_request.errors
            if errors:
                if verbose:
                    print(f'Error: upload batch number {batch_number} unsuccessful')
                return errors
            else:
                if verbose:
                    print(f'Success: upload batch number {batch_number} complete')               
    return []

def batch_rows_to_project(client:labelboxClient, project_id_to_batch_dict:dict, priority:int=5, batch_name:str=str(uuid.uuid4()), batch_size:int=1000):
    """ Takes a large amount of data row IDs and creates subsets of batches to send to a project
    Args:
        client                      :   Required (labelbox.client.Client) - Labelbox Client object
        project_id_to_batch_dict    :   Required (dict) Dictionary where {key=project_id : value=list_of_data_row_ids}
        priority                    :   Optinoal (int) - Between 1 and 5, what priority to give to data row batches sent to projects
        batch_name                  :   Optional (str) : Prefix to add to batch name - script generates batch number, which it adds to said prefix
        batch_size                  :   Optional (int) : Size of batches to send to project
    Returns:
        True
    """
    batch_number = 0
    for project_id in project_id_to_batch_dict:
        project - client.get_project(project_id)
        for i in range(0, len(project_id_to_batch_dict[project_id]), batch_size):
            batch_number += 1
            data_row_ids = project_id_to_batch_dict[project_id]
            subset = data_row_ids[i:] if i+batch_size >= len(data_row_ids) else data_row_ids[i:i+batch_size]
            project.create_batch(name=f"{batch_name}-{batch_number}", data_rows=subset)
    return True  
