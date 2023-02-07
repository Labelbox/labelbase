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
        GQL payload             :   Dictionary where {keys="accessDeniedGlobalKeys", "deletedDataRowGlobalKeys", "fetchedDataRows", "notFoundGlobalKeys"}
        existing_dr_to_gk       :   Dictinoary where {key=data_row_id : value=global_key} for data rows in use by global keys passed in. 
                                    If this = {}, then all global keys are free to use
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
    # If there are deleted data rows holding global keys, clear them and re-do the check
    if res["deletedDataRowGlobalKeys"]:
        client.clear_global_keys(res["deletedDataRowGlobalKeys"])   
        res = None
        while not res:
            query_job_id = client.execute(query_str_1, {"global_keys":global_keys})['dataRowsForGlobalKeys']['jobId']
            res = client.execute(query_str_2, {"job_id":query_job_id})['dataRowsForGlobalKeysResult']['data']  
    # Create a dictionary where {key=data_row_id : value=global_key}
    existing_dr_to_gk = {}
    for i in range(0, len(res["fetchedDataRows"])):
        data_row_id = res["fetchedDataRows"][i]["id"]
        global_key = global_keys[i]
        if data_row_id:
            existing_dr_to_gk[data_row_id] = global_key             
    return res, existing_dr_to_gk

def batch_create_data_rows(client:labelboxClient, upload_dict:dict, 
                           skip_duplicates:bool=True, divider:str="___", batch_size:int=20000, verbose:bool=False):
    """ Uploads data rows, skipping duplocate global keys or auto-generating new unique ones. Upload dict must be in the following format:
        {
            dataset_id : {
                global_key : {
                    "data_row" : {} -- This is your data row upload as a dictionary (must have keys "row_data", "global_key", at a minimum)
                }
                global_key : {
                    "data_row" : {}
                }
            },
            dataset_id : {
                global_key : {
                    "data_row" : {}
                }
                global_key : {
                    "data_row" : {}
                }
            }
        }    
    Args:
        client                                  :   Required (labelbox.client.Client) - Labelbox Client object
        upload_dict                             :   Required (dict) - Dictionary in the format outlined above
        skip_duplicates                         :   Optional (bool) - If True, will skip duplicate global_keys, otherwise will generate a unique global_key with a suffix "_1", "_2" and so on
        divider                                 :   Optional (str) - If skip_duplicates=False, uploader will auto-add a suffix to global keys to create unique ones, where new_global_key=old_global_key+divider+clone_counter
        batch_size                              :   Optional (int) - Upload batch size, 20,000 is recommended
        verbose                                 :   Optional (bool) - If True, prints information about code execution
    Returns:
        upload_errors                           :   Either a list Labelbox upload errors or an empty list if no errors
        updated_dict                            :   Updated dataset_to_global_key_to_upload_dict if global keys were removed or updated
    """
    updated_dict = {}
    for dataset_id in upload_dict.keys():
        dataset = client.get_dataset(dataset_id)
        global_key_to_upload_dict = upload_dict[dataset_id]
        global_keys_list = list(global_key_to_upload_dict.keys())
        payload, existing_data_row_to_global_key = check_global_keys(client, global_keys_list)
        if payload["deletedDataRowGlobalKeys"]:
            client.clear_global_keys(res["deletedDataRowGlobalKeys"])        
        payload, existing_data_row_to_global_key = check_global_keys(client, global_keys_list)
        loop_counter = 0
        while existing_data_row_to_global_key: # If existing data rows are using gloval keys
            if skip_duplicates: # Drop from global_key_to_upload_dict if we're skipping duplicates
                if verbose:
                    print(f"Warning: Global keys in this upload are in use by active data rows, skipping the upload of data rows affected") 
                for gk in existing_data_row_to_global_key.keys():
                    del global_key_to_upload_dict[gk]
                break
            else: # Create new suffix, replace in global_key_to_upload_dict, refresh existing_data_row_to_global_key
                loop_counter += 1 # Count the amount of attempts - this is used as a suffix                
                if verbose:
                    print(f"Warning: Global keys in this upload are in use by active data rows, attempting to add the following suffix to affected data rows: '{divider}{loop_counter}'")                   
                for gk in existing_data_row_to_global_key.values():
                    gk_root = gk if loop_counter == 1 else gk[:-(len(divider)+len(str(loop_counter)))] # Root global key, no suffix
                    new_gk = f"{gk_root}{divider}{loop_counter}" # New global key with suffix
                    upload_value = global_key_to_upload_dict[gk] # Grab data row upload
                    upload_value["data_row"]["global_key"] = new_gk # Replace the global key in our data row upload value
                    del global_key_to_upload_dict[gk] # Delete global key that's in use already
                    global_key_to_upload_dict[new_gk] = upload_value # Replace with new global key
                global_keys_list = list(global_key_to_upload_dict.keys()) # Make a new global key list
                payload, existing_data_row_to_global_key = check_global_keys(client, global_keys_list) # Refresh existing_data_row_to_global_key
        updated_dict[dataset_id] = global_key_to_upload_dict # Since we may have dropped/replaced some global keys, we will return a modified index            
        upload_list = [upload_value["data_row"] for upload_value in global_key_to_upload_dict.values()]
        if verbose:
            print(f'Beginning data row upload for dataset ID {dataset_id}: uploading {len(upload_list)} data rows')
        batch_number = 0
        for i in range(0,len(upload_list),batch_size):
            batch_number += 1 # Count uploads
            batch = upload_list[i:] if i + batch_size >= len(upload_list) else upload_list[i:i+batch_size] # Determine batch
            if verbose:
                print(f'Batch #{batch_number}: {len(batch)} data rows')
            task = dataset.create_data_rows(batch)
            task.wait_till_done()
            errors = task.errors
            if errors:
                if verbose: 
                    print(f'Error: Upload batch number {batch_number} unsuccessful')
                e = errors
                break
            else:
                if verbose: 
                    print(f'Success: Upload batch number {batch_number} successful')  
                e = []
    if verbose:
        print(f'Upload complete - all data rows uploaded')
    return e, updated_dict

def batch_upload_annotations(client:labelboxClient, project_id_to_upload_dict:dict, import_name:str=str(uuid.uuid4()), 
                             how:str="import", batch_size:int=10000, verbose=False):
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
    # Determine the upload type
    if how.lower() == "mal":
        from labelbox import MALPredictionImport as upload_protocol
        if verbose:
            print(f"Uploading annotations as non-submitted pre-labels (MAL)")            
    elif how.lower() == "import":
        from labelbox import LabelImport as upload_protocol
        if verbose:
            print(f"Uploading annotations as submitted labels (Label Import)")
    else:
        return f"No annotation upload attempted - import method must be wither 'mal' or 'import' - received value {how}"
    batch_number = 0        
    # For each project
    if project_id_to_upload_dict:
        for project_id in project_id_to_upload_dict:
            annotations = project_id_to_upload_dict[project_id]
            # Create a dicationary where {key=data_row_id : value=list_of_annotations}
            data_row_id_to_upload_dict = {}
            for annotation in annotations:
                data_row_id = annotation['dataRow']['id']
                if data_row_id not in data_row_id_to_upload_dict:
                    data_row_id_to_upload_dict[data_row_id] = [annotation]
                else:
                    data_row_id_to_upload_dict[data_row_id].append(annotation)                   
            # Create ndjson batches at the data row level            
            data_row_list = list(data_row_id_to_upload_dict.keys())
            if verbose:
                print(f"Uploading {len(annotations)} annotations for {len(data_row_list)} data rows to project with ID {project_id}")             
            for i in range(0, len(data_row_list), batch_size):
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
    else:
        return "No annotation upload attempted"


def batch_rows_to_project(client:labelboxClient, project_id_to_batch_dict:dict, priority:int=5, 
                          batch_name:str=str(uuid.uuid4()), batch_size:int=1000, verbose:bool=False):
    """ Takes a large amount of data row IDs and creates subsets of batches to send to a project
    Args:
        client                      :   Required (labelbox.client.Client) - Labelbox Client object
        project_id_to_batch_dict    :   Required (dict) Dictionary where {key=project_id : value=list_of_data_row_ids}
        priority                    :   Optinoal (int) - Between 1 and 5, what priority to give to data row batches sent to projects
        batch_name                  :   Optional (str) : Prefix to add to batch name - script generates batch number, which it adds to said prefix
        batch_size                  :   Optional (int) : Size of batches to send to project
        verbose                     :   Optional (bool) - If True, prints information about code execution        
    Returns:
        Empty list if successful, errors if unsuccessful
    """
    try:
        batch_number = 0
        for project_id in project_id_to_batch_dict:
            data_row_ids = project_id_to_batch_dict[project_id]
            if verbose:
                print(f"Sending {len(data_row_ids)} data rows to project with ID {project_id}")
            project = client.get_project(project_id)
            for i in range(0, len(data_row_ids), batch_size):
                batch_number += 1
                subset = data_row_ids[i:] if i+batch_size >= len(data_row_ids) else data_row_ids[i:i+batch_size]
                project.create_batch(name=f"{batch_name}-{batch_number}", data_rows=subset)
        if verbose:
            print(f"All data rows have been batched to the specified project(s)")
        return []  
    except Exception as e:
        print(f"Batching data rows to project unsuccessful: {e}")
        return e
