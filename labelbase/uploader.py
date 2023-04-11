from labelbox import Client as labelboxClient
from labelbox import Dataset as labelboxDataset
from labelbox import Project as labelboxProject
import uuid
import math

def create_global_key_to_label_id_dict(client:labelboxClient, project_id:str, global_keys:list):
    """ Creates a dictionary where { key=global_key : value=label_id } by exporting labels from a project
    Args:
        client          :   Required (labelbox.client.Client) - Labelbox Client object    
        project_id      :   Required (str) - Labelbox Project ID
        global_keys     :   Required (list(str)) - List of global key strings
    Returns:
        Dictionary where { key=global_key : value=label_id }
    """      
    global_key_to_label_id_dict = {}
    project = client.get_project(project_id)
    labels = project.export_labels(download=True)
    for label in labels:
        if label['Global Key'] in global_keys:
            global_key_to_label_id_dict[label['Global Key']] = label['ID']
    return global_key_to_label_id_dict

def create_global_key_to_data_row_id_dict(client:labelboxClient, global_keys:list, timeout_seconds=600, batch_size:int=20000):
    """ Creates a dictionary where { key=global_key : value=data_row_id } using client.get_data_row_ids_for_global_keys()
    Args:
        client          :   Required (labelbox.client.Client) - Labelbox Client object    
        global_keys     :   Required (list(str)) - List of global key strings
    Returns:
        Dictionary where {key=global_key : value=data_row_id}
    """    
    global_key_to_data_row_dict = {}
    for i in range(0, len(global_keys), batch_size):
        gks = global_keys[i:] if i + batch_size >= len(global_keys) else global_keys[i:i+batch_size]  
        res = client.get_data_row_ids_for_global_keys(gks, timeout_seconds=timeout_seconds)
        if res['errors']:
            raise ValueError(f"{res}")
        global_key_to_data_row_dict.update({gks[i] : res['results'][i] for i in range(0, len(gks))})
    return global_key_to_data_row_dict

def check_global_keys(client:labelboxClient, global_keys:list, batch_size=20000):
    """ Checks if data rows exist for a set of global keys - if data rows exist, returns as dictionary { key=data_row_id : value=global_key }
    Args:
        client                  :   Required (labelbox.client.Client) - Labelbox Client object    
        global_keys             :   Required (list(str)) - List of global key strings
        batch_size              :   Optional (int) - Query check batch size, 20,000 is recommended        
    Returns:
        existing_drid_to_gk     :   Dictinoary where { key=data_row_id : value=global_key }
                                    If this = {}, then all global keys are free to use
    """
    # Initiate a dictionary where { key=data_row_id : value=global_key }
    existing_drid_to_gk = {}
    # Enforce global keys as strings
    global_keys_list = [str(x) for x in global_keys]      
    # Create a query job to get data row IDs given global keys
    query_str_1 = """query get_datarow_with_global_key($global_keys:[ID!]!){dataRowsForGlobalKeys(where:{ids:$global_keys}){jobId}}"""
    query_str_2 = """query get_job_result($job_id:ID!){dataRowsForGlobalKeysResult(jobId:{id:$job_id}){data{
                    accessDeniedGlobalKeys\ndeletedDataRowGlobalKeys\nfetchedDataRows{id}\nnotFoundGlobalKeys}jobStatus}}"""      
    # Batch global key checks
    for i in range(0, len(global_keys_list), batch_size):
        batch_gks = global_keys_list[i:] if i + batch_size >= len(global_keys_list) else global_keys_list[i:i+batch_size]  
        # Run the query job
        res = None
        while not res:
            query_job_id = client.execute(query_str_1, {"global_keys":batch_gks})['dataRowsForGlobalKeys']['jobId']
            res = client.execute(query_str_2, {"job_id":query_job_id})['dataRowsForGlobalKeysResult']['data']       
        # Check query job results for fetched data rows
        for i in range(0, len(res["fetchedDataRows"])):
            data_row_id = res["fetchedDataRows"][i]["id"]
            if data_row_id:
                existing_drid_to_gk[data_row_id] = batch_gks[i]
    return existing_drid_to_gk

def batch_create_data_rows(
    client:labelboxClient, upload_dict:dict, skip_duplicates:bool=True, 
    divider:str="___", batch_size:int=20000, verbose:bool=False):
    """ Uploads data rows, skipping duplicate global keys or auto-generating new unique ones. 
    
    upload_dict must be in the following format:
    {
        global_key : {
            "data_row" : {}, -- This is your data row upload as a dictionary
            "dataset_id" : "" -- Labelbox Dataset ID
        },
        global_key : {
            "data_row" : {},
            "dataset_id" : ""
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
    # Default error message    
    e = "Success"
    # Vet all global keys
    global_keys = list(upload_dict.keys()) # Get all global keys
    if verbose:
        print(f"Vetting global keys")
    for i in range(0, len(global_keys), batch_size): # Check global keys 20k at a time
        gks = global_keys[i:] if i + batch_size >= len(global_keys) else global_keys[i:i+batch_size] # Batch of global keys to vet 
        existing_data_row_to_global_key = check_global_keys(client, gks) # Returns empty list if there are no duplicates
        loop_counter = 0
        while existing_data_row_to_global_key:
            if skip_duplicates: # Drop in-use global keys if we're skipping duplicates
                if verbose:
                    print(f"Warning: Global keys in this upload are in use by active data rows, skipping the upload of data rows affected") 
                for gk in existing_data_row_to_global_key.values():
                    del upload_dict[gk]
                break
            else: # Create new suffix for taken global keys if we're not skipping duplicates
                loop_counter += 1 # Suffix counter     
                if verbose:
                    print(f"Warning: Global keys in this upload are in use by active data rows, attempting to add the following suffix to affected data rows: '{divider}{loop_counter}'")                   
                for egk in existing_data_row_to_global_key.values(): # For each existing global key, remove and replace with new global key
                    gk_root = egk if loop_counter == 1 else egk[:-(len(divider)+len(str(loop_counter)))] # Root global key, no suffix
                    new_gk = f"{gk_root}{divider}{loop_counter}" # New global key with suffix
                    upload_value = upload_dict[egk] # Grab data row from old global key
                    upload_value["data_row"]["global_key"] = new_gk # Update global key value in data row
                    del upload_dict[egk] # Delete old global key
                    upload_dict[new_gk] = upload_value # Replace with new data row / global key
                global_keys = list(upload_dict.keys()) # Make a new global key list
                gks = global_keys[i:] if i + batch_size >= len(global_keys) else global_keys[i:i+batch_size] # Determine batch
                existing_data_row_to_global_key = check_global_keys(client, gks) # Refresh existing_data_row_to_global_key
    if verbose:
        print(f"Global keys vetted")    
    # Dictionary where { key=dataset_id : value=list_of_uploads }
    dataset_id_to_upload_list = {}
    for gk in upload_dict:
        dataset_id = upload_dict[gk]["dataset_id"]
        data_row = upload_dict[gk]["data_row"]
        if dataset_id not in dataset_id_to_upload_list.keys():
            dataset_id_to_upload_list[dataset_id] = []
        dataset_id_to_upload_list[dataset_id].append(data_row)
    # Perform uploads grouped by dataset ID
    for dataset_id in dataset_id_to_upload_list:
        dataset = client.get_dataset(dataset_id)       
        upload_list = dataset_id_to_upload_list[dataset_id]
        if verbose:
            print(f'Beginning data row upload for Dataset with ID {dataset_id} - uploading {len(upload_list)} data rows')
        batch_number = 0
        for i in range(0,len(upload_list),batch_size):
            batch = upload_list[i:] if i + batch_size >= len(upload_list) else upload_list[i:i+batch_size] # Determine batch
            batch_number += 1
            if verbose:
                print(f'Batch #{batch_number}: {len(batch)} data rows')
            task = dataset.create_data_rows(batch)
            errors = task.errors
            if errors:
                if verbose: 
                    print(f'Error: Upload batch number {batch_number} unsuccessful')
                e = errors
                break
            else:
                if verbose: 
                    print(f'Success: Upload batch number {batch_number} successful')  
    if verbose:
        print(f'Upload complete - all data rows uploaded')
    return e, upload_dict

def batch_upload_annotations(
    client:labelboxClient, upload_dict:dict, import_name:str=str(uuid.uuid4()), 
    how:str="import", batch_size:int=10000, verbose=False):
    """ Batch imports labels given a batch size via MAL or LabelImport
    
    upload_dict must be in the following format:
    {
        global_key : {
            "project_id" : "", -- Labelbox Project ID
            "annotations" : [] -- List of annotations for a given data row
        },
        global_key : {
            "project_id" : "",
            "annotations" : []
        }  
    }      
    
    Args:
        client                      :   Required (labelbox.client.Client) - Labelbox Client object
        upload_dict                 :   Required (dict) - Dictionary in the format outlined above
        import_name                 :   Optional (str) - Name to give to import jobs - will have a batch number suffix
        how                         :   Optional (str) - Upload method - options are "mal" and "import" - defaults to "import"
        batch_size                  :   Optional (int) - Desired batch upload size - this size is determined by annotation counts, not by data row count
        verbose                     :   Optional (bool) - If True, prints information about code execution
    Returns: 
        A list of errors if there is one - if empty list, upload was successful
    """
    # Default error message 
    e = "Success" 
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
    # Dictionary where { key=project_id : value= { data_row_id : annotations_list } }
    project_id_to_upload_list = {}
    for gk in upload_dict:
        project_id = upload_dict[gk]["project_id"]
        data_row_id = upload_dict[gk]["annotations"][0]["dataRow"]["id"]
        annotations = upload_dict[gk]["annotations"]
        if project_id not in project_id_to_upload_list.keys():
            project_id_to_upload_dict[project_id] = {}
        project_id_to_upload_dict[project_id][data_row_id] = annotations
    batch_number = 0        
    # For each project, upload in batches grouped by data row IDs 
    for project_id in project_id_to_upload_dict:
        data_row_id_to_upload = project_id_to_upload_dict[project_id]
        # Get all data rows IDs to upload annotations for in this project
        data_row_ids_list = list(data_row_id_to_upload_dict.keys())
        if verbose:
            print(f"Uploading annotations for {len(data_row_ids_list)} data rows to project with ID {project_id}")          
        for i in range(0, len(data_row_ids_list), batch_size):
            # Determine which data row IDs are in this batch
            data_row_ids = data_row_ids_list[i:] if i+batch_size >= len(data_row_ids_list) else data_row_ids_list[i:i+batch_size]
            # Collect uploads from this batch
            upload = []
            for drid in data_row_ids:
                upload.extend(data_row_id_to_upload[drid])
            batch_number += 1
            if verbose:
                print(f"Batch #{batch_number}: {len(upload)} annotations for {len(data_row_ids_batch)} data rows")
            # Upload annotations
            import_request = upload_protocol.create_from_objects(client, project_id, f"{import_name}-{batch_number}", upload)
            errors = import_request.errors
            if errors:
                if verbose:
                    print(f'Error: upload batch number {batch_number} unsuccessful')
                e = errors
                break
            else:
                if verbose:
                    print(f'Success: upload batch number {batch_number} complete')   
    return e

def batch_rows_to_project(
    client:labelboxClient, upload_dict:dict, priority:int=5, 
    batch_name:str=str(uuid.uuid4()), batch_size:int=1000, verbose:bool=False):
    """ Takes a large amount of data row IDs and creates subsets of batches to send to a project
    
    upload_dict must be in the following format:
    {
        global_key : {
            "project_id" : "" -- Labelbox Project ID to batch data rows to
        },
        global_key : {
            "project_id" : ""
        }
    }   
    
    Args:
        client                      :   Required (labelbox.client.Client) - Labelbox Client object
        upload_dict                 :   Required (dict) - Dictionary in the format outlined above
        priority                    :   Optinoal (int) - Between 1 and 5, what priority to give to data row batches sent to projects
        batch_name                  :   Optional (str) : Prefix to add to batch name - script generates batch number, which it adds to said prefix
        batch_size                  :   Optional (int) : Size of batches to send to project
        verbose                     :   Optional (bool) - If True, prints information about code execution        
    Returns:
        Empty list if successful, errors if unsuccessful
    """
    # Default error message 
    e = "Success"
    # Create a dictionary where { key=project_id : value=list_of_data_row_ids }
    project_id_to_data_row_ids = {}
    global_key_to_data_row_id = create_global_key_to_data_row_id_dict(client=client, global_keys=list(upload_dict.keys()))
    for gk in upload_dict:
        project_id = upload_dict[gk]["project_id"]
        if project_id not in project_id_to_data_row_ids.keys():
            project_id_to_data_row_ids[project_id] = []
        project_id_to_data_row_ids[project_id].append(global_key_to_data_row_id[gk])
    # Create batches of data rows to projects in batches
    try:
        batch_number = 0
        for project_id in project_id_to_batch_dict:
            project = client.get_project(project_id)            
            data_row_ids = project_id_to_data_row_ids[project_id]
            if verbose:
                print(f"Sending {len(data_row_ids)} data rows to project with ID {project_id}")
            for i in range(0, len(data_row_ids), batch_size):
                batch_number += 1
                subset = data_row_ids[i:] if i+batch_size >= len(data_row_ids) else data_row_ids[i:i+batch_size]
                project.create_batch(name=f"{batch_name}-{batch_number}", data_rows=subset)
        if verbose:
            print(f"All data rows have been batched to the specified project(s)")
    except Exception as errors:
        e = errors
    return e

def batch_add_data_rows_to_model_run(
    client:labelboxClient, upload_dict:dict, batch_size:int=1000, verbose:bool=False):
    """ Adds existing Labelbox data rows (not labels) to a model run
    
    upload_dict must be in the following format:
    {
        global_key : {
            "model_run_id" : "" -- Labelbox Model Run ID to batch data rows / Labels to
        },
        global_key : {
            "model_run_id" : ""
        }
    }     
    
    Args:
        client                      :   Required (labelbox.client.Client) - Labelbox Client object
        upload_dict                 :   Required (dict) - Dictionary in the format outlined above
        batch_size                  :   Optional (int) : Size of batches to send to project
        verbose                     :   Optional (bool) - If True, prints information about code execution        
    Returns:
        Empty list if successful, errors if unsuccessful    
    """
    # Default error message     
    e = "Success"
    try:        
        # Dictionary where { key=model_run_id : list_of_global_keys }
        model_run_to_global_keys = {}
        for gk in upload_dict:
            model_run_id = upload_dict[gk]["model_run_id"]
            if model_run_id not in model_run_to_global_keys.keys():
                model_run_to_global_keys[model_run_id] = []
            model_run_to_global_keys[model_run_id].append(gk)
        # For each model_run, batch data rows in groups of batch_size
        for model_run_id in model_run_to_global_keys:
            model_run = client.get_model_run(model_run_id)
            global_keys = model_run_to_global_keys[model_run_id]
            for i in range(0, len(global_keys), batch_size):
                batch_number += 1
                subset = global_keys[i:] if i+batch_size >= len(global_keys) else global_keys[i:i+batch_size]
                model_run.upsert_data_rows(global_keys=subset)
    except Exception as errors:
        e = errors
    return e

def batch_add_ground_truth_to_model_run(
    client:labelboxClient, upload_dict:dict, batch_size:int=1000, verbose:bool=False):
    """ Adds existing Labelbox data rows (not labels) to a model run
    
    upload_dict must be in the following format:
    {
        global_key : {
            "model_run_id" : "", -- Labelbox Model Run ID to batch data rows / Labels to
            "project_id" : "" -- Labelbox Project ID to pull label IDs from
        },
        global_key : {
            "model_run_id" : "",
            "project_id" :
        }
    }     
    
    Args:
        client                      :   Required (labelbox.client.Client) - Labelbox Client object
        upload_dict                 :   Required (dict) - Dictionary in the format outlined above
        batch_size                  :   Optional (int) : Size of batches to send to project
        verbose                     :   Optional (bool) - If True, prints information about code execution        
    Returns:
        Empty list if successful, errors if unsuccessful    
    """
    # Default error message     
    e = "Success"
    try:        
        # Dictionary where { key=project_id : value=list_of_global_keys }
        project_id_to_global_keys = {}
        # Dictionary where { key=model_run_id : value=list_of_global_keys }
        model_run_id_to_global_keys = {}
        for gk in upload_dict:
            # Update project_id_to_global_keys
            project_id = upload_dict[gk]["project_id"]
            if project_id not in project_id_to_global_keys.keys():
                project_id_to_global_keys[project_id] = []
            project_id_to_global_keys[project_id].append(gk)
            # Update model_run_id_to_global_keys
            model_run_id = upload_dict[gk]["model_run_id"]
            if model_run_id not in model_run_id_to_global_keys.keys():
                model_run_id_to_global_keys[model_run_id] = []
            model_run_id_to_global_keys[model_run_id].append(gk)
        # Dictionary where { key=global_key : value=label_id }
        global_key_to_label_id = {}
        for project_id in project_id_to_global_keys:
            global_key_to_label_id.update(create_global_key_to_label_id_dict(client=client, project_id=project_id, global_keys=global_keys))
        # For each model_run, batch data rows in groups of batch_size
        for model_run_id in model_run_id_to_global_keys:
            model_run = client.get_model_run(model_run_id)
            global_keys = model_run_to_global_keys[model_run_id]
            label_ids = [global_key_to_label_id[gk] for gk in global_keys]
            for i in range(0, len(label_ids), batch_size):
                batch_number += 1
                subset = label_ids[i:] if i+batch_size >= len(label_ids) else label_ids[i:i+batch_size]
                model_run.upsert_labels(label_ids=subset)
    except Exception as errors:
        e = errors
    return e

def batch_upload_predictions(
    client:labelboxClient, upload_dict:dict, batch_size:int=1000, verbose:bool=False):
    """ Uploads predictions to a Labelbox Model Run
    
    upload_dict must be in the following format:
    {
        global_key : {
            "model_run_id" : "", -- Labelbox Model Run ID to batch data rows / Labels to
            "predictions" : [] -- List of Labelbox Predictions in NDJSON format
        },
        global_key : {
            "model_run_id" : "", -- Labelbox Model Run ID to batch data rows / Labels to
            "predictions" : [] -- List of Labelbox Predictions in NDJSON format
        }
    }     
    
    Args:
        client                      :   Required (labelbox.client.Client) - Labelbox Client object
        upload_dict                 :   Required (dict) - Dictionary in the format outlined above
        batch_size                  :   Optional (int) : Size of batches to send to project
        verbose                     :   Optional (bool) - If True, prints information about code execution        
    Returns:
        Empty list if successful, errors if unsuccessful    
    """
    # Default error message     
    e = "Success"
    try:
        # Dictionary where { key=model_run_id : value={key=global_key : value=list_of_prediction_ndjsons} } -- mrid_gk_preds
        mrid_gk_preds = {}
        for gk in upload_dict:
            mrid = upload_dict[gk]["model_run_id"]
            if mrid not in mrid_gk_preds.keys():
                mrid_gk_preds[mrid] = {}
            mrid_gk_preds[mrid][gk] = upload_dict[gk]["predictions"]
        for mrid in mrid_gk_preds:
            model_run = client.get_model_run(mrid)
            gk_to_preds = mrid_gk_preds[mrid]
            # Get all data rows IDs for this project
            global_keys = list(gk_to_preds.keys())
            if verbose:
                print(f"Uploading predictions for {len(list(global_keys))} data rows to Model {model_run.model_id}  Run {model.name}")          
            for i in range(0, len(global_keys), batch_size):
                global_keys = global_keys[i:] if i+batch_size >= len(global_keys) else global_keys[i:i+batch_size]
                upload = []
                for gk in global_keys:
                    upload.extend(gk_to_preds[gk])
                batch_number += 1
                if verbose:
                    print(f"Batch #{batch_number}: {len(upload)} annotations for {len(global_keys)} data rows")            
                if verbose:
                    print(f"Batch #{batch_number}: {len(upload)} annotations for {len(global_keys)} data rows")
                import_request = model_run.add_predictions(name=f"{import_name}-{batch_number}", predictions=upload)
                errors = import_request.errors
                if errors:
                    if verbose:
                        print(f'Error: upload batch number {batch_number} unsuccessful')
                    e = errors
                else:
                    if verbose:
                        print(f'Success: upload batch number {batch_number} complete')   
    except Exception as error:
        e = error
    return e
