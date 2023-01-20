from labelbox import Client as labelboxClient
from labelbox import Dataset as labelboxDataset
from labelbase import connector

def batch_create_data_rows(client:lableboxClient, dataset:labelboxDataset, global_key_to_upload_dict:dict, 
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
    payload = connector.check_global_keys(client, global_keys_list)
    if payload:
        loop_counter = 0
        while len(payload['notFoundGlobalKeys']) != len(global_keys_list):
            # If global keys are taken by deleted data rows, clearn global keys from deleted data rows
            if payload['deletedDataRowGlobalKeys']:
                if verbose:
                    print(f"Warning: Global keys in this upload are in use by deleted data rows, clearing all global keys from deleted data rows")
                client.clear_global_keys(payload['deletedDataRowGlobalKeys'])
                payload = connector.check_global_keys(client, global_keys_list)
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
                payload = connector.check_global_keys(client, global_keys_list) 
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
  
