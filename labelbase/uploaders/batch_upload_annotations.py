from labelbox import Client as labelboxClient
from labelbox import Project as labelboxProject
import uuid
import math

def batch_upload_annotations(client:labelboxClient, project:labelboxProject, annotations:list, import_name:str=str(uuid.uuid4()), 
                             how:str="MAL", batch_size:int=20000, verbose=False):
    """ Batch imports labels given a batch size via MAL or LabelImport
    Args:
        project         :   Required (labelbox.schema.project.Project) - Labelbox Project object
        annotations     :   Required (list) - List of annotations as ndjson dictionaries
        import_name     :   Optional (str) - Name to give to import jobs - will have a batch number suffix
        how             :   Optional (str) - Upload method - options are "mal" and "labelimport" - defaults to "labelimport"
        batch_size      :   Optional (int) - Desired batch upload size - this size is determined by annotation counts, not by data row count
        verbose         :   Optional (bool) - If True, prints information about code execution
    Returns: 
        A list of errors if there is one, True if upload failed, False if successful
    """
    if how == "mal":
        from labelbox import MALPredictionImport as upload_protocol
        if verbose:
            print(f"Uploading {len(annotations)} annotations non-submitted pre-labels (MAL)")            
    else:
        from labelbox import LabelImport as upload_protocol
        if how != "labelimport":
            if verbose:
                print(f"'how' variable was neither `mal` or `labelimport` - defaulting to Label Import")
        if verbose:
            print(f"Uploading {len(annotations)} annotations as submitted labels (Label Import)")
    batch_count = int(math.ceil(len(annotations) / len(batch_size))) # Determine number of batches needed
    batch_dict = {number : {} for number in range(0, batch_count)} # Dictionary where {key=batch_number : value={key=data_row_id : value=annotation_list}}
    for annotation in annotations:
        in_batch = False 
        for batch_number in batch_dict:
            if annotation['dataRow']['id'] in batch_dict[batch_number].keys(): # If this annotation's data row is part of a batch, add the annotation to that batch
                in_batch = True
                batch_dict[batch_number][annotation['dataRow']['id']].append(annotation)
        if not in_batch: # If this annotation's data row is not part of a batch, add the annotation to the batch under the batch_size
            for batch_number in batch_dict:
                if len(list(batch_dict[batch_number].keys())) >= batch_size:
                    continue
                else:
                    batch_dict[batch_number][annotation['dataRow']['id']] = [annotation]
    for batch_number in batch_dict:
        batch = []
        for data_row_id in batch_dict[batch].keys():
            batch.extend(batch_dict[batch_number][data_row_id])
        if verbose:
            print(f'Batch Number {batch_number}: Uploading {len(batch)} annotations')
        import_request = upload_protocol.create_from_objects(self.lb_client, project.uid, f"{import_name}-{batch_number}", batch)
        errors = import_request.errors
        if errors:
            if verbose:
                print(f'Error: upload batch number {batch_number} unsuccessful')
            return errors
        else:
            if verbose:
                print(f'Success: upload batch number {batch_number} complete')               
    return []
  
