from labelbox import Client as labelboxClient
from labelbase import connector
from labelbox.schema.dataset import Dataset as labelboxDataset
from labelbox.schema.project import Project as labelboxProject
from labelbox.schema.data_row_metadata import DataRowMetadataKind
from google.api_core import retry
import math
import uuid
from datetime import datetime
from dateutil import parser
import pytz


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
    def __init__(self, lb_api_key=None, lb_endpoint='https://api.labelbox.com/graphql', lb_enable_experimental=False, lb_app_url="https://app.labelbox.com"):  
        self.lb_client = labelboxClient(lb_api_key, endpoint=lb_endpoint, enable_experimental=lb_enable_experimental, app_url=lb_app_url)
      
    def sync_metadata_fields(self, table, get_columns_function, add_column_function, get_unique_values_function, metadata_index:dict={}, verbose:bool=False):
        """ Ensures Labelbox's Metadata Ontology and your input have all necessary metadata fields / columns given a metadata_index
        Args:
            table                       :   Required - Input user table
            get_columns_function        :   Required (function) - Function that can get the column names from the table provided, returns list of strings
            add_column_function         :   Required (function) - Function that can add an empty column to the table provided, returns table
            get_unique_values_function  :   Required (function) - Function that grabs all unique values from a column, returns list of strings
            metadata_index              :   Optional (dict) - Dictionary where {key=column_name : value=metadata_type} - metadata_type must be one of "enum", "string", "datetime" or "number"
            verbose                     :   Required (bool) - If True, prints information about code execution
        Returns:
            Updated table if successful, False if not
        """
        # Get your metadata ontology, grab all the metadata field names
        lb_mdo, lb_metadata_names = connector.refresh_metadata_ontology(self.lb_client)
        # Convert your meatdata_index values from strings into labelbox.schema.data_row_metadata.DataRowMetadataKind types
        conversion = {"enum" : DataRowMetadataKind.enum, "string" : DataRowMetadataKind.string, "datetime" : DataRowMetadataKind.datetime, "number" : DataRowMetadataKind.number}
        # Check to make sure the value in your metadata index is one of the accepted values        
        connector.enforce_metadata_index(metadata_index, verbose)
        # If your table doesn't have columns for all your metadata_field_names, make columns for them
        if type(table) != bool:
            if metadata_index:
                column_names = get_columns_function(table)
                for metadata_field_name in metadata_index.keys():
                    if metadata_field_name not in column_names:
                        table = add_column_function(table, column_name=metadata_field_name, default_value=None)
        # If Labelbox doesn't have metadata for all your metadata_field_names, make Labelbox metadata fields
        for metadata_field_name in metadata_index.keys():
            metadata_type = metadata_index[metadata_field_name]
            # Check to see if a metadata index input is a metadata field in Labelbox. If not, create the metadata field in Labelbox. 
            if metadata_field_name not in lb_metadata_names:
                enum_options = get_unique_values_function(table, metadata_field_name) if metadata_type == "enum" else []
                lb_mdo.create_schema(name=metadata_field_name, kind=conversion[metadata_type], options=enum_options)
                lb_mdo, lb_metadata_names = connector.refresh_metadata_ontology(self.lb_client)
        if 'lb_integration_source' not in lb_metadata_names:
            lb_mdo.create_schema(name='lb_integration_source', kind=conversion["string"])
        return table      

    def get_metadata_schema_to_name_key(self, lb_mdo=False, divider="///", invert=False):
        """ Creates a dictionary where {key=metadata_schema_id: value=metadata_name_key} 
        - name_key is name for all metadata fields, and for enum options, it is "parent_name{divider}child_name"
        Args:
            lb_mdo              :   Optional (labelbox.schema.data_row_metadata.DataRowMetadataOntology) - Labelbox metadata ontology
            divider             :   Optional (str) - String separating parent and enum option metadata values
            invert              :   Optional (bool) - If True, inverts the dictionary to be where {key=metadata_name_key: value=metadata_schema_id}
        Returns:
            Dictionary where {key=metadata_schema_id: value=metadata_name_key} - or the inverse
        """
        lb_mdo = self.lb_client.get_data_row_metadata_ontology() if not lb_mdo else lb_mdo
        lb_metadata_dict = lb_mdo.reserved_by_name
        lb_metadata_dict.update(lb_mdo.custom_by_name)
        metadata_schema_to_name_key = {}
        for metadata_field_name_key in lb_metadata_dict:
            if type(lb_metadata_dict[metadata_field_name_key]) == dict:
                metadata_schema_to_name_key[lb_metadata_dict[metadata_field_name_key][next(iter(lb_metadata_dict[metadata_field_name_key]))].parent] = str(metadata_field_name_key)
                for enum_option in lb_metadata_dict[metadata_field_name_key]:
                    metadata_schema_to_name_key[lb_metadata_dict[metadata_field_name_key][enum_option].uid] = f"{str(metadata_field_name_key)}{str(divider)}{str(enum_option)}"
            else:
                metadata_schema_to_name_key[lb_metadata_dict[metadata_field_name_key].uid] = str(metadata_field_name_key)
        return_value = metadata_schema_to_name_key if not invert else {v:k for k,v in metadata_schema_to_name_key.items()}
        return return_value  

    @retry.Retry(predicate=retry.if_exception_type(Exception), deadline=240.)
    def upload_local_file(self, file_path:str):
        """ Wraps client.upload_file() in retry logic
        Args:
            file_path   :   Required (str) - Data row file path
        Returns:
            URL corresponding to the uploaded asset
        """ 
        return self.lb_client.upload_file(file_path)

    def batch_create_data_rows(self, dataset:labelboxDataset, global_key_to_upload_dict:dict, skip_duplicates:bool=True, divider:str="___", batch_size:int=20000, verbose:bool=False):
        """ Uploads data rows, skipping duplocate global keys or auto-generating new unique ones
        Args:
            dataset                     :   Required (labelbox.schema.dataset.Dataset) : Labelbox Dataset object
            global_key_to_upload_dict   :   Required (dict) : Dictionary where {key=global_key : value=data_row_dict to-be-uploaded to Labelbox}
            skip_duplicates             :   Optional (bool) - If True, will skip duplicate global_keys, otherwise will generate a unique global_key with a suffix "_1", "_2" and so on
            divider                     :   Optional (str) - If skip_duplicates=False, uploader will auto-add a suffix to global keys to create unique ones, where new_global_key=old_global_key+divider+clone_counter
            batch_size                  :   Optional (int) : Upload batch size, 20,000 is recommended
            verbose                     :   Required (bool) - If True, prints information about code execution
        Returns:
            Upload errors
        """
        global_keys_list = list(global_key_to_upload_dict.keys())
        payload = connector.check_global_keys(self.lb_client, global_keys_list)
        if payload:
            loop_counter = 0
            while len(payload['notFoundGlobalKeys']) != len(global_keys_list):
                # If global keys are taken by deleted data rows, clearn global keys from deleted data rows
                if payload['deletedDataRowGlobalKeys']:
                    if verbose:
                        print(f"Warning: Global keys in this upload are in use by deleted data rows, clearing all global keys from deleted data rows")
                    self.lb_client.clear_global_keys(payload['deletedDataRowGlobalKeys'])
                    payload = connector.check_global_keys(self.lb_client, global_keys_list)
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
                    payload = connector.check_global_keys(self.lb_client, global_keys_list) 
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
    
    def get_or_create_dataset(self, name:str, integration:str="DEFAULT", verbose:bool=False):
        """ Gets or creates a Labelbox dataset given a dataset name and an integration name
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
                print(f'Using existing dataset with ID {dataset.uid}')
        except:
            dataset = connector.create_dataset_with_integration(client=self.lb_client, name=name, integration=integration, verbose=verbose)
            if verbose:
                print(f'Created a new dataset with ID {dataset.uid}') 
        return dataset      
    
    def get_ontology_schema_to_name_path(ontology, divider:str="///", invert:bool=False, detailed:bool=False):
        """ Recursively iterates through an ontology to create a dictionary where {key=schema_id : value=name_path}
        Where name_path = parent{divider}answer{divider}parent{divider}answer.... where divider="///"
        Args:
            ontology_normalized     :   Required (dict or labelbox.schema.ontology.Ontology) - Either Labelbox Ontology object or labelbox.schema.ontology.Ontology.normalized dictionary
            divider                 :   Optional (str) - String delineating the tool/classification/answer path for a given schema ID
            invert                  :   Optional (bool) - If True, will invert the dictionary to where {key=name_path : value=schema_id}
            detailed                :   Optional (bool) - If True, the dictionary will have the same key, but the values will be {"name" "color" "type" "kind" "parent_schema_ids"}
        Returns:
            Dictionary where {key=schema_id : value=name_path} - or the inverse - value can be more detailed if detailed=True
        """
        def map_layer(feature_dict:dict={}, node_layer:list= [], parent_name_path:str="", divider:str="///", invert:bool=False, detailed:bool=False, encoded_value:int=0):
            """ Recursive function that does the following for each node in a node_layer:
                    1. Creates a name_path given the parent_name_path
                    2. Adds schema_id : name_path to your working dictionary
                    3. If there's another layer for a given node, recursively calls itself, passing it its own name key as it's childrens' parent_name_path
            Args:
                feature_dict        :   Required (dict) - Building dictionary of ontology information
                node_layer          :   Required (list) - A list of ontology classification, tool, or option dictionaries
                parent_name_path    :   Required (str) - A concatenated list of parent node names separated with `divider` value, creating a unique mapping key
                invert              :   Optional (bool) - If True, will invert the dictionary to where {key=name_path : value=schema_id}
                divider             :   Optional (str) - String delineating the tool/classification/answer path for a given schema ID
                detailed            :   Optional (bool) - If True, the dictionary will have the same key, but the values will be {"name" "color" "type" "kind" "parent_schema_ids"}
                encoded_value       :   Optional (int) - Counts the number of nodes in an ontology, creating an encoded numerical value for schemas
            Returns:
                feature_dict
            """
            if node_layer:
                for node in node_layer:
                    encoded_value += 1
                    if "tool" in node.keys():
                        node_name = node["name"]
                        next_layer = node["classifications"]
                        node_type = node["tool"]
                    node_kind = "tool"   
                elif "instructions" in node.keys():
                    node_name = node["instructions"]
                    next_layer = node["options"]
                    node_kind = "classification"
                    node_type = node["type"]                        
                else:
                    node_type = "option"
                    node_name = node["label"]
                    next_layer = node.get("options", [])
                    node_kind = "branch_option" if next_layer else "leaf_option" 
                name_path = f"{parent_name_path}{divider}{node_name}" if parent_name_path else node_name
                dict_key = node['featureSchemaId'] if invert else name_path
                dict_value = name_path if invert else node['featureSchemaId']
                if detailed:
                    dict_value = {"type":node_type,"kind":node_kind, "encoded_value" : encoded_value}
                    if invert:
                        dict_value["name_path"]=name_path
                    else:
                        dict_value["schema_id"]=node['featureSchemaId']
                feature_dict.update({dict_key : dict_value})
                if next_layer:
                    feature_dict, encoded_value = map_layer(feature_dict, next_layer, name_path, divider, invert=invert, detailed=detailed, encoded_value=encoded_value)
        return feature_dict, encoded_value
    if type(ontology) == labelbox.Ontology:
        ontology_normalized = ontology.normalized
    elif type(ontology) == dict:
        ontology_normalized = ontology
    else:
        raise TypeError(f"Input for ontology must be either a Lablbox ontology object or a dictionary representation of a Labelbox ontology - received input of type {ontology}") 
    if ontology_normalized["tools"]:
        working_dictionary, working_encoded_value = map_layer(feature_dict={}, node_layer=ontology_normalized["tools"], divider=divider, invert=invert, detailed=detailed)
    else:
        working_dictionary = {} 
        working_encoded_value = 0
    if ontology_normalized["classifications"]:
        working_dictionary, working_encoded_value = map_layer(feature_dict=working_dictionary, node_layer=ontology_normalized["classifications"], divider=divider, invert=invert, detailed=detailed, encoded_value=working_encoded_value)
    return working_dictionary
 
    def batch_upload_annotations(self, project:labelboxProject, annotations:list, import_name:str=str(uuid.uuid4()), how:str="MAL", batch_size:int=20000, verbose=False):
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

    def process_metadata_value(self, metadata_value, metadata_type:str, parent_name:str, metadata_name_key_to_schema:dict, divider:str="///"):
        """ Processes inbound values to ensure only valid values are added as metadata to Labelbox given the metadata type. Returns None if invalid or None
        Args:
            metadata_value              :   Required (any) - Value to-be-screeened and inserted as a proper metadata value to-be-uploaded to Labelbox
            metadata_type               :   Required (str) - Either "string", "datetime", "enum", or "number"
            parent_name                 :   Required (str) - Parent metadata field name
            metadata_name_key_to_schema :   Required (dict) - Dictionary where {key=metadata_field_name_key : value=metadata_schema_id}
            divider                     :   Required (str) - String delimiter for all name keys generated
        Returns:
            The proper data type given the metadata type for the input value. None if the value is invalud - should be skipped
        """
        if not metadata_value: # Catch empty values
            return_value = None
        if str(metadata_value) == "nan": # Catch NaN values
            return_value = None
        # By metadata type
        if metadata_type == "enum": # For enums, it must be a schema ID - if we can't match it, we have to skip it
            name_key = f"{parent_name}{divider}{str(metadata_value)}"
            if name_key in metadata_name_key_to_schema.keys():
                return_value = metadata_name_key_to_schema[name_key]
            else:
                return_value = None                  
        elif metadata_type == "number": # For numbers, it's ints as strings
            try:
                return_value = str(int(row_value))
            except:
                return_value = None                  
        elif metadata_type == "string": 
            return_value = str(metadata_value)
        else: # For datetime, it's an isoformat string
            if type(metadata_value) == str:
                return_value = parser.parse(metadata_value).astimezone(pytz.utc).replace(tzinfo=None)
            elif type(metadata_value) == datetime.datetime:
                return_value = metadata_value.astimezone(pytz.utc).replace(tzinfo=None)
            else:
                return_value = None       
        return return_value    
    
