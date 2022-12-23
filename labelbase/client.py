from labelbox import Client as labelboxClient
from labelbox.schema.dataset import Dataset as labelboxDataset
from labelbox.schema.project import Project as labelboxProject
from labelbox.schema.data_row_metadata import DataRowMetadataKind
import math
import uuid


class Client:
    """ A Labelboost Client, containing a Labelbox Client, that can perform a plethora of helper functions
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
    
    def refresh_metadata_ontology(self):
        """ Refreshes a Labelbox Metadata Ontology
        Returns 
            lb_mdo              :   labelbox.schema.data_row_metadata.DataRowMetadataOntology
            lb_metadata_names   :   List of metadata field names from a Labelbox metadata ontology
        """
        lb_mdo = self.lb_client.get_data_row_metadata_ontology()
        lb_metadata_names = [field['name'] for field in lb_mdo._get_ontology()]
        return lb_mdo, lb_metadata_names
    
    def enforce_metadata_index(self, metadata_index:dict, verbose:bool=False):
        """ Ensure your metadata_index is in the proper format. Returns True if it is, and False if it is not
        Args:
            metadata_index      :   Required (dict) - Dictionary where {key=metadata_field_name : value=metadata_type}
            verbose             :   Required (bool) - If True, prints information about code execution
        Returns:
            True if the metadata_index is valid, False if not
        """
        for metadata_field_name in metadata_index:
            if metadata_index[metadata_field_name] not in ["enum", "string", "datetime", "number"]:
                if verbose:
                    print(f"Invalid value in metadata_index for key {metadata_field_name} - must be `enum`, `string`, `datetime`, or `number`")
                return False
        if verbose:
            print(f"Valid metadata_index")
        return True
      
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
        lb_mdo, lb_metadata_names = self.refresh_metadata_ontology()
        # Convert your meatdata_index values from strings into labelbox.schema.data_row_metadata.DataRowMetadataKind types
        conversion = {"enum" : DataRowMetadataKind.enum, "string" : DataRowMetadataKind.string, "datetime" : DataRowMetadataKind.datetime, "number" : DataRowMetadataKind.number}
        # Check to make sure the value in your metadata index is one of the accepted values        
        check = self.enforce_metadata_index(metadata_index, verbose)
        if not check:
            return False
        # If your table doesn't have columns for all your metadata_field_names, make columns for them
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
                lb_mdo, lb_metadata_names = self.refresh_metadata_ontology()
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
        global_keys_list = list(global_key_to_upload_dict.keys())
        payload = check_global_keys(self.lb_client, global_keys_list)
        if payload:
            loop_counter = 0
            while len(payload['notFoundGlobalKeys']) != len(global_keys_list):
                # If global keys are taken by deleted data rows, clearn global keys from deleted data rows
                if payload['deletedDataRowGlobalKeys']:
                    if verbose:
                        print(f"Warning: Global keys in this upload are in use by deleted data rows, clearing all global keys from deleted data rows")
                    self.lb_client.clear_global_keys(payload['deletedDataRowGlobalKeys'])
                # If global keys are taken by existing data rows, either skip them on upload or update the global key to have a "_{loop_counter}" suffix
                elif payload['fetchedDataRows']:
                    loop_counter += 1                    
                    if verbose and skip_duplicates:
                        print(f"Warning: Global keys in this upload are in use by active data rows, skipping the upload of data rows affected")         
                    elif verbose:
                        print(f"Warning: Global keys in this upload are in use by active data rows, adding the following suffix to affected data rows: '{divider}{loop_counter}'")   
                    for i in range(0, len(payload['fetchedDataRows'])):
                        current_global_key = str(global_keys_list[i])
                        new_global_key = f"{current_global_key[:-3]}{divider}{loop_counter}" if current_global_key[-3:-1] == divider else f"{current_global_key}{divider}{loop_counter}"
                        if payload['fetchedDataRows'][i] != "":                            
                            if skip_duplicates:
                                del global_key_to_upload_dict[current_global_key] # Delete this data_row_upload_dict from your upload_dict
                            else:
                                new_upload_dict = global_key_to_upload_dict[current_global_key] # Grab the existing data_row_upload_dict
                                del global_key_to_upload_dict[current_global_key] # Delete this data_row_upload_dict from your upload_dict
                                new_upload_dict['global_key'] = new_global_key # Put new global key values in this data_row_upload_dict
                                global_key_to_upload_dict[new_global_key] = new_upload_dict # Add your new data_row_upload_dict to your upload_dict
                    global_keys_list = list(global_key_to_upload_dict.keys())
                payload = check_global_keys(self.lb_client, global_keys_list)
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
                    if type(errors) == str:
                        print(f'Data Row Creation Error: {errors}')
                    else:
                        print(f'Data Row Creation Error: {errors[0]}')
                return errors
        return []

    def create_dataset_with_integration(self, dataset_name:str, integration_name="DEFAULT", verbose=False):
        """ Creates a Labelbox dataset given a desired dataset name and a desired delegated access integration name
        Args:
            client              :   Required (labelbox.client.Client) : Labelbox Client object with an api key
            dataset_name        :   Required (str) : Desired dataset name
            integration_name    :   Optional (str) : Existing Labelbox delegated access setting for new dataset
            verbose             :   Optional (bool) - If True, prints information about code execution
        Returns:
            labelbox.shema.dataset.Dataset object
        """
        for iam_integration in self.lb_client.get_organization().get_iam_integrations(): # Gets all iam_integration DB objects
            if iam_integration.name == integration_name: # If the names match, reassign the iam_integration input value
                integration_name = iam_integration
                if verbose:
                    print(f"Creating a Labelbox dataset with name {dataset_name} and delegated access name {integration_name.name}")
                break
        if (type(integration_name) == str) and (verbose):
            print(f"Creating a Labelbox dataset with name {dataset_name} and the default delegated access setting")
        lb_dataset = self.lb_client.create_dataset(name=dataset_name, iam_integration=integration_name) # Create the Labelbox dataset
        if verbose:
            print(f"Dataset ID: {lb_dataset.uid}")    
        return lb_dataset         

    def get_ontology_schema_to_name_path(self, ontology_normalized:dict, divider:str="///", invert:bool=False):
        """ Recursively iterates through an ontology to create a dictionary where {key=schema_id : value=name_path}
        Where name_path = parent{divider}answer{divider}parent{divider}answer.... where divider="///"
        Args:
            ontology_normalized     :   Required (dict) : Ontology as a dictionary from ontology.normalized (where type(ontology) == labelbox.schema.ontology.Ontology)
            divider                 :   Optional (str) - String delineating the tool/classification/answer path for a given schema ID
            invert                  :   Optional (bool) : If True, will invert the dictionary to where {key=name_path : value=schema_id}
        Returns:
            Dictionary where {key=schema_id : value=name_path} - or the inverse
        """
        def map_layer(feature_dict:dict={}, node_layer:list= [], parent_name_path:str="", divider:str="///"):
            """ Recursive function that does the following for each node in a node_layer:
                    1. Creates a name_path given the parent_name_path
                    2. Adds schema_id : name_path to your working dictionary
                    3. If there's another layer for a given node, recursively calls itself, passing it its own name key as it's childrens' parent_name_path
            Args:
                feature_dict              :     Dictionary where {key=schema_id : value=name_path}
                node_layer                :     A list of classifications, tools, or option dictionaries
                parent_name_path           :     A concatenated list of parent node names separated with "///" creating a unique mapping key
            Returns:
                feature_dict
            """
            if node_layer:
                for node in node_layer:
                    if "tool" in node.keys():
                        node_name = node["name"]
                        next_layer = node["classifications"]
                    elif "instructions" in node.keys():
                        node_name = node["instructions"]
                        next_layer = node["options"]
                    else:
                        node_name = node["label"]
                        next_layer = node["options"] if 'options' in node.keys() else []
                    if parent_name_path:
                        name_path = parent_name_path + divider + node_name
                    else:
                        name_path = node_name
                    feature_dict.update({node['featureSchemaId'] : name_path})
                    if next_layer:
                        feature_dict = map_layer(feature_dict, next_layer, name_path. divider)
            return feature_dict
        ontology_schema_to_name_path = map_layer(feature_dict={}, node_layer=ontology_normalized["tools"], divider=divider) if ontology_normalized["tools"] else {}
        if ontology_normalized["classifications"]:
            ontology_schema_to_name_path = map_layer(feature_dict=ontology_schema_to_name_path, node_layer=ontology_normalized["classifications"], divider=divider)
        if invert:
            return {v: k for k, v in ontology_schema_to_name_path.items()} 
        else:
            return ontology_schema_to_name_path        
 
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
                    print(f'There were errors with this upload - see the return value for more details')
                return errors
        return []
