from labelbox import Client as labelboxClient
from labelbox import Project as labelboxProject
from labelbase.ontology import get_ontology_schema_to_name_path
from labelbase.metadata import get_metadata_schema_to_name_key, get_metadata_schema_to_type
from labelbase.annotate import flatten_label

def export_and_flatten_labels(client:labelboxClient, project, include_metadata:bool=True, include_performance:bool=True, 
    include_agreement:bool=False, verbose:bool=False, mask_method:str="png", divider="///", export_filters:dict=None):
    """ Exports and flattens labels from a Labelbox Project
    Args:
        client:                 :   Required (labelbox.Client) - Labelbox Client object
        project                 :   Required (str / lablebox.Project) - Labelbox Project ID or lablebox.Project object to export labels from
        include_metadata        :   Optional (bool) - If included, exports metadata fields
        include_performance     :   Optional (bool) - If included, exports labeling performance
        include_agreement       :   Optional (bool) - If included, exports consensus scores       
        verbose                 :   Optional (bool) - If True, prints details about code execution; if False, prints minimal information
        mask_method             :   Optional (str) - Specifies your input mask data format
                                        - "url" leaves masks as-is
                                        - "array" converts URLs to numpy arrays
                                        - "png" converts URLs to png byte strings        
        divider                 :   Optional (str) - String delimiter for schema name keys and suffix added to duplocate global keys 
        export_filters          :   Optional (dict) - Filters for project export
                                        - "last_activity_at": [<start_date>, <end_date>] formatted as YYYY-MM-DD or YYYY-MM-DD hh:mm:ss, or None
                                        - "label_created_at": [<start_date>, <end_date>] formatted as YYYY-MM-DD or YYYY-MM-DD hh:mm:ss, or None
                                        - "data_row_ids": [<list of datarow ids>] 
    Returns:
        List of dictionaries where { key = column_name : value = row_value }
    """
    if mask_method not in ["url", "png", "array"]:
        raise ValueError(f"Please specify the mask_method you want to download your segmentation masks in - must be either 'url' 'png' or 'array'")
    project = project if type(project) == labelboxProject else client.get_project(project)
    if verbose:
        print(f"Exporting labels from Labelbox for project with ID {project.uid}")

    export_params = {'performance_details': True, 'label_details': True, 'datarow_details': True}
    if include_metadata:
        export_params['metadata_fields'] = True
        mdo = client.get_data_row_metadata_ontology()
        metadata_schema_to_type = get_metadata_schema_to_type(client=client, lb_mdo=mdo, invert=False)
        metadata_schema_to_name_key = get_metadata_schema_to_name_key(client=client, lb_mdo=mdo, invert=False, divider=divider)
    if export_filters is not None:
        task = project.export_v2(params=export_params, filters=export_filters)
    else:
        task = project.export_v2(params=export_params)
    task.wait_till_done()
    export = task.result
    if verbose:
        print(f"Export complete: {len(export)} labels exported")   
    
    ontology_index = get_ontology_schema_to_name_path(project.ontology(), invert=True, divider=divider, detailed=True)
    schema_to_name_path = get_ontology_schema_to_name_path(project.ontology(), invert=False, divider=divider, detailed=False)   
    flattened_labels = [] 
    if verbose:
        print(f"Flattening labels...")
    for label in export:
        for nested_label in label['projects'][project.uid]['labels']:
            if not nested_label['performance_details']['skipped']:
                flat_label = {
                    "row_data" : label['data_row']["row_data"],
                    "data_row_id" : label['data_row']["id"],
                    "label_id" : nested_label["id"],
                }
                if 'global_key' in label['data_row']:
                    flat_label["global_key"] = label['data_row']['global_key']
                else:
                    flat_label["global_key"] = None

                if 'external_id' in label['data_row']:
                    flat_label["external_id"] = label['data_row']['external_id']
                else:
                    flat_label["external_id"] = None
                res = flatten_label(client=client, label_dict=nested_label, ontology_index=ontology_index, datarow_id=label['data_row']['id'], mask_method=mask_method, divider=divider)            
                for key, val in res.items():
                    flat_label[f"annotation{divider}{str(key)}"] = val
                if include_agreement:
                    if 'benchmark_score' in nested_label['performance_details']:
                        flat_label["benchmark_score"] = nested_label['performance_details']['benchmark_score']
                    elif 'consensus_score' in nested_label['performance_details']:
                        flat_label["consensus_score"] = nested_label['performance_details']['consensus_score']
                if include_performance:
                    flat_label["created_by"] = nested_label['label_details']['created_by']
                    flat_label["seconds_to_create"] = nested_label['performance_details']['seconds_to_create']
                    flat_label["seconds_to_review"] = nested_label['performance_details']['seconds_to_review']
                    flat_label["seconds_to_label"] = nested_label['performance_details']['seconds_to_create'] - nested_label['performance_details']['seconds_to_review']
                    for metadata in label['metadata_fields']:
                        try:
                            if metadata['value'] in metadata_schema_to_name_key.keys():
                                name_path = metadata_schema_to_name_key[metadata['value']].split(divider)
                                field_name = name_path[0]
                                metadata_value = name_path[1]
                                metadata_type = metadata_schema_to_type[metadata['value']]
                            else:
                                field_name = metadata['schema_name']
                                for key in metadata_schema_to_name_key.keys():
                                    if metadata_schema_to_name_key[key] == field_name:
                                        metadata_type = metadata_schema_to_type[key]
                                if type(metadata['value']) == list:
                                    values = []
                                    for value in metadata['value']:
                                        values.append(value['schema_name'])
                                    metadata_value = values
                                else:
                                    metadata_value = metadata['value']
                        except:
                            field_name = metadata['schema_name']
                            for key in metadata_schema_to_name_key.keys():
                                if metadata_schema_to_name_key[key] == field_name:
                                    metadata_type = metadata_schema_to_type[key]
                            if type(metadata['value']) == list:
                                values = []
                                for value in metadata['value']:
                                    values.append(value['schema_name'])
                                metadata_value = values
                            else:
                                metadata_value = metadata['value']
                        if field_name != "lb_integration_source":
                            flat_label[f'metadata{divider}{metadata_type}{divider}{field_name}'] = metadata_value
                flattened_labels.append(flat_label)
    if verbose:
        print(f"Labels flattened")            
    return flattened_labels 
  
  
