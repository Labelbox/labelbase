from labelbox import Client as labelboxClient
from labelbox import Project as labelboxProject
from labelbase.ontology import get_ontology_schema_to_name_path
from labelbase.metadata import get_metadata_schema_to_name_key, get_metadata_schema_to_type
from labelbase.annotate import flatten_label

def export_and_flatten_labels(
    client:labelboxClient, project, include_metadata:bool=True, include_performance:bool=True, 
    include_agreement:bool=False, verbose:bool=False, divider="///"):
    """ Exports and flattens labels from a Labelbox Project
    Args:
        client:                 :   Required (labelbox.Client) - Labelbox Client object
        project                 :   Required (str / lablebox.Project) - Labelbox Project ID or lablebox.Project object to export labels from
        include_metadata        :   Optional (bool) - If included, exports metadata fields
        include_performance     :   Optional (bool) - If included, exports labeling performance
        include_agreement       :   Optional (bool) - If included, exports consensus scores       
        verbose                 :   Optional (bool) - If True, prints details about code execution; if False, prints minimal information
        divider                 :   Optional (str) - String delimiter for schema name keys and suffix added to duplocate global keys 
    Returns:
        List of dictionaries where { key = column_name : value = row_value }
    """
    project = project if type(project) == labelboxProject else client.get_project(project)
    if verbose:
        print(f"Exporting labels from Labelbox for project with ID {project.uid}")
    export = project.export_labels(download=True)
    if verbose:
        print(f"Export complete: {len(export)} labels exported")   
    if include_metadata:
        data_row_ids = list(set([label['DataRow ID'] for label in export]))
        if verbose:
            print(f"Exporting metadata from Labelbox for {len(data_row_ids)} data row IDs")
        mdo = client.get_data_row_metadata_ontology()
        metadata_export = mdo.bulk_export(data_row_ids=data_row_ids)
        metadata_export_index = {x.data_row_id : x for x in metadata_export}
        metadata_schema_to_type = get_metadata_schema_to_type(client=client, lb_mdo=mdo, invert=False)
        metadata_schema_to_name_key = get_metadata_schema_to_name_key(client=client, lb_mdo=mdo, invert=False, divider=divider)
        if verbose:
            print(f"Metadata export complete")
    ontology_index = get_ontology_schema_to_name_path(project.ontology(), invert=True, divider=divider, detailed=True)
    schema_to_name_path = get_ontology_schema_to_name_path(project.ontology(), invert=False, divider=divider, detailed=False)   
    flattened_labels = [] 
    if verbose:
        print(f"Flattening labels...")
    for label in export:
        if not label['Skipped']:
            flat_label = {
                "global_key" : label["Global Key"],
                "row_data" : label["Labeled Data"],
                "data_row_id" : label["DataRow ID"],
                "label_id" : label["ID"],
                "external_id" : label["External ID"]
            }
            res = flatten_label(label_dict=label, ontology_index=ontology_index, schema_to_name_path=schema_to_name_path, divider=divider)            
            for key, val in res.items()}:
                flat_label[f"annotation{divider}{str(key)}"] = val
            if include_agreement:
                flat_label["consensus_score"] = label["Agreement"]
            if include_performance:
                flat_label["created_by"] = label["Created By"]
                flat_label["seconds_to_create"] = label["Seconds to Create"]
                flat_label["seconds_to_review"] = label["Seconds to Review"]
                flat_label["seconds_to_label"] = label["Seconds to Label"]
            if include_metadata:
                data_row_metadata = metadata_export_index[label["DataRow ID"]].fields
                for metadata in data_row_metadata:
                    metadata_type = metadata_schema_to_type[metadata.schema_id]
                    if metadata.value in metadata_schema_to_name_key.keys():
                        name_path = metadata_schema_to_name_key[metadata.value].split(divider)
                        field_name = name_path[0]
                        metadata_value = name_path[1]
                    else:
                        field_name = metadata.name
                        metadata_value = metadata.value
                    if field_name != "lb_integration_source":
                        flat_label[f'metadata{divider}{metadata_type}{divider}{field_name}'] = metadata_value
            flattened_labels.append(flat_label)
    if verbose:
        print(f"Labels flattened")            
    return flattened_labels 
  
  
