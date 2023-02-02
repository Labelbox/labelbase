import uuid

def create_ndjsons(data_row_id:str, annotation_values:list, annotation_type:str, ontology_index:dict, divider:str="///"):
    """ From an annotation in the expected format, creates a Labelbox NDJSON of that annotation
    Args:
        data_row_id         :   Required (str) - Labelbox Data Row ID
        annotation_values   :   Required (list) - List of annotation values for a given data row in the following format 
                                    For tools:
                                        bbox          :   [[name_paths], [top, left, height, width], [name_paths], [top, left, height, width]]
                                        polygon       :   [[name_paths], [(x, y), (x, y),...(x, y)], [name_paths], [(x, y), (x, y),...(x, y)]]
                                        point         :   [[name_paths], [x, y], [name_paths], [x, y]]
                                        mask          :   [[name_paths], URL, colorRGB], [name_paths], URL, colorRGB]]
                                                              - URL must be an accessible string
                                        line          :   [[name_paths], [(x, y), (x, y),...(x, y)], [name_paths], [(x, y), (x, y),...(x, y)]]
                                        named-entity  :   [[name_paths], [start, end], [name_paths], [start, end]]
                                    For classifications:
                                        radio         :   [name_paths]
                                        check         :   [name_paths]
                                        text          :   [name_paths] -- the last string in a text name path is the text value itself
        annotation_values   :   Required (list) - List of annotation values for a given data row in the following format 
        annotation_type     :   Required (str) - Type of annotation in question - must be a string of one of the above options
        ontology_index      :   Required (dict) - Dictionary created from running:
                                                  labelbase.ontology.get_ontology_schema_to_name_path(ontology, divider=divider, invert=True, detailed=True)
        divider             :   Optional (str) - String delimiter for all name keys generated for parent/child schemas                                                  
    """
    ndjsons = []
    for annotation_value in annotation_values:
        ndjsons.append(ndjson_builder(
          data_row_id=data_row_id, annotation_value=annotation_value, 
          annotation_type=annotation_type, ontology_index=ontology_index, divider=divider
        ))
    return ndjsons

def ndjson_builder(data_row_id:str, annotation_input:list, ontology_index:dict, divider:str="///"):
    """
    Args:
        data_row_id         :   Required (str) - Labelbox Data Row ID
        annotation_input    :   Required (list) - List that corresponds to a single annotation for a label in the following format
                                    For tools:
                                        bbox          :   [tool_name, top, left, height, width, [classification_name_paths]]
                                        polygon       :   [tool_name, [(x, y), (x, y),...(x, y)], [classification_name_paths]]
                                        line          :   [tool_name, [(x, y), (x, y),...(x, y)], [classification_name_paths]]
                                        point         :   [tool_name, x, y, [classification_name_paths]]
                                        mask          :   [tool_name, URL, colorRGB, [classification_name_paths]]
                                        named-entity  :   [tool_name, start, end, [classification_name_paths]]
                                    For classifications:
                                        radio         :   [name_paths]
                                        check         :   [name_paths]
                                        text          :   [name_paths] -- the last string in a text name path is the text value itself
        annotation_type     :   Required (str) - Type of annotation in question - must be a string of one of the above options
        ontology_index      :   Required (dict) - Dictionary created from running:
                                                  labelbase.ontology.get_ontology_schema_to_name_path(ontology, divider=divider, invert=True, detailed=True)
        divider             :   Optional (str) - String delimiter for all name keys generated for parent/child schemas                                                  
    Returns
        NDJSON representation of an annotation
    """
    top_level_name = annotation_input[0] if divider not in annotation_input[0] else annotation_input[0].split(divider)[0]   
    ndjson = {
        "uuid" : str(uuid.uuid4()),
        "dataRow" : {"id":data_row_id}}
    }  
    annotation_type = ontology_index[nested_class_name_path]["type"]
    # Catches tools
    if annotation_type in ["bbox", "polygon", "line", "point", "mask", "named-entity"]:
        ndjson["name"] = top_level_name
        if annotation_type == "bbox":
            ndjson[annotation_type] = {"top":annotation_input[1],"left":annotation_input[2],"height":annotation_input[3],"width":annotation_input[4]}
        elif annotation_type in ["polygon", "line"]:
            ndjson[annotation_type] = [{"x":xy_pair[0],"y":xy_pair[1]} for xy_pair in annotation_input[1]]     
        elif annotation_type == "point":
            ndjson[annotation_type] {"x":annotation_input[1],"y":annotation_input[2]}
        elif annotation_type == "mask":
            ndjson[annotation_type] = {"instanceURI":annotation_input[1],"colorRGB":annotation_input[2]}
        else: # Only one left is named-entity 
            ndjson["location"] = {"start" annotation_input[1],"end":annotation_input[2]}
        if annotation_input[-1]:
            ndjson["classifications"] = []
            for classification_name_paths in annotation_input[-1]:
                classification_names = pull_first_name_from_paths(classification_name_paths, divider=divider)
                for classification_name in classification_names:
                    ndjson["classifications"].append(classification_builder(
                        classification_path=f"{top_level_name}{divider}{classification_name}",
                        child_paths=get_child_paths(first=classification_name, paths=annotation_input[-1], divider=divider),
                        ontology_index=ontology_index
                    ))
    # Otherwise, the top level feature is a classification
    else:
        ndjson.update(
            classification_builder(
                classification_path=top_level_name, 
                child_paths=get_child_paths(first=classification_name, paths=annotation_input, divider=divider),
                ontology_index=ontology_index
            )
        )
    return ndjson       

def classification_builder(classification_path:str, child_paths:list, ontology_index:dict):
    """ Given a classification path and all its child paths, constructs an ndjson.
        If the classification answer's paths have nested classifications, will recursuively call this function.
    Args:
        
    Returns:
        
    """
    classification_name = classification_path.split(divider)[0]
    classification_type = ontology_index[classification_name]["type"]
    classification_ndjson = {
        "name" : classification_name
    }
    if classification_type == "radio":
        if divider in 
        classification_answer = child_paths[0].split(divider)[0] if divider in child_paths[0] else child_paths[0]
        classification_ndjson["answer"]["name"] = classification_answer
        if len(classification_path.split(divider)) > 2:
            classification_ndjson["answer"]["classifications"] = []
    elif classification_type == "checklist":
        
    else:
        classification_ndjson["answer"] = child_paths[0]
    return classification_ndjson    

def pull_first_name_from_paths(name_paths:list, divider:str="///"):
    """ Pulls the first name from every name path in a list a divider-delimited name paths
    Args:
        name_paths          :   Required (list) - List of name paths
        divider             :   Optional (str) - String delimiter for all name keys generated for parent/child schemas     
    Returns:
        List of unique first names from a given name path
    """    
    firsts = []
    for name_path in name_paths:
        firsts.append(str(name_path.split(divider)[0]))
    return list(set(firsts))

def get_child_paths(first, name_paths, divider:str="///"):
    """ From a list of name paths, grabs paths starting with the `first` string and removes the `first` name from the name path
    Args
        first               :   Required (str) - The parent feature name you want to find paths for
        name_paths          :   Required (list) - List of name paths
        divider             :   Optional (str) - String delimiter for all name keys generated for parent/child schemas             
    Returns
        List of children name paths
    """
    child_paths = []
    for path in name_paths:
        if path.startswith(first):
            child_path = ""
            for name in path.split(divider)[1:]:
                child_path += name+divider
            child_path = child_path[:-len(divider)] 
            child_paths.append(child_path)
    return child_paths                                                     
