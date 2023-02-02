import uuid

def create_ndjsons(data_row_id:str, annotation_values:list, ontology_index:dict, divider:str="///"):
    """ From an annotation in the expected format, creates a Labelbox NDJSON of that annotation
    Args:
        data_row_id         :   Required (str) - Labelbox Data Row ID
        annotation_values   :   Required (list) - List of annotation value lists where 1 list element must correspond to the following format:
                                    For tools:
                                        bbox          :   [tool_name, top, left, height, width, [nested_classification_name_paths]]
                                        polygon       :   [tool_name, [(x, y), (x, y),...(x, y)], [nested_classification_name_paths]]
                                        line          :   [tool_name, [(x, y), (x, y),...(x, y)], [nested_classification_name_paths]]
                                        point         :   [tool_name, x, y, [nested_classification_name_paths]]
                                        mask          :   [tool_name, URL, colorRGB, [nested_classification_name_paths]]
                                        named-entity  :   [tool_name, start, end, [nested_classification_name_paths]]
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
            data_row_id=data_row_id, 
            annotation_value=annotation_value, 
            ontology_index=ontology_index, 
            divider=divider
        ))
    return ndjsons

def ndjson_builder(data_row_id:str, annotation_input:list, ontology_index:dict, divider:str="///"):
    """ Returns an ndjson of an annotation given a list of values - the values needed differ depending on the annotation type
    Args:
        data_row_id         :   Required (str) - Labelbox Data Row ID
        annotation_input    :   Required (list) - List that corresponds to a single annotation for a label in the following format
                                    For tools:
                                        bbox          :   [tool_name, top, left, height, width, [nested_classification_name_paths]]
                                        polygon       :   [tool_name, [(x, y), (x, y),...(x, y)], [nested_classification_name_paths]]
                                        line          :   [tool_name, [(x, y), (x, y),...(x, y)], [nested_classification_name_paths]]
                                        point         :   [tool_name, x, y, [nested_classification_name_paths]]
                                        mask          :   [tool_name, URL, colorRGB, [nested_classification_name_paths]]
                                        named-entity  :   [tool_name, start, end, [nested_classification_name_paths]]
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
        "dataRow" : {"id":data_row_id}
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
                    ndjson["classifications"].append(
                        classification_builder(
                            classification_path=f"{top_level_name}{divider}{classification_name}",
                            child_paths=get_child_paths(first=classification_name, paths=annotation_input[-1], divider=divider),
                            ontology_index=ontology_index,
                            divider=divider
                        )
                    )
    # Otherwise, the top level feature is a classification
    else:
        ndjson.update(
            classification_builder(
                classification_path=top_level_name, 
                answer_paths=get_child_paths(first=classification_name, paths=annotation_input, divider=divider),
                ontology_index=ontology_index,
                divider=divider
            )
        )
    return ndjson       

def classification_builder(classification_path:str, answer_paths:list, ontology_index:dict, divider:str="///"):
    """ Given a classification path and all its child paths, constructs an ndjson.
        If the classification answer's paths have nested classifications, will recursuively call this function.
    Args:
        
    Returns:
        
    """
    # classification_path = "tool///classification" or "classification"
    # answer_paths = ["answer///question_1///answer_1///question_1_1///answer_1_1", "answer///question_2///answer_1", "answer///question_2///answer_2"]
    c_type = ontology_index[classification_path]["type"]
    # classification_name = "classification"
    c_name = classification_path.split(divider)[-1] if divider in classification_path else classification_path
    classification_ndjson = {
        "name" : c_name
    }
    if c_type == "radio":
        answer_name = pull_first_name_from_paths(name_paths=answer_paths, divider=divider)[0]
        classification_ndjson["answer"] = {
            "name" : answer_name
        }
        n_c_paths = get_child_paths(first=answer_name, name_paths=answer_paths, divider=divider)
        n_c_names = pull_first_name_from_paths(name_paths=n_c_paths, divider=divider)
        for n_c_name in n_c_names:
            if n_c_name:
                if "classifications" not in classification_ndjson["answer"].keys():
                    classification_ndjson["answer"]["classifications"] = []
                n_a_paths = get_child_paths(first=n_c_name, paths=n_c_paths, divider=divider)  
                classification_ndjson["answer"]["classifications"].append(
                    classification_builder(
                        classification_path=f"{classification_path}{divider}{answer_name}{divider}{n_c_name}",
                        answer_paths=n_a_paths,
                        ontology_index=ontology_index,
                        divider=divider
                    )
                )
    elif classification_type == "checklist":
        classification_ndjson["answers"] = []
        answer_names = pull_first_name_from_paths(name_paths=answer_paths, divider=divider)
        for answer_name in answer_names:
            answer_ndjson = {
                "name" : answer_name
            }
            n_c_paths = get_child_paths(first=answer_name, name_paths=answer_paths, divider=divider)
            n_c_names = pull_first_name_from_paths(name_paths=n_c_paths, divider=divider)
            for n_c_name in n_c_names:
                if n_c_name:
                    if "classifications" not in answer_ndjson.keys():
                        answer_ndjson["classifications"] = []
                    n_a_paths = get_child_paths(first=n_c_name, paths=n_c_paths, divider=divider) 
                    answer_ndjson["answer"]["classifications"].append(
                        classification_builder(
                            classification_path=f"{classification_path}{divider}{answer_name}{divider}{n_c_name}",
                            answer_paths=n_a_paths,
                            ontology_index=ontology_index,
                            divider=divider                            
                        )
                    )
    else:
        classification_ndjson["answer"] = answer_paths[0]
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
                child_path += str(name)+str(divider)
            child_path = child_path[:-len(divider)] 
            child_paths.append(child_path)
    return child_paths                                                     
