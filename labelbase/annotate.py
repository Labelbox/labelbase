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

def ndjson_builder(data_row_id:str, annotation_input:list, annotation_type:str, ontology_index:dict, divider:str="///"):
    """
    Args:
        data_row_id         :   Required (str) - Labelbox Data Row ID
        annotation_input    :   Required (list) - List that corresponds to a single annotation for a label in the following format
                                    For tools:
                                        bbox          :   [[name_paths], top, left, height, width]
                                        polygon       :   [[name_paths], [(x, y), (x, y),...(x, y)]]
                                        line          :   [[name_paths], [(x, y), (x, y),...(x, y)]]                                        
                                        point         :   [[name_paths], x, y]
                                        mask          :   [[name_paths], URL, colorRGB]
                                        named-entity  :   [[name_paths], start, end]
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
    ndjson = {
      "uuid" : str(uuid.uuid4()),
      "dataRow" : {"id":data_row_id}}
    }  
    # Catches tools
    if annotation_type in ["bbox", "polygon", "line", "point", "mask", "named-entity"]:
        annotation_name_path = annotation_input[0][0]
        top_level_name = annotation_name_path if divider not in annotation_name_path else annotation_name_path.split(divider)[0]
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
    # Otherwise, the top level feature is a classification
    else:
        top_level_name =  annotation_input[0].split(divider)[0]
        ndjson["name"] = top_level_name
        if annotation_type in ["radio", "checklist"]:
            updated_paths = remove_first_from_paths(name_paths=annotation_input, divider=divider)
        if annotation_type == "radio":
            ndjson["answer"] = build_answer_ndjson(name_paths=updated_paths, parent_path=top_level_name, ontology_index=ontology_index, divider=divider)
        elif annotation_type == "checklist":
            ndjson["answers"] = []
            answer_paths = pull_first_from_paths(name_paths=updated_paths, divider=divider)
            for answer in answers:
                updated_paths = pull_children_paths(first=answer, name_paths=annotation_input, divider=divider)
                ndjson["answers"].append(build_answer_ndjson(name_paths=updated_paths, ontology_index=ontology_index, divider=divider))
        else: # Catches text answers
            ndjson["answer"] = annotation_input[0].split(divider)[1]
    return ndjson            

def build_answer_ndjson(name_paths:list, parent_path:str, ontology_index:dict, divider:str="///"):
    """ Given a list of name paths (where the first name is shared), creates an ndjson by reading the ontology index
    Args:
        name_paths          :   Required (list) - List of name paths that all share the same first answer name
        parent_path         :   Required (str) - Name path that leads to the shared first name
        ontology_index      :   Required (dict) - Dictionary created from running:
                                                  labelbase.ontology.get_ontology_schema_to_name_path(ontology, divider=divider, invert=True, detailed=True)
        divider             :   Optional (str) - String delimiter for all name keys generated for parent/child schemas                                                                                                            
    Returns:
    """
    # From your name paths, pull the shared top-level answer
    answer = pull_first_from_paths(name_paths=name_paths, divider=divider)[0]
    # For this top-level answer, construct a full name path
    answer_name_path = f"{parent_path}{divider}{answer}"
    # Start your answer dict
    return_ndjson = {"name" : answer}
    # Remove your answer name from your name paths - you're left with nested name paths
    nested_class_paths = remove_first_from_paths(name_paths=name_paths, divider=divider)
    for nested_class_path in nested_class_paths:
        if nested_class_path:
            if "classifications" not in return_ndjson.keys():
                return_ndjson["classifications"] = []
    # If there are nested name paths, recursively call this function to create answer ndjsons for each nested classification
    if "classifications" in return_ndjson.keys():
        # Grab all unique nested classfication names
        nested_class_names = pull_first_from_paths(name_paths=updated_paths, divider=divider)
        for nested_class_name in nested_class_names:
            # Construct a full name path for this nested classification to determine the classification type
            nested_class_name_path = f"{answer_name_path}{divider}{nested_class_name}"
            nested_class_type = ontology_index[nested_class_name_path]
            # Start your nested classification dict
            nested_class_ndjson = {"name" : nested_class_name}
            # If it's a radio, there should only be one answer - so grab all name paths that start with your classification name
            if nested_class_type == "radio":
                # Grab all name paths starting with your nested class
                relevant_name_paths = pull_children_paths(first=nested_class_name, name_paths=nested_class_paths, divider=divider)
                # Remove the nested class, leaving your answer name paths
                nested_answer_paths = remove_first_from_paths(name_paths=relevant_name_paths, divider=divider)
                nested_class_ndjson["answer"] = build_answer_ndjson(
                    name_paths=nested_answer_paths, parent_path=nested_class_name_path, ontology_index=ontology_index divider=divider
                )
            # If it's a checklist, there could be multiple answers - so grab all name paths that start with your classification name
            elif nested_class_type == "checklist":
                nested_class_ndjson["answers"] = []
            # If it's a text, there isn't any nested classifications - we can grab the text value from our name path and be done
            else:
                relevant_name_paths = pull_children_paths(first=nested_class_name, name_paths=nested_class_paths, divider=divider)
                text_answer = remove_first_from_paths(name_paths=relevant_name_paths, divider=divider)[0]
                nested_class_ndjson["answer"] = text_answer
            return_ndjson["classifications"].append(nested_class_ndjson)
    return return_ndjson
    
def remove_first_from_paths(name_paths:list, divider:str="///"):
    """ Removes the first name from every name path in a list a divider-delimited name paths
    Args:
        name_paths          :   Required (list) - List of name paths
        divider             :   Optional (str) - String delimiter for all name keys generated for parent/child schemas     
    Returns:
        List of name paths with the first name from said path removed
    """
    updated_name_paths = []
    for name_path in name_paths:
        names = name_path.split(divider)[1:]
        updated_name_path = ""
        for name in names:
            updated_name_path += name+divider
        updated_name_path = updated_name_path[:-len(divider)]  
        updated_name_paths.append(updated_name_path)
    return updated_name_paths

def pull_first_from_paths(name_paths:list, divider:str="///"):
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

def pull_children_paths(first, name_paths, divider:str="///"):
    """ From a list of name paths, returns only the name paths that are children of the first name provided
    Args
        first               :   Required (str) - The parent feature name you want to find paths for
        name_paths          :   Required (list) - List of name paths
        divider             :   Optional (str) - String delimiter for all name keys generated for parent/child schemas             
    Returns
        List of name paths that have first name provided
    """
    relevant_paths = []
    for path in name_paths:
        if path.startswith(first):
            relevant_paths.append(path)
    return relevant_paths
