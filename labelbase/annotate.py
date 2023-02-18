import uuid

def get_leaf_paths(export_classifications:list, schema_to_name_path:dict, divider:str="///"):
    """ Given a flat list of labelox export classifications, constructs leaf name paths given a divider
    Args:
        export_classifications  :   Required (list) - List of classifications from label["Label"]["objects"][0]["classifications"] or label["Label"]["classificaitons"]
        schema_to_name_path     :   Required (dict) - Dictionary where {key=schema_id : value=feature_name_path} created from running:
                                            labelbase.get_ontology_schema_to_name_path(ontology, divider=divider, invert=False, detailed=False)
        divider                 :   Optional (str): String delimiter for name paths
    Returns:
        List of all leaf name paths 
    """
    def build_leaf_paths(root:dict, acc="", name_paths=[], divider="///"):
        for parent in root.keys():
            name_path = f"{acc}{divider}{parent}" if acc else f"{parent}"
            child = root[parent]
            if child:
                name_paths = build_leaf_paths(root=root[parent], acc=name_path, name_paths=name_paths)
            else:
                name_paths.append(name_path)
        return name_paths    
    name_paths = []
    for cla in export_classifications:
        if type(cla) == dict:
            if "answers" in cla.keys():
                for answer in cla["answers"]:
                    name_paths.append(schema_to_name_path[answer["schemaId"]])
            if "answer" in cla.keys():
                if type(cla["answer"]) == str:
                    name_paths.append(schema_to_name_path[cla["schemaId"]]+divider+cla["answer"])
                else:
                    name_paths.append(schema_to_name_path[cla["answer"]["schemaId"]]) 
        else:
            for c in cla:
                if "answers" in c.keys():
                    for answer in c["answers"]:
                        name_paths.append(schema_to_name_path[answer["schemaId"]])
                if "answer" in c.keys():
                    if type(c["answer"]) == str:
                        name_paths.append(schema_to_name_path[c["schemaId"]]+divider+c["answer"])
                    else:
                        name_paths.append(schema_to_name_path[c["answer"]["schemaId"]])                 
    root = {}
    for input_path in name_paths:
        parts = input_path.split(divider)
        current_node = root
        for part in parts:
            if part not in current_node:
                current_node[part] = {}
            current_node = current_node[part]    
    return build_leaf_paths(root)  

def flatten_label(label_dict:dict, ontology_index:dict, schema_to_name_path:dict, divider:str="///"):
    """ For a label from project.export_labels(download=True), creates a flat dictionary where:
            { key = annotation_type + divider + annotation_name  :  value = [annotation_value, list_of_nested_name_paths]}
        Each accepted annotation type and the expected output annotation value is listed below:
            For tools:
                bbox            :   [[top, left, height, width], [nested_classification_name_paths], [top, left, height, width], [nested_classification_name_paths]]
                polygon         :   [[(x, y), (x, y),...(x, y)], [nested_classification_name_paths], [(x, y), (x, y),...(x, y)], [nested_classification_name_paths]]
                line            :   [[(x, y), (x, y),...(x, y)], [nested_classification_name_paths], [(x, y), (x, y),...(x, y)], [nested_classification_name_paths]]
                point           :   [[x, y], [nested_classification_name_paths], [x, y], [nested_classification_name_paths]]
                mask            :   [[URL, colorRGB], [nested_classification_name_paths], [URL, colorRGB], [nested_classification_name_paths]]
                named-entity    :   [[start, end], [nested_classification_name_paths], [start, end], [nested_classification_name_paths]]
            For classifications:
                radio           :   [[answer_name_paths]]
                check           :   [[answer_name_paths]]
                text            :   [[answer_name_paths]] -- the last string in a text name path is the text value itself
    Args:    
        label_dict              :   Required (dict) - Dictionary representation of a label from project.export_labels(download=True)
        ontology_index          :   Required (dict) - Dictionary created from running:
                                            labelbase.ontology.get_ontology_schema_to_name_path(ontology, divider=divider, invert=True, detailed=True)
        schema_to_name_path     :   Required (dict) - Dictionary where {key=schema_id : value=feature_name_path} created from running:
                                            labelbase.ontology.get_ontology_schema_to_name_path(ontology, divider=divider, invert=False, detailed=False)
        divider                 :   Optional (str) - String delimiter for name paths        
    Returns:        
        Dictionary with one key per annotation class in a given label in the specified format written above
    """
    flat_label = {}
    annotations = label_dict["Label"]
    objects = annotations["objects"]
    classifications = annotations["classifications"]
    if objects:
        for obj in objects:
            annotation_type = ontology_index[obj["title"]]["type"]
            annotation_type = "mask" if annotation_type == "raster-segmentation" else annotation_type
            annotation_type = "bbox" if annotation_type == "rectangle" else annotation_type
            column_name = f'{annotation_type}{divider}{obj["title"]}'           
            if column_name not in flat_label.keys():
                flat_label[column_name] = []
            if "bbox" in obj.keys():
                annotation_value = [obj["bbox"]["top"], obj["bbox"]["left"], obj["bbox"]["height"], obj["bbox"]["width"]]
            elif "polygon" in obj.keys():
                annotation_value = [[coord["x"], coord["y"]] for coord in obj["polygon"]]
            elif "line" in obj.keys():
                annotation_value = [[coord["x"], coord["y"]] for coord in obj["line"]]
            elif "point" in obj.keys():
                annotation_value = [obj["point"]["x"], obj["point"]["y"]]
            elif "location" in obj.keys():
                annotation_value = [obj["location"]["start"], obj["location"]["end"]]
            else:
                annotation_value = [obj["instanceURI"], [0,0,0]]
            if "classifications" in obj.keys():
                nested_classification_name_paths = get_leaf_paths(
                    export_classifications=obj["classifications"], 
                    schema_to_name_path=schema_to_name_path,
                    divider=divider
                )
                return_paths = get_child_paths(first=obj["title"], name_paths=nested_classification_name_paths, divider=divider)
            else:
                return_paths = []
            flat_label[column_name].append([annotation_value, return_paths])
    if classifications:
        leaf_paths = get_leaf_paths(
            export_classifications=classifications, 
            schema_to_name_path=schema_to_name_path,
            divider=divider
        )
        classification_names = pull_first_name_from_paths(
            name_paths=leaf_paths, 
            divider=divider
        )
        for classification_name in classification_names:
            annotation_type = ontology_index[classification_name]["type"]
            child_paths = get_child_paths(first=classification_name, name_paths=leaf_paths, divider=divider)
            flat_label[f'{annotation_type}{divider}{classification_name}'] = [[name_path for name_path in child_paths]]
    return flat_label

def create_ndjsons(top_level_name:str, annotation_inputs:list, ontology_index:dict, divider:str="///"):
    """ From an annotation in the expected format, creates a Labelbox NDJSON of that annotation -- note the data row ID is not added here
        Each accepted annotation type and the expected input annotation value is listed below:
            For tools:
                bbox            :   [[top, left, height, width], [nested_classification_name_paths], [top, left, height, width], [nested_classification_name_paths]]
                polygon         :   [[(x, y), (x, y),...(x, y)], [nested_classification_name_paths], [(x, y), (x, y),...(x, y)], [nested_classification_name_paths]]
                line            :   [[(x, y), (x, y),...(x, y)], [nested_classification_name_paths], [(x, y), (x, y),...(x, y)], [nested_classification_name_paths]]
                point           :   [[x, y], [nested_classification_name_paths], [x, y], [nested_classification_name_paths]]
                mask            :   [[URL, colorRGB], [nested_classification_name_paths], [URL, colorRGB], [nested_classification_name_paths]]
                named-entity    :   [[start, end], [nested_classification_name_paths], [start, end], [nested_classification_name_paths]]
            For classifications:
                radio           :   [[answer_name_paths]]
                check           :   [[answer_name_paths]]
                text            :   [[answer_name_paths]] -- the last string in a text name path is the text value itself
    Args:
        data_row_id             :   Required (str) - Labelbox Data Row ID
        top_level_name          :   Required (str) - Name of the top-level tool or classification        
        annotation_inputs       :   Required (list) - List of annotation value lists where 1 list element must correspond to the following format:        
        annotation_type         :   Required (str) - Type of annotation in question - must be a string of one of the above options
        ontology_index          :   Required (dict) - Dictionary created from running:
                                            labelbase.ontology.get_ontology_schema_to_name_path(ontology, divider=divider, invert=True, detailed=True)
        divider                 :   Optional (str) - String delimiter for name paths        
    """
    ndjsons = []
    if type(annotation_inputs) == list:
        for annotation_input in annotation_inputs:
            ndjsons.append(ndjson_builder(
                top_level_name=top_level_name,
                annotation_input=annotation_input, 
                ontology_index=ontology_index, 
                divider=divider
            ))
    return ndjsons

def ndjson_builder(top_level_name:str, annotation_input:list, ontology_index:dict, divider:str="///"):
    """ Returns an ndjson of an annotation given a list of values - the values needed differ depending on the annotation type
    Args:
        top_level_name          :   Required (str) - Name of the top-level tool or classification        
        annotation_input        :   Required (list) - List that corresponds to a single annotation for a label in the specified format
        ontology_index          :   Required (dict) - Dictionary created from running:
                                            labelbase.ontology.get_ontology_schema_to_name_path(ontology, divider=divider, invert=True, detailed=True)
        divider                 :   Optional (str) - String delimiter for name paths        
    Returns
        NDJSON representation of an annotation
    """
    annotation_type = ontology_index[top_level_name]["type"]
    ndjson = {
        "uuid" : str(uuid.uuid4())
    }  
    # Catches tools
    if annotation_type in ["bbox", "polygon", "line", "point", "mask", "named-entity"]:
        ndjson["name"] = top_level_name
        if annotation_type == "bbox":
            ndjson[annotation_type] = {"top":annotation_input[0][0],"left":annotation_input[0][1],"height":annotation_input[0][2],"width":annotation_input[0][3]}
        elif annotation_type in ["polygon", "line"]:
            ndjson[annotation_type] = [{"x":xy_pair[0],"y":xy_pair[1]} for xy_pair in annotation_input[0]]     
        elif annotation_type == "point":
            ndjson[annotation_type] = {"x":annotation_input[0][0],"y":annotation_input[0][1]}
        elif annotation_type == "mask":
            ndjson[annotation_type] = {"instanceURI":annotation_input[0][0],"colorRGB":annotation_input[0][1]}
        else: # Only one left is named-entity 
            ndjson["location"] = {"start" : annotation_input[0][0],"end":annotation_input[0][1]}
        if annotation_input[1]:
            ndjson["classifications"] = []
            classification_names = pull_first_name_from_paths(name_paths=annotation_input[1], divider=divider)
            for classification_name in classification_names:
                ndjson["classifications"].append(
                    classification_builder(
                        classification_path=classification_name,
                        answer_paths=get_child_paths(first=classification_name, name_paths=annotation_input[1], divider=divider),
                        ontology_index=ontology_index,
                        tool_name=top_level_name,
                        divider=divider
                    )
                )
    # Otherwise, the top level feature is a classification
    else:
        ndjson.update(
            classification_builder(
                classification_path=top_level_name, 
                answer_paths=annotation_input,
                ontology_index=ontology_index,
                divider=divider
            )
        )
    return ndjson    

def classification_builder(classification_path:str, answer_paths:list, ontology_index:dict, tool_name:str="", divider:str="///"):
    """ Given a classification path and all its child paths, constructs an ndjson.
        If the classification answer's paths have nested classifications, will recursuively call this function.
    """
    index_input = f"{tool_name}{divider}{classification_path}" if tool_name else classification_path
    c_type = ontology_index[index_input]["type"]
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
                n_a_paths = get_child_paths(first=n_c_name, name_paths=n_c_paths, divider=divider)  
                classification_ndjson["answer"]["classifications"].append(
                    classification_builder(
                        classification_path=f"{classification_path}{divider}{answer_name}{divider}{n_c_name}",
                        answer_paths=n_a_paths,
                        ontology_index=ontology_index,
                        tool_name=tool_name,
                        divider=divider
                    )
                )
    elif c_type == "checklist":
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
                    n_a_paths = get_child_paths(first=n_c_name, name_paths=n_c_paths, divider=divider) 
                    answer_ndjson["classifications"].append(
                        classification_builder(
                            classification_path=f"{classification_path}{divider}{answer_name}{divider}{n_c_name}",
                            answer_paths=n_a_paths,
                            ontology_index=ontology_index,
                            tool_name=tool_name,
                            divider=divider                            
                        )
                    )
            classification_ndjson["answers"].append(answer_ndjson)
    else:
        classification_ndjson["answer"] = answer_paths[0]
    return classification_ndjson

def pull_first_name_from_paths(name_paths:list, divider:str="///"):
    """ Pulls the first name from every name path in a list a divider-delimited name paths
    Args:    
        name_paths              :   Required (list) - List of name paths
        divider                 :   Optional (str) - String delimiter for all name keys generated for parent/child schemas     
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
        first                   :   Required (str) - The parent feature name you want to find paths for
        name_paths              :   Required (list) - List of name paths
        divider                 :   Optional (str) - String delimiter for all name keys generated for parent/child schemas             
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
