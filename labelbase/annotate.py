import uuid
import json
from labelbase.masks import mask_to_bytes

from labelbox import Client as labelboxClient
import labelbox as lb

def create_ndjsons(top_level_name:str, annotation_inputs:list, ontology_index:dict, confidence:bool=False, mask_method:str="url", divider:str="///"):
    """ From an annotation in the expected format, creates a Labelbox NDJSON of that annotation -- note the data row ID is not added here
        Each accepted annotation type and the expected input annotation value is listed below:
        ** IF confidence == False **
            For tools:
                bbox            :   [[top, left, height, width], [nested_classification_name_paths]], [[top, left, height, width], [nested_classification_name_paths]]
                polygon         :   [[(x, y), (x, y),...(x, y)], [nested_classification_name_paths]], [[(x, y), (x, y),...(x, y)], [nested_classification_name_paths]]
                line            :   [[(x, y), (x, y),...(x, y)], [nested_classification_name_paths]], [(x, y), (x, y),...(x, y)], [nested_classification_name_paths]]
                point           :   [(x, y), [nested_classification_name_paths]], [(x, y), [nested_classification_name_paths]]
                mask            :   [[URL, colorRGB], [nested_classification_name_paths]], [[URL, colorRGB], [nested_classification_name_paths]]
                                            OR
                                    [[array, colorRGB], [nested_classification_name_paths]], [[array, colorRGB], [nested_classification_name_paths]]
                                            OR
                                    [[png_bytes, None], [nested_classification_name_paths]], [[png_bytes, None], [nested_classification_name_paths]]                                    
                named-entity    :   [[(start, end), [nested_classification_name_paths]], [(start, end)], [nested_classification_name_paths]]
            For classifications:
                radio           :   [[answer_name_paths]]
                check           :   [[answer_name_paths]]
                text            :   [[answer_name_paths]] -- the last string in a text name path is the text value itself
        ** IF confidence == True **                
            For tools:
                bbox            :   [[top, left, height, width], [nested_classification_name_paths], confidence_score], [[top, left, height, width], [nested_classification_name_paths], confidence_score]
                polygon         :   [[(x, y), (x, y),...(x, y)], [nested_classification_name_paths], confidence_score], [[(x, y), (x, y),...(x, y)], [nested_classification_name_paths], confidence_score]
                line            :   [[(x, y), (x, y),...(x, y)], [nested_classification_name_paths], confidence_score], [(x, y), (x, y),...(x, y)], [nested_classification_name_paths], confidence_score]
                point           :   [(x, y), [nested_classification_name_paths], confidence_score], [(x, y), [nested_classification_name_paths], confidence_score]
                mask            :   [[URL, colorRGB], [nested_classification_name_paths], confidence_score], [[URL, colorRGB], [nested_classification_name_paths], confidence_score]
                                            OR
                                    [[array, colorRGB], [nested_classification_name_paths], confidence_score], [[array, colorRGB], [nested_classification_name_paths], confidence_score]
                                            OR
                                    [[png_bytes, None], [nested_classification_name_paths], confidence_score], [[png_bytes, None], [nested_classification_name_paths], confidence_score]                                    
                named-entity    :   [[(start, end), [nested_classification_name_paths], confidence_score], [(start, end)], [nested_classification_name_paths], confidence_score]
            For classifications:
                radio           :   [[answer_name_paths], confidence_score]
                check           :   [[answer_name_paths], confidence_score]
                text            :   [[answer_name_paths], confidence_score] -- the last string in a text name path is the text value itself                
    Args:
        data_row_id             :   Required (str) - Labelbox Data Row ID
        top_level_name          :   Required (str) - Name of the top-level tool or classification        
        annotation_inputs       :   Required (list) - List of annotation value lists where the list of lists corresponds to the above format:        
        annotation_type         :   Required (str) - Type of annotation in question - must be a string of one of the above options
        ontology_index          :   Required (dict) - Dictionary created from running:
                                            labelbase.ontology.get_ontology_schema_to_name_path(ontology, divider=divider, invert=True, detailed=True)
        confidence              :   Optional (bool) - If True, will expect a different format and add confidence scores to each ndjson created
        mask_method             :   Optional (str) - Specifies your input mask data format
                                        - "url" means your mask is an accessible URL (must provide color)
                                        - "array" means your mask is a numpy array (must provide color)
                                        - "png" means your mask value is a png-string                                       
        divider                 :   Optional (str) - String delimiter for name paths        
    """
    if mask_method not in ["url", "png", "array"]:
        raise ValueError(f"Mask method must be either `url`, `png` or `array`")
    ndjsons = []
    if (type(annotation_inputs) == str) and (annotation_inputs!=""):
        annotation_inputs = json.loads(annotation_inputs.replace("'",'"').replace("None","null"))
    if type(annotation_inputs) == list:
        for annotation_input in annotation_inputs:
            ndjsons.append(ndjson_builder(
                top_level_name=top_level_name,
                annotation_input=annotation_input, 
                ontology_index=ontology_index, 
                confidence=confidence,
                mask_method=mask_method,
                divider=divider
            ))
    return ndjsons

def ndjson_builder(top_level_name:str, annotation_input:list, ontology_index:dict, confidence:bool=False, mask_method:str="url", divider:str="///"):
    """ Returns an ndjson of an annotation given a list of values - the values needed differ depending on the annotation type
    Args:
        top_level_name          :   Required (str) - Name of the top-level tool or classification        
        annotation_input        :   Required (list) - List that corresponds to a single annotation for a label in the specified format
        ontology_index          :   Required (dict) - Dictionary created from running:
                                            labelbase.ontology.get_ontology_schema_to_name_path(ontology, divider=divider, invert=True, detailed=True)
        confidence              :   Optional (bool) - If True, will expect a different format and add confidence scores to each ndjson created                                            
        mask_method             :   Optional (str) - Specifies your input mask data format
                                        - "url" treats annotation input values as URLs uploads them directly
                                        - "array" converts the annotation input values into png bytes
                                        - "png" uploads png bytes directly
        divider                 :   Optional (str) - String delimiter for name paths        
    Returns
        NDJSON representation of an annotation
    """
    annotation_type = ontology_index[top_level_name]["type"]
    if ontology_index['project_type'] == str(lb.MediaType.Geospatial_Tile):
        annotation_type = 'geo_' + annotation_type

    ndjson = {
        "uuid" : str(uuid.uuid4())
    }  
    # Catches tools
    if annotation_type in ["bbox", "polygon", "line", "point", "mask", "named-entity", "geo_bbox", "geo_polygon", "geo_line", "geo_point"]:
        if confidence:
            ndjson["confidence"] = annotation_input[2] if len(annotation_input) == 3 else 0.0
        ndjson["name"] = top_level_name
        if annotation_type == "geo_bbox":
                ndjson['bbox'] = {
                    'top': annotation_input[0][0][1][1],
                    'left': annotation_input[0][0][1][0],
                    'height': annotation_input[0][0][3][1] - annotation_input[0][0][1][1],        
                    'width': annotation_input[0][0][3][0] - annotation_input[0][0][1][0]
                }
        elif annotation_type == "geo_polygon":
            polygon_points_ndjson = []
            for sub in annotation_input[0][0]:
                polygon_points_ndjson.append({"x":sub[0], "y":sub[1]})
            ndjson["polygon"] = polygon_points_ndjson
        elif annotation_type == "geo_line":
            line_points_ndjson = []
            for sub in annotation_input[0]:
                line_points_ndjson.append({"x":sub[0], "y":sub[1]})
            ndjson["line"] = line_points_ndjson
        elif annotation_type == "geo_point":
            ndjson["point"] = {'x':annotation_input[0][0], 'y':annotation_input[0][1]}
        elif annotation_type == "bbox":
            ndjson[annotation_type] = {"top":annotation_input[0][0],"left":annotation_input[0][1],"height":annotation_input[0][2],"width":annotation_input[0][3]}
        elif annotation_type in ["polygon", "line"]:
            ndjson[annotation_type] = [{"x":xy_pair[0],"y":xy_pair[1]} for xy_pair in annotation_input[0]]     
        elif annotation_type == "point":
            ndjson[annotation_type] = {"x":annotation_input[0][0],"y":annotation_input[0][1]}
        elif annotation_type == "mask":
            if mask_method == "url": 
                ndjson[annotation_type] = {"instanceURI":annotation_input[0][0],"colorRGB":annotation_input[0][1]}
            elif mask_method == "array": # input masks as numpy arrays
                png = mask_to_bytes(input=annotation_input[0][0], method=mask_method, color=annotation_input[0][1], output="png")
                ndjson[annotation_type] = {"png":png}
            else: # Only one left is png
                ndjson[annotation_type] = {"png":annotation_input[0][0]}
        else: # Only one left is named-entity 
            ndjson['data']["location"] = {"start" : annotation_input[0][0],"end":annotation_input[0][1]}
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
                answer_paths=[annotation_input[0]],
                ontology_index=ontology_index,
                divider=divider
            )
        )
        if confidence:
            ndjson["confidence"] = annotation_input[1] if len(annotation_input) == 2 else 0.0        
    return ndjson   

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

def classification_builder(classification_path:str, answer_paths:list, ontology_index:dict, tool_name:str="", divider:str="///"):
    """ Given a classification path and all its child paths, constructs an ndjson.
        If the classification answer's paths have nested classifications, will recursuively call this function.
    """
    # Determine full classification path, including tool name
    index_input = f"{tool_name}{divider}{classification_path}" if tool_name else classification_path
    # Determine the classification type from full classification path
    c_type = ontology_index[index_input]["type"] 
    # Get the current classification name at the end of full classification path
    c_name = classification_path.split(divider)[-1] if divider in classification_path else classification_path 
    # Initiate your classification ndjson
    classification_ndjson = {
        "name" : c_name
    }
    # If this is a radio, there's only one answer
    if c_type == "radio":
        # For the current classification, get the first answer path where the current classification is the parent feature
        answer_name = pull_first_name_from_paths(name_paths=answer_paths, divider=divider)[0]
        classification_ndjson["answer"] = {
            "name" : answer_name
        }
        # For the current answer, get all nested classification paths where the current answer is the parent feature 
        n_c_paths = get_child_paths(first=answer_name, name_paths=answer_paths, divider=divider)
        # Get the current nested classification names
        n_c_names = pull_first_name_from_paths(name_paths=n_c_paths, divider=divider)
        # For each nested classification path, loop this process, finding answers and nested classifications
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
    # If this is a checklist, there are potentially multiple answers
    elif c_type == "checklist":
        classification_ndjson["answers"] = []
        # For the current classification, get all answer paths where the current classification is the parent feature        
        answer_names = pull_first_name_from_paths(name_paths=answer_paths, divider=divider)
        # For each current answer....
        for answer_name in answer_names:
            answer_ndjson = {
                "name" : answer_name
            }
            # For the current answer, get all nested classification paths where the current answer is the parent feature                
            n_c_paths = get_child_paths(first=answer_name, name_paths=answer_paths, divider=divider)
            # Get the current nested classification names            
            n_c_names = pull_first_name_from_paths(name_paths=n_c_paths, divider=divider)
            # For each nested classification path, loop this process, finding answers and nested classifications                             
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
    # If this is text, there the answer is whatever is at the end of the current answer path
    else:
        classification_ndjson["answer"] = answer_paths[0]
    return classification_ndjson  

def get_leaf_paths(classifications, current_path="", divider="///"):
    """ Given a flat list of labelox export classifications, constructs leaf name paths given a divider
    Args:
        classifications         :   Required (list) - List of classifications from exported label
        current_path            :   Optional (str) - Used to recursively build name paths for nested classifications
        divider                 :   Optional (str) - String delimiter for name paths
    Returns:
        List of all leaf name paths 
    """
    name_paths = []
    for classification in classifications:
        if current_path == "":
            name_path = classification['name']
        else:
            name_path = f"{current_path}{divider}{classification['name']}"
        if "text_answer" in classification.keys():
            name_path = f"{name_path}{divider}{classification['text_answer']['content']}"
            name_paths.append(name_path)
        if "checklist_answers" in classification.keys():
            for answer in classification['checklist_answers']:
                new_path = f"{name_path}{divider}{answer['name']}"
                if "classifications" in answer.keys():
                    if len(answer['classifications']) > 0:
                        name_paths += get_leaf_paths(answer['classifications'], current_path=new_path, divider=divider)
                    else:
                        name_paths.append(new_path)
                else:
                    name_paths.append(new_path)
        if "radio_answer" in classification.keys():
            answer = classification['radio_answer']
            new_path = f"{name_path}{divider}{answer['name']}"
            if "classifications" in answer.keys():
                if len(answer['classifications']) > 0:
                    name_paths += get_leaf_paths(answer['classifications'], current_path=new_path, divider=divider)
                else:
                    name_paths.append(new_path)
            else:
                name_paths.append(new_path)
    return name_paths
                    

def flatten_label(client:labelboxClient, label_dict:dict, ontology_index:dict, datarow_id:str="", mask_method:str="url", divider:str="///"):
    """ For a label from project.export_v2(), creates a flat dictionary where:
            { key = annotation_type + divider + annotation_name  :  value = [annotation_value, list_of_nested_name_paths]}
        Each accepted annotation type and the expected output annotation value is listed below:
            For tools:
                bbox            :   [[top, left, height, width], [nested_classification_name_paths], [top, left, height, width], [nested_classification_name_paths]]
                polygon         :   [[(x, y), (x, y),...(x, y)], [nested_classification_name_paths], [(x, y), (x, y),...(x, y)], [nested_classification_name_paths]]
                line            :   [[(x, y), (x, y),...(x, y)], [nested_classification_name_paths], [(x, y), (x, y),...(x, y)], [nested_classification_name_paths]]
                point           :   [[x, y], [nested_classification_name_paths], [x, y], [nested_classification_name_paths]]
                mask            :   [[URL, colorRGB], [nested_classification_name_paths], [URL, colorRGB], [nested_classification_name_paths]]
                                            OR
                                    [[array, colorRGB], [nested_classification_name_paths], [array, colorRGB], [nested_classification_name_paths]]
                                            OR
                                    [[png_bytes, "null"], [nested_classification_name_paths], [png_bytes, "null"], [nested_classification_name_paths]]                                    
                named-entity    :   [[start, end], [nested_classification_name_paths], [start, end], [nested_classification_name_paths]]
            For classifications:
                radio           :   [[answer_name_paths]]
                check           :   [[answer_name_paths]]
                text            :   [[answer_name_paths]] -- the last string in a text name path is the text value itself
    Args:    
        client                  :   Required        - Labelbox client
        label_dict              :   Required (dict) - Dictionary representation of a label from project.export_labels(download=True)
        ontology_index          :   Required (dict) - Dictionary created from running:
                                            labelbase.ontology.get_ontology_schema_to_name_path(ontology, divider=divider, invert=True, detailed=True)
        datarow_id              :   Required (str) - Datarow id, only required for datarows with mask annotations
        mask_method             :   Optional (str) - Specifies your desired mask data format
                                        - "url" leaves masks as-is
                                        - "array" converts URLs to numpy arrays
                                        - "png" converts URLs to png byte strings                                               
        divider                 :   Optional (str) - String delimiter for name paths        
    Returns:        
        Dictionary with one key per annotation class in a given label in the specified format written above
    """
    flat_label = {}
    annotations = label_dict['annotations']
    objects = annotations["objects"]
    classifications = annotations["classifications"]
    if objects:
        for obj in objects:
            annotation_type = ontology_index[obj["name"]]["type"]
            annotation_type = "mask" if annotation_type == "raster-segmentation" else annotation_type
            annotation_type = "bbox" if annotation_type == "rectangle" else annotation_type
            if 'geojson' in obj.keys():
                annotation_type = 'geo_' + annotation_type
            column_name = f'{annotation_type}{divider}{obj["name"]}'           
            if column_name not in flat_label.keys():
                flat_label[column_name] = []
            if "bounding_box" in obj.keys():
                annotation_value = [obj["bounding_box"]["top"], obj["bounding_box"]["left"], obj["bounding_box"]["height"], obj["bounding_box"]["width"]]
            elif "polygon" in obj.keys():
                annotation_value = [[coord["x"], coord["y"]] for coord in obj["polygon"]]
            elif "line" in obj.keys():
                annotation_value = [[coord["x"], coord["y"]] for coord in obj["line"]]
            elif "point" in obj.keys():
                annotation_value = [obj["point"]["x"], obj["point"]["y"]]
            elif "data" in obj.keys():
                annotation_value = [obj['data']["location"]["start"], obj['data']["location"]["end"]]
            elif "geojson" in obj.keys():
                annotation_value = obj['geojson']['coordinates']
            else:
                if mask_method == "url":
                    annotation_value = [obj['mask']["url"], [255,255,255]]
                elif mask_method == "array": 
                    array = mask_to_bytes(client=client, input=obj['mask']["url"], datarow_id=datarow_id, method="url", color=[255,255,255], output="array")
                    annotation_value = [array, [255,255,255]]
                else:
                    png = mask_to_bytes(client=client, input=obj['mask']["url"], datarow_id=datarow_id, method="url", color=[255,255,255], output="png")
                    annotation_value = [png, "null"]
            if "classifications" in obj.keys():
                if len(obj['classifications']) > 0:
                    return_paths = get_leaf_paths(
                        classifications=obj["classifications"], 
                        divider=divider
                    )
                    print(return_paths)
                else:
                    return_paths = []
            else:
                return_paths = []
            flat_label[column_name].append([annotation_value, return_paths])
    if classifications:
        leaf_paths = get_leaf_paths(
            classifications=classifications, 
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
