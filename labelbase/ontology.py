from labelbox import Ontology as labelboxOntology

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
                dict_key = node['featureSchemaId'] if not invert else name_path
                if detailed:
                    if not invert:
                        dict_value = {"name":node_name,"type":node_type,"kind":node_kind,"encoded_value":encoded_value,"name_path":name_path}
                    else:
                        dict_value = {"name":node_name,"type":node_type,"kind":node_kind,"encoded_value":encoded_value,"schema_id":node['featureSchemaId']}
                else:
                    dict_value = name_path if not invert else node['featureSchemaId']
                feature_dict.update({dict_key : dict_value})
                if next_layer:
                    feature_dict, encoded_value = map_layer(feature_dict, next_layer, name_path, divider, invert=invert, detailed=detailed, encoded_value=encoded_value)
        return feature_dict, encoded_value
    if type(ontology) == labelboxOntology:
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
