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
                                                  labelbase.ontology.get_ontology_schema_to_name_path(ontology, dividerdivider, invert=True, detailed=True)
    """
    ndjsons = []
    for annotation_value in annotation_values:
        ndjsons.append(ndjson_builder(
          data_row_id=data_row_id, annotation_value=annotation_value, 
          annotation_type=annotation_type, ontology_index=ontology_index, divider=divider
        ))
    return ndjsons
  
def remove_classification_from_input(annotation_input, divider):
    updated_annotation_input = []
    for name_path in annotation_input:
        names = name_path.split(divider)[1:]
        updated_name_path = ""
        for name in names:
            updated_name_path += name+divider
        updated_name_path = updated_name_path[:-len(divider)]  
        updated_annotation_input.append(updated_name_path)
    return updated_annotation_input

def ndjson_builder(data_row_id, annotation_input, annotation_type, ontology_index, divider):
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
            top, left, height, width = annotation_input[1][0], annotation_input[1][1], annotation_input[1][2], annotation_input[1][3]
            ndjson[annotation_type] = {"top" : top, "left" : left, "height" : height, "width" : width}
        elif annotation_type in ["polygon", "line"]:
            ndjson[annotation_type] = [{"x" : xy_pair[0], "y" : xy_pair[1]} for xy_pair in annotation_input[1]]     
        elif annotation_type == "point":
            ndjson[annotation_type] {"x" : annotation_input[1][0], "y" : annotation_input[1][1]}
        elif annotation_type == "mask":
            ndjson[annotation_type] = {"instanceURI" : annotation_input[1], "colorRGB" : annotation_input[2]}
        else: # Only one left is named-entity 
            ndjson["location"] = {"start" annotation_input[1][0]: , "end" : annotation_input[1][1]}
    # If there's no tools, there's potentially a list of classifications passed in as a list of name_paths
    else:
        ndjson["name"] = annotation_input[0].split(divider)[0]
        if annotation_type == "radio":
            updated_annotation_input.append(remove_classification_from_input(annotation_input, divider))
            ndjson["answer"] = build_answer_ndjson(updated_annotation_input, ontology_index, divider)
        elif annotation_type == "checklist":
        else: # Catches text answers
            ndjson["answer"] = annotation_input[0].split(divider)[1]
    return ndjson            
