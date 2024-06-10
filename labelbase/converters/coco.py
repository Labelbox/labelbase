from labelbox import Project as labelboxProject, StreamType, Client as labelboxClient
from labelbase.ontology import get_ontology_schema_to_name_path
import copy
import datetime
from google.api_core import retry
import requests
from PIL import Image 
import numpy as np
from io import BytesIO
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from shapely.geometry import Polygon
import cv2

def _to_coco_bbox_converter(data_row_id:str, annotation:dict, category_id:str):
    """ Given a label dictionary and a bounding box annotation from said label, will return the coco-converted bounding box annotation dictionary
    Args:
        data_row_id (str)               :     Labelbox Data Row ID for this label
        annotation (dict)               :     Annotation dictionary from label['Label']['objects'], which comes from project.export_labels()
        category_id (str)               :     Desired category_id for the coco_annotation
    Returns:
        An annotation dictionary in the COCO format
    """
    coco_annotation = {
        "image_id": data_row_id,
        "bbox": [annotation['bounding_box']['left'], annotation['bounding_box']['top'], annotation['bounding_box']['width'], annotation['bounding_box']['height']],
        "category_id": category_id,
        "id": annotation['feature_id']
    }
    return coco_annotation

def _to_coco_line_converter(data_row_id:str, annotation:dict, category_id:str):
    """ Given a label dictionary and a line annotation from said label, will return the coco-converted line annotation dictionary
    Args:
        data_row_id (str)               :     Labelbox Data Row ID for this label
        annotation (dict)               :     Annotation dictionary from label['Label']['objects'], which comes from project.export_labels()
        category_id (str)               :     Desired category_id for the coco_annotation
    Returns:
        An annotation dictionary in the COCO format
    """
    line = annotation['line']
    coco_line = []
    num_line_keypoints = 0
    for coordinates in line:
        coco_line.append(coordinates['x'])
        coco_line.append(coordinates['y'])
        coco_line.append(2)
        num_line_keypoints += 1
    coco_annotation = {
        "image_id": data_row_id,
        "keypoints": coco_line,
        "num_keypoints": num_line_keypoints,
        "category_id" : category_id,
        "id": annotation['feature_id']
    }
    return coco_annotation, num_line_keypoints

def _to_coco_point_converter(data_row_id:str, annotation:dict, category_id:str):
    """ Given a label dictionary and a point annotation from said label, will return the coco-converted point annotation dictionary
    Args:
        data_row_id (str)               :     Labelbox Data Row ID for this label
        annotation (dict)               :     Annotation dictionary from label['Label']['objects'], which comes from project.export_labels()
        category_id (str)               :     Desired category_id for the coco_annotation
    Returns:
        An annotation dictionary in the COCO format
    """  
    coco_annotation = {
        "image_id": data_row_id,
        "keypoints": [annotation['point']['x'], annotation['point']['y'], 2],
        "num_keypoints": 1,
        "category_id" : category_id,
        "id": annotation['feature_id']
    }
    return coco_annotation  

def _to_coco_polygon_converter(data_row_id:str, annotation:dict, category_id:str):
    """Given a label dictionary and a point annotation from said label, will return the coco-converted polygon annotation dictionary
    Args:
        data_row_id (str)               :     Labelbox Data Row ID for this label
        annotation (dict)               :     Annotation dictionary from label['Label']['objects'], which comes from project.export_labels()
        category_id (str)               :     Desired category_id for the coco_annotation
    Returns:
        An annotation dictionary in the COCO format
    """  
    poly_points = [[coord['x'], coord['y']] for coord in annotation['polygon']]
    bounds = Polygon(poly_points).bounds
    coco_annotation = {
        "image_id" : data_row_id,
        "segmentation" : [[item for sublist in poly_points for item in sublist]],
        "bbox" : [bounds[0], bounds[1], bounds[2]-bounds[0], bounds[3]-bounds[1]],
        "area" : Polygon(poly_points).area,
        "id": annotation['feature_id'],
        "iscrowd" : 0,
        "category_id" : category_id
    }  
    return coco_annotation
  
@retry.Retry(predicate=retry.if_exception_type(Exception), deadline=120.)
def download_mask(url:str, labelboxClient: labelboxClient):
    """Incorporates retry logic into the download of a mask / polygon Instance URI
    Args:
        annotation (dict)       :     A dictionary pertaining to a label exported from Labelbox
    Returns:
        A numPy array of said mask
    """ 
    return requests.get(url, headers=labelboxClient.headers).content

def _to_coco_mask_converter(data_row_id:str, annotation:dict, category_id:str, client:labelboxClient):
    """Given a label dictionary and a mask annotation from said label, will return the coco-converted segmentation mask annotation dictionary
    Args:
        data_row_id (str)               :     Labelbox Data Row ID for this label
        annotation (dict)               :     Annotation dictionary from label['Label']['objects'], which comes from project.export_labels()
        category_id (str)               :     Desired category_id for the coco_annotation
        client (str)                    :     labelbox Client object
    Returns:
        An annotation dictionary in the COCO format
    """  
    mask_data = download_mask(annotation["mask"]["url"], client)
    binary_mask_arr = np.array(Image.open(BytesIO(mask_data)))
    if binary_mask_arr.ndim == 3:
        print("3DDDD")
        binary_mask_arr = binary_mask_arr[:,:,:1]
    contours = cv2.findContours(binary_mask_arr, cv2.RETR_LIST, cv2.CHAIN_APPROX_SIMPLE)[0]
    coords = contours[0]
    poly_points = np.array([[coords[i][0][0], coords[i][0][1]] for i in range(0, len(coords))])
    bounds = Polygon(poly_points).bounds
    coco_annotation = {
        "image_id" : data_row_id,
        "segmentation" : [[item for sublist in poly_points for item in sublist]],
        "bbox" : [bounds[0], bounds[1], bounds[2]-bounds[0], bounds[3]-bounds[1]],
        "area" : Polygon(poly_points).area,
        "id": annotation['feature_id'],
        "iscrowd" : 0,
        "category_id" : category_id
    }  
    return coco_annotation

def _to_coco_annotation_converter(data_row_id:str, annotation:dict, ontology_schema_to_name_path:dict, client:labelboxClient):
    """ Wrapper to triage and multithread the coco annotation conversion - if nested classes exist, the category_id will be the first radio/checklist classification answer available
    Args:
        data_row_id (str)                   :     Labelbox Data Row ID for this label
        annotation (dict)                   :     Annotation dictionary from label['Label']['objects'], which comes from project.export_labels()
        ontology_schema_to_name_path (dict) :     A dictionary where {key=featureSchemaId : value = {"encoded_value"} which corresponds to category_id
    Returns:
        A dictionary corresponding to te coco annotation syntax - the category ID used will be the top-level tool 
    """
    max_line_keypoints = 0
    category_id = ontology_schema_to_name_path[annotation['feature_schema_id']]['encoded_value']
    if "classifications" in annotation.keys():
        if annotation['classifications']:
            for classification in annotation['classifications']:
                if 'radio_answer' in classification.keys():
                    if type(classification['radio_answer']) == dict:
                        category_id = ontology_schema_to_name_path[classification['radio_answer']['feature_schema_id']]['encoded_value']
                        break
                elif 'checklist_answers' in classification.keys():
                    category_id = ontology_schema_to_name_path[classification['checklist_answers'][0]['feature_schema_id']]['encoded_value']
                    break
    if "bounding_box" in annotation.keys():
        coco_annotation = _to_coco_bbox_converter(data_row_id, annotation, category_id)
    elif "line" in annotation.keys():
        coco_annotation, max_line_keypoints = _to_coco_line_converter(data_row_id, annotation, category_id)
    elif "point" in annotation.keys():
        coco_annotation = _to_coco_point_converter(data_row_id, annotation, category_id)
    elif "polygon" in annotation.keys():
        coco_annotation = _to_coco_polygon_converter(data_row_id, annotation, category_id)
    else: 
        coco_annotation = _to_coco_mask_converter(data_row_id, annotation, category_id, client)     
    return coco_annotation, max_line_keypoints

def export_labels(project:labelboxProject, labelboxClient: labelboxClient, verbose:bool=True, divider:str="///"):
    """ Given a project and a list of labels, will create the COCO export json
    Args:
        project:   Required (labelbox.schema.project.Project) - Labelbox Project object
        labelboxClient:   Required (labelbox.Client) - Labelbox Client object
        verbose:   Optional (bool) - If True, prints information about code execution
        divider:   Optional (str) - String delineating the tool/classification/answer path for a given schema ID
    Returns:
        Dicationary with 'info', 'licenses', 'images', 'annotations', and 'annotations' keys corresponding to a COCO dataset format
    """
    if verbose:
        print(f'Exporting labels from project ID {project.uid}')
    exported_datarows_task = project.export_v2(params={"data_row_details": True})
    exported_datarows_task.wait_till_done()
    exported_datarows = exported_datarows_task.result
    if verbose:
        print(f'Export complete - {exported_datarows_task.get_total_lines(stream_type=StreamType.RESULT)} datarows to convert')
    info = {
        'description' : project.name, 'url' : f'https://app.labelbox.com/projects/{project.uid}/overview', 'version' : "1.0", 
        'year' : datetime.datetime.now().year, 'contributor' : project.created_by().email, 'date_created' : datetime.datetime.now().strftime('%Y/%m/%d'),
    }
    licenses = [{"url" : "N/A","id" : 1,"name" : "N/A"}]
    # Create a dictionary where {key=data_row_id : value=data_row}
    data_rows = {}
    if verbose:
        print(f'Exporting Data Rows from Project...')
    for item in exported_datarows:
        data_rows.update({
                item["data_row"]["id"]: {
                    "global_key": item["data_row"]["global_key"],
                    "height": item["media_attributes"]["height"],
                    "width": item["media_attributes"]["width"],
                    "created_at": item["data_row"]["details"]["created_at"],
                    "row_data": item["data_row"]["row_data"],
                }
            })
    if verbose:                        
        print(f'Export complete. {len(data_rows)} Data Rows Exported')
        print(f'Converting Data Rows into a COCO Dataset...')
    images = []
    data_row_check = set() # This is a check for projects where one data row has multiple labels (consensus, benchmark)
    if verbose:
        for item in tqdm(exported_datarows):
            datarow_id = item["data_row"]["id"]
            labels = item["projects"][project.uid]["labels"]
            for label in labels:
                data_row = data_rows[datarow_id]
                if datarow_id not in data_row_check:
                    data_row_check.add(datarow_id)
                    images.append({
                        "license" : 1, "file_name" : data_row["global_key"], "height" : data_row["height"],
                        "width" : data_row["width"], "date_captured" : data_row["created_at"],
                        "id" : datarow_id, "coco_url": data_row["row_data"]
                    })
    else:
        for item in exported_datarows:
            datarow_id = item["data_row"]["id"]
            labels = item["projects"][project.uid]["labels"]
            for label in labels:
                data_row = data_rows[datarow_id]
                if datarow_id not in data_row_check:
                    data_row_check.add(datarow_id)
                    images.append({
                        "license" : 1, "file_name" : data_row["global_key"], "height" : data_row["height"],
                        "width" : data_row["width"], "date_captured" : data_row["created_at"],
                        "id" : datarow_id, "coco_url": data_row["row_data"]
                    })
    if verbose:                     
        print(f'Data Rows Converted into a COCO Dataset.')  
        print(f'Converting Annotations into the COCO Format...')

    annotations = []
    ontology_schema_to_name_path = get_ontology_schema_to_name_path(project.ontology().normalized, detailed=True) 
    global_max_keypoints = 0
    futures = []
    if verbose:
        with ThreadPoolExecutor() as exc:
            for item in exported_datarows:
                datarow_id = item["data_row"]["id"]
                labels = item["projects"][project.uid]["labels"]
                for label in tqdm(labels):
                    for annotation in label['annotations']['objects']:
                        futures.append(exc.submit(_to_coco_annotation_converter, datarow_id, annotation, ontology_schema_to_name_path, labelboxClient))
                for f in tqdm(as_completed(futures)):
                    res = f.result()
                    if res[1] > global_max_keypoints:
                        global_max_keypoints = copy.deepcopy(res[1])
                    annotations.append(res[0])    
    else:
        with ThreadPoolExecutor() as exc:
            for item in exported_datarows:
                datarow_id = item["data_row"]["id"]
                labels = item["projects"][project.uid]["labels"]
                for label in labels:
                    for annotation in label['annotations']['objects']:
                        futures.append(exc.submit(_to_coco_annotation_converter, datarow_id, annotation, ontology_schema_to_name_path, labelboxClient))
                for f in as_completed(futures):
                    res = f.result()
                    if res[1] > global_max_keypoints:
                        global_max_keypoints = copy.deepcopy(res[1])
                    annotations.append(res[0])       
    if verbose:                     
        print(f'Annotation Conversion Complete. Converted {len(annotations)} annotations into the COCO Format.')                        
        print(f'Converting the Ontology into the COCO Dataset Format...') 
    categories = []        
    for featureSchemaId in ontology_schema_to_name_path:
        if ontology_schema_to_name_path[featureSchemaId]["type"] == "line": 
            keypoints = []
            skeleton = []
            for i in range(0, global_max_keypoints): 
                keypoints.append(str("line_")+str(i+1))
                skeleton.append([i, i+1])
            categories.append({
                "supercategory" : ontology_schema_to_name_path[featureSchemaId]['name'],
                "id" : ontology_schema_to_name_path[featureSchemaId]["encoded_value"],
                "name" : ontology_schema_to_name_path[featureSchemaId]['name'],
                "keypoints" : keypoints, "skeleton" : skeleton,
            })
        elif ontology_schema_to_name_path[featureSchemaId]["type"] == "point": 
            categories.append({
                "supercategory" : ontology_schema_to_name_path[featureSchemaId]['name'],
                "id" : ontology_schema_to_name_path[featureSchemaId]["encoded_value"],
                "name" : ontology_schema_to_name_path[featureSchemaId]['name'],
                "keypoints" : ['point'], "skeleton" : [0, 0],
            })        
        elif ontology_schema_to_name_path[featureSchemaId]['kind'] == 'tool':
            categories.append({
                "supercategory" : ontology_schema_to_name_path[featureSchemaId]['name'],
                "id" : ontology_schema_to_name_path[featureSchemaId]["encoded_value"],
                "name" : ontology_schema_to_name_path[featureSchemaId]['name']
            })     
        elif len(ontology_schema_to_name_path[featureSchemaId]['name_path'].split(divider)) == 2:
            categories.append({
                "supercategory" : ontology_schema_to_name_path[featureSchemaId]['name_path'].split(divider)[0],
                "id" : ontology_schema_to_name_path[featureSchemaId]["encoded_value"],
                "name" : ontology_schema_to_name_path[featureSchemaId]['name']
            })
    if verbose:            
        print(f'Ontology Conversion Complete')
        print(f'COCO Conversion Complete')   
    return {"info" : info, "licenses" : licenses, "images" : images, "annotations" : annotations, "categories" : categories}      
