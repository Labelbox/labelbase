from labelbox import Client
import copy
import json
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

def to_coco_bbox_converter(data_row_id, annotation, category_id):
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
        "bbox": [annotation['bbox']['left'], annotation['bbox']['top'], annotation['bbox']['width'], annotation['bbox']['height']],
        "category_id": category_id,
        "id": annotation['featureId']
    }
    return coco_annotation

def to_coco_line_converter(data_row_id, annotation, category_id):
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
        "id": annotation['featureId']
    }
    return coco_annotation, num_line_keypoints

def to_coco_point_converter(data_row_id, annotation, category_id):
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
        "id": annotation['featureId']
    }
    return coco_annotation  

def to_coco_polygon_converter(data_row_id, annotation, category_id):
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
        "id": annotation['featureId'],
        "iscrowd" : 0,
        "category_id" : category_id
    }  
    return coco_annotation
  
@retry.Retry(predicate=retry.if_exception_type(Exception), deadline=1200.)
def download_mask(annotation):
    """Incorporates retry logic into the download of a mask / polygon Instance URI
    Args:
        annotation (dict)       :     A dictionary pertaining to a label exported from Labelbox
    Returns:
        A numPy array of said mask
    """ 
    return requests.get(annotation['instanceURI']).content 

def to_coco_mask_converter(data_row_id, annotation, category_id):
    """Given a label dictionary and a mask annotation from said label, will return the coco-converted segmentation mask annotation dictionary
    Args:
        data_row_id (str)               :     Labelbox Data Row ID for this label
        annotation (dict)               :     Annotation dictionary from label['Label']['objects'], which comes from project.export_labels()
        category_id (str)               :     Desired category_id for the coco_annotation
    Returns:
        An annotation dictionary in the COCO format
    """  
    mask_data = download_mask(annotation['instanceURI'])
    binary_mask_arr = np.where(np.array(Image.open(BytesIO(mask_data))))
    contours = cv2.findContours(binary_mask_arr, cv2.RETR_LIST, cv2.CHAIN_APPROX_SIMPLE)[0]
    coords = contours[0]
    poly_points = np.array([[coords[i][0][0], coords[i][0][1]] for i in range(0, len(coords))])
    bounds = Polygon(poly_points).bounds
    coco_annotation = {
        "image_id" : data_row_id,
        "segmentation" : [[item for sublist in poly_points for item in sublist]],
        "bbox" : [bounds[0], bounds[1], bounds[2]-bounds[0], bounds[3]-bounds[1]],
        "area" : Polygon(poly_points).area,
        "id": annotation['featureId'],
        "iscrowd" : 0,
        "category_id" : category_id
    }  
    return coco_annotation

def to_coco_annotation_converter(data_row_id, annotation, ontology_index):
    """ Wrapper to triage and multithread the coco annotation conversion - if nested classes exist, the category_id will be the first radio/checklist classification answer available
    Args:
        data_row_id (str)               :     Labelbox Data Row ID for this label
        annotation (dict)               :     Annotation dictionary from label['Label']['objects'], which comes from project.export_labels()
        ontology_index (dict)           :     A dictionary where {key=featureSchemaId : value = {"encoded_value"} which corresponds to category_id
    Returns:
        A dictionary corresponding to te coco annotation syntax - the category ID used will be the top-level tool 
    """
    max_line_keypoints = 0
    category_id = ontology_index[annotation['schemaId']]['encoded_value']
    if "classifications" in annotation.keys():
        if annotation['classifications']:
            for classification in annotation['classifications']:
                if 'answer' in classification.keys():
                    if type(classification['answer']) == dict:
                        category_id = ontology_index[classification['schemaId']]['encoded_value']
                        break
                else:
                    category_id = ontology_index[classification['answers'][0]['schemaId']]['encoded_value']
                    break
    if "bbox" in annotation.keys():
        coco_annotation = to_coco_bbox_converter(data_row_id, annotation, category_id)
    elif "line" in annotation.keys():
        coco_annotation, max_line_keypoints = to_coco_line_converter(data_row_id, annotation, category_id)
    elif "point" in annotation.keys():
        coco_annotation = to_coco_point_converter(data_row_id, annotation, category_id)
    elif "polygon" in annotation.keys():
        coco_annotation = to_coco_polygon_converter(data_row_id, annotation, category_id)
    else: 
        coco_annotation = to_coco_mask_converter(data_row_id, annotation, category_id)     
    return coco_annotation, max_line_keypoints

def export_to_coco(project):
    """ Given a project and a list of labels, will create the COCO export json
    Args:
        project (labelbox.schema.project.Project)   :   Labelbox project object
    Returns:

    """
    labels_list = project.export_labels(download=True)
    # Info section generated from project information
    info = {
        'description' : project.name,
        'url' : f'https://app.labelbox.com/projects/{project.uid}/overview',
        'version' : "1.0", 
        'year' : datetime.datetime.now().year,
        'contributor' : project.created_by().email,
        'date_created' : datetime.datetime.now().strftime('%Y/%m/%d'),
    }
    # Licenses section is left empty
    licenses = [
        {
            "url" : "N/A",
            "id" : 1,
            "name" : "N/A"
        }
    ]
    # Create a dictionary where {key=data_row_id : value=data_row}
    data_rows = {}
    print(f'Exporting Data Rows from Project...')
    for dataset in project.datasets():
        for data_row in dataset.export_data_rows():
            data_rows.update({data_row.uid : data_row})  
    print(f'\nExport complete. {len(data_rows)} Data Rows Exported')
    # Images section generated from data row export
    print(f'\nConverting Data Rows into a COCO Dataset...\n')
    images = []
    data_row_check = [] # This is a check for projects where one data row has multiple labels (consensus, benchmark)
    for label in tqdm(labels_list):
        data_row = data_rows[label['DataRow ID']]
        if label['DataRow ID'] not in data_row_check:
            data_row_check.append(label['DataRow ID'])
            images.append({
                "license" : 1,
                "file_name" : data_row.external_id,
                "height" : data_row.media_attributes['height'],
                "width" : data_row.media_attributes['width'],
                "date_captured" : data_row.created_at.strftime('%Y-%m-%d %H:%M:%S'),
                "id" : data_row.uid,
                "coco_url": data_row.row_data
            })
    print(f'\nData Rows Converted into a COCO Dataset.')  
    annotations = []
    print(f'\nConverting Annotations into the COCO Format...\n')
    ontology_index = index_ontology(project.ontology().normalized) 
    global_max_keypoints = 0
    futures = []
    with ThreadPoolExecutor() as exc:
        for label in tqdm(labels_list):
            for annotation in label['Label']['objects']:
                futures.append(exc.submit(coco_annotation_converter, label['DataRow ID'], annotation, ontology_index))
        for f in as_completed(futures):
            res = f.result()
            if res[1] > global_max_keypoints:
                global_max_keypoints = copy.deepcopy(res[1])
            annotations.append(res[0])    
    print(f'\nAnnotation Conversion Complete. Converted {len(annotations)} annotations into the COCO Format.')    
    categories = []
    print(f'\nConverting the Ontology into the COCO Dataset Format...') 
    for featureSchemaId in ontology_index:
        if ontology_index[featureSchemaId]["type"] == "line": 
            keypoints = []
            skeleton = []
            for i in range(0, global_max_keypoints): 
                keypoints.append(str("line_")+str(i+1))
                skeleton.append([i, i+1])
            categories.append({
                "supercategory" : ontology_index[featureSchemaId]['name'],
                "id" : ontology_index[featureSchemaId]["encoded_value"],
                "name" : ontology_index[featureSchemaId]['name'],
                "keypoints" : keypoints,
                "skeleton" : skeleton,
            })
        elif ontology_index[featureSchemaId]["type"] == "point": 
            categories.append({
                "supercategory" : ontology_index[featureSchemaId]['name'],
                "id" : ontology_index[featureSchemaId]["encoded_value"],
                "name" : ontology_index[featureSchemaId]['name'],
                "keypoints" : ['point'],
                "skeleton" : [0, 0],
            })        
        elif ontology_index[featureSchemaId]['kind'] == 'tool':
            categories.append({
                "supercategory" : ontology_index[featureSchemaId]['name'],
                "id" : ontology_index[featureSchemaId]["encoded_value"],
                "name" : ontology_index[featureSchemaId]['name']
            })     
        elif len(ontology_index[featureSchemaId]['parent_featureSchemaIds']) == 2:
            supercategory = ontology_index[ontology_index[featureSchemaId]['parent_featureSchemaIds'][0]]['name']
            categories.append({
                "supercategory" : supercategory,
                "id" : ontology_index[featureSchemaId]["encoded_value"],
                "name" : ontology_index[featureSchemaId]['name']
            })
    print(f'\nOntology Conversion Complete')       
    coco_dataset = {
        "info" : info,
        "licenses" : licenses,
        "images" : images,
        "annotations" : annotations,
        "categories" : categories
    }      
    print(f'\nCOCO Conversion Complete')    
    return coco_dataset
