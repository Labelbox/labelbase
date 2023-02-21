import numpy as np
from PIL import Image
from io import BytesIO
import requests
from labelbox.data import annotation_types as lb_types
from labelbox.data.serialization import NDJsonConverter

def mask_to_bytes(input:str, method:str="url", color:int=0, output:str="png"):
    """ Given a mask input, returns a png bytearray of said mask a a dictionary
    Args:
        input     :   Required (str) - URL of a mask
        method    :   Required (str) - Either "url" or "array" - determines how you want the input treated
        output    :   Required (str) - Either "array" or "png" - determines how you want the data returned
    Returns:
        Mask as a numpy array or as string png bytes
    """
    # Convert URL or array mask into a binary array
    if method not in ["url", "array"]:
        raise ValueError(f'Downloading bytes requires input method to be either a "url" or a "array" - received method {method}')
    if output not in ["array", "png"]:
        raise ValueError(f'Downloading bytes requires output method to be either a "png" or a "array" - received method {method}')     
    # Either download a mask URL or ensure the shape of your numpy array
    if method == "url":
        binary_mask = np.array(Image.open(BytesIO(requests.get(input).content)))[:,:,0]
    else:
        binary_mask = input if len(input.shape)==2 else input[:,:,0]
    # Return either a numpy array or a png byte string
    if output == "array":
        return binary_mask
    else:
        mask_label = lb_types.Label(
            data=lb_types.ImageData(uid=""),
            annotations=[
                lb_types.ObjectAnnotation(
                    name="", 
                    value=lb_types.Mask(
                        mask=lb_types.MaskData.from_2D_arr(binary_mask), 
                        color=color
                    )
                )
            ]
        )
        # Convert back into ndjson
        mask_png = list(NDJsonConverter.serialize([mask_label]))[0]["mask"]["png"]
        return png_mask
