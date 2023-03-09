import numpy as np
from PIL import Image
from io import BytesIO
import requests
from labelbox.data import annotation_types as lb_types
from labelbox.data.serialization import NDJsonConverter

def mask_to_bytes(input:str, method:str="url", color=[255,255,255], output:str="png"):
    """ Given a mask input, returns a png bytearray of said mask
    Args:
        input     :   Required (str) - URL of a mask
        method    :   Required (str) - Either "url" or "array" - determines how you want the input treated
        color     :   Required (arr or int) - The color of your mask in your input value
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
        r = requests.get(input).content
        np_mask = np.array(Image.open(BytesIO(r)))[:,:,:3]
    else:
        if len(input.shape) == 3:
            np_mask = input
        elif len(input.shape) == 2:
            np_mask = [input, input, input]
        else:
            raise ValueError(f"Input segmentation mask arrays must either be 2D or 3D - shape of input mask: {input.shape}")
    if type(color) == int:
        np_color = [color, color, color]
    elif len(color) == 3:
        np_color = color
    else:
        raise ValueError(f"The specified color of your segmentation mask must either be a number (for 2D masks) or an RGB code (for 3D masks) - {color}")
    # Return either a numpy array or a png byte string
    if output == "array":
        return np_mask
    else:
        mask_label = lb_types.Label(
            data=lb_types.ImageData(uid=""),
            annotations=[
                lb_types.ObjectAnnotation(
                    name="", 
                    value=lb_types.Mask(
                        mask=lb_types.MaskData(arr=np_mask), 
                        color=np_color
                    )
                )
            ]
        )
        # Convert back into ndjson
        mask_png = list(NDJsonConverter.serialize([mask_label]))[0]["mask"]["png"]
        return mask_png
