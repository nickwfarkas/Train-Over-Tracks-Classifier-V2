import json
import urllib3
import boto3
from datetime import datetime

def lambda_handler(event, context):
    file_name = ""
    prediction = {}
    if(not (file_name := upload_to_s3(event["pathParameters"]["camera_name"])) or not (prediction := rek(file_name))):
        return json.dumps({
            "statusCode": 400,
            "message": "Failed to communicate,try again later",
            "body":{
                "prediction": -99,
            }
        })
    
    return parse_prediction(file_name, prediction)

def parse_prediction(file_name: str, pred: dict) -> dict:
    labels = []
    average_confidence = 0
    for label in pred['Labels']:
        labels.append((label['Name'].replace(" ",""), label['Confidence']))
        average_confidence+=label['Confidence']
    average_confidence/=len(labels)
    if(average_confidence >= 50):
        upload_to_storage(file_name, labels, 1)
        return json.dumps({
            "statusCode": 200,
            "message": "There is a train over the tracks",
            "body":{
                "prediction": 1
            }
        })
    else:
        upload_to_storage(file_name, labels, 0)
        return json.dumps({
            "statusCode": 200,
            "message": "There is not a train over the tracks",
            "body":{
                "prediction": 0
            }
        })

def get_current_crossing_image(camera_name: str):
    url = f"http://rrcrossings.woodhavenmi.org/{camera_name}.jpg?rnd="
    try:
        http = urllib3.PoolManager()
        image_request = http.request("GET", url)
    except Exception as e:
        print(f"Connection to camera failed: {str(e)}")
        return None
    return image_request.data

def rek(image_name: str) -> dict:
    try:
        rek = boto3.client('rekognition')
        response = rek.detect_labels(
        Image={
            'S3Object': {
                'Bucket': 'train-over-tracks-inference-storage-open',
                'Name': image_name,
            },
        },
        MaxLabels=10,
        MinConfidence=0,
        Settings={
            "GeneralLabels": {
                "LabelInclusionFilters": 
                    [ "Train", "Shipping Container", "Freight Car" ]
                }
            }
        )
    except Exception as e:
        print(f"Rekognition Failed: {str(e)}")
        return {}
    return response

def upload_to_s3(camera_name: str) -> str:
    try:
        s3 = boto3.resource('s3')
        if(local_file := get_current_crossing_image(camera_name)):
            uid = str(datetime.now())
            s3.Bucket('train-over-tracks-inference-storage-open').put_object(Key=f'{uid}.jpg', Body=local_file)
        else:
            print(f"Upload Failed")
            return ""
    except Exception as e:
        print(f"Upload Failed: {str(e)}")
        return ""
    return uid+'.jpg'

def upload_to_storage(s3_object_name: str, labels: list, prediction: int):
        path = f'/tmp/{s3_object_name}'
        file_name = f'{s3_object_name.split(".")[0]}_'
        for label in labels:
            file_name+=f'{label[0]}-{label[1]}_'
        file_name+=f'{prediction}.jpg'
        path = f'/tmp/{s3_object_name}'
        glacier = boto3.resource('glacier')
        s3 = boto3.resource('s3')
        s3.Bucket('train-over-tracks-inference-storage-open').download_file(s3_object_name, path)
        with open(path, 'rb') as f:
            s3.Bucket("train-over-tracks-inference-storage-open-archive").put_object(Key=file_name, Body=f)
        s3.Bucket('train-over-tracks-inference-storage-open').delete_objects(
            Delete={
                    'Objects': [
                            {
                                'Key': s3_object_name
                            },
                        ],
                    'Quiet': False
                })
