# libraries imports here
import requests as req
import boto3
from botocore.exceptions import ClientError
import os
import time
from datetime import datetime
import json

def nearby_search(
    lat: str = None,
    lon: str = None,
    next_page_token: str = None,
    types: str = 'restaurant|bar|meal_delivery|meal_takeaway|cafe',
    radius: str = None,
    response_format: str = 'json'
    ):
    """Function that makes the requests for the Maps API - Nearby Search. 
    
    Parameters:
        lat (str): Latitude value.
        lon (str): Longitude value.
        types (str): Restricts the results to places matching the specified\
            type. 
            Access to see more: 
            URL
        radius (str): Defines the distance (in meters) within which to return\
            place results.
        response_format: Response format (json or xml).

    Returns:
        Response of the request
    """
    API_KEY = os.environ['GMAPS_API_KEY']
    endpoint = 'https://maps.googleapis.com/maps/api/place/nearbysearch/'
    payload={}
    headers = {}

    if next_page_token is None:
        # request the API
        url_loc = f"{endpoint}{response_format}?location={lat}%2c{lon}&radius=\
{radius}&type={types}&key={API_KEY}"
        response = req.request("GET", url_loc, headers=headers, data=payload)
        return response
    else:
        time.sleep(4)
        url_loc = f"{endpoint}{response_format}?pagetoken={next_page_token}\
&key={API_KEY}"
        response = req.request("GET", url_loc, headers=headers, data=payload)
        return response

# function that saves data in S3 bucket
def s3_put_object(
    bucket_name: str, 
    file_key: str,
    file: object
    ):
    """Upload a file to an S3 bucket

    Parameters:
        file: File to upload
        bucket: Bucket to upload to
        object_name: S3 object name. If not specified then file_name is used
    Return:
        True if file was uploaded, else False
    """
    # File to upload
    #json.dumps(file, ensure_ascii=False).encode('utf8')
    # Upload the file
    s3_client = boto3.client("s3")
    try:
        response = s3_client.put_object(
            Bucket=bucket_name, 
            Key=file_key, 
            Body=file,
            ContentType="text/plain;charset=utf-8"
            )
    except ClientError as e:
        print(e)
        return False
    return True

def send_to_details_lambda(results, context):
    region = "sa-east-1"
    acc_id = "820949372807"
    event_bus_name = f"arn:aws:events:{region}:{acc_id}:event-bus/default"
    print("sending details")
    # Get all Place Id's
    places_ids = [place['place_id'] for place in results]
    event_to_details = {'places_ids': places_ids}
    print(event_to_details)
    # send the events to EventBridge and then to the nearby lambda
    event_bridge_client = boto3.client('events')
    # Define the parameters for the PutEvents operation
    put_events_params = {
        "Entries": [
            {
            "Source": context.function_name,
            "DetailType": "Nearby to Details",
            "Detail": json.dumps(event_to_details),
            "EventBusName": event_bus_name
        }
        ]
    }
    print(put_events_params)
    # Send the event to EventBridge
    response = event_bridge_client.put_events(**put_events_params)
    print(response)

# lambda_handler function
def lambda_handler(event, context):
    print(event)
    # Define the initial variables
    coordinate = (event["coordinate"][0], event["coordinate"][1])
    radius = event["radius"]

    # requesting the Nearby Places API
    response = nearby_search(
        lat=coordinate[0],
        lon=coordinate[1],
        radius=radius
        )
    response_dict = json.loads(response.text)
    response_encoded = json.dumps(
        response_dict, 
        ensure_ascii=False
        ).encode('utf8')  
    # Upload to S3
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S%f")
    coordinate_string = str(coordinate[0])+"_"+str(coordinate[1])
    bucket = "dcpgm-sor"
    prefix = "gmaps/nearby/"
    file_name = f"{coordinate_string}_{timestamp}_1.json"
    key = f"{prefix}{file_name}"

    s3_put_object(
        bucket_name=bucket,
        file_key=key,
        file=response_encoded
        )
    
    send_to_details_lambda(response_dict['results'], context)

    # pagination handler
    for page in [2,3]:
        if "next_page_token" in response_dict:
            print("token", page)
            token = response_dict["next_page_token"]
            response = nearby_search(next_page_token=token)
            response_dict = json.loads(response.text)
            response_encoded = json.dumps(
                response_dict, 
                ensure_ascii=False
                ).encode('utf8')
            # Upload to S3
            file_name = f"{coordinate_string}_{timestamp}_{page}.json"
            key = f"{prefix}{file_name}"

            s3_put_object(
                bucket_name=bucket,
                file_key=key,
                file=response_encoded
                )

            # Create details lambda event in EventBridge
            print("pre-sending")
            send_to_details_lambda(response_dict['results'], context)
        else:
            print("Sem token")
            break