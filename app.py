# libraries imports here
import requests as req
import boto3
from botocore.exceptions import ClientError
import os
import time
from datetime import datetime
import json

# function that retry and abort on attempts to do something
def retry_abort(func: function, max_retries: int = 3):
    def retry_abort_wrapper(*args, **kwargs):
        function_name = func.__name__
        print(f"Initializing the function: {function_name}")
        for attempt in range(1, max_retries+1):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                print(e)
                if attempt == max_retries:
                    # abort
                    raise e
                time.sleep(5)
    return retry_abort_wrapper

# function that requests in the nearby search
@retry_abort
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
    #####
    if next_page_token is None:
        # request the API
        url_loc = f"{endpoint}{response_format}?location={lat}%2c{lon}&radius=\
{radius}&type={types}&key={API_KEY}"
        response = req.request("GET", url_loc, headers=headers, data=payload)
        return response
    else:
        time.sleep(4)
        token = next_page_token
        url_loc = f"{endpoint}{response_format}?pagetoken={token}\
&key={API_KEY}"
        response = req.request("GET", url_loc, headers=headers, data=payload)
        return response

# function that saves data in S3 bucket
@retry_abort
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
    upload_byte_stream = bytes(file.encode("UTF-8"))

    # Upload the file
    s3_client = boto3.client("s3")
    try:
        response = s3_client.put_object(
            Bucket=bucket_name, 
            Key=file_key, 
            Body=upload_byte_stream
            )
    except ClientError as e:
        print(e)
        return False
    return True

def send_to_details_lambda(results, context):
    
    # Get all Place Id's
    places_ids = [place['place_id'] for place in results]
    event_to_details = {'places_ids': places_ids}

    # send the events to EventBridge and then to the nearby lambda
    event_bridge_client = boto3.client('events')

    # Define the parameters for the PutEvents operation
    put_events_params = {
        'Entries': [
                {
                'Source': context.function_name,
                'Target': "data-cloud-project-gmaps-details",
                'Detail': json.dumps(event_to_details)
            }
        ]
    }
    # Send the event to EventBridge
    response = event_bridge_client.put_events(**put_events_params)
    print(response)

# lambda_handler function
def lambda_handler(event, context):

    # Define the initial variables
    coordinate = (event["latitude"], event["longitude"])
    radius = event["radius"]

    # requesting the Nearby Places API
    response = nearby_search(
        lat=coordinate[0],
        lon=coordinate[1],
        radius=radius
        )
    response_dict = json.loads(response.text)
    
    # Upload to S3
    t = datetime.now()
    timestamp = datetime.strftime(t, "%Y%m%d%H%M%S%f")
    coordinate_string = str(coordinate[0])+"_"+str(coordinate[1])
    bucket = "SoR"
    prefix = "gmaps/nearby/"
    file_name = f"{coordinate_string}_{timestamp}_1.json"
    key = f"{prefix}{file_name}"

    s3_put_object(
        bucket_name=bucket,
        file_key=key,
        file=response_dict
        )
    
    # Create details lambda event in EventBridge
    send_to_details_lambda(response_dict['results'], context)

    # pagination handler
    for page in range(2,4):
        if "next_page_token" in response_dict:
            token = response_dict["next_page_token"]
            response = nearby_search(next_page_token=token)
            response_dict = json.loads(response.text)

            # Upload to S3
            t = datetime.now()
            timestamp = datetime.strftime(t, "%Y%m%d%H%M%S%f")
            file_name = f"{coordinate_string}_{timestamp}_{page}.json"
            key = f"{prefix}{file_name}"

            s3_put_object(
                bucket_name=bucket,
                file_key=key,
                file=response_dict
                )

            # Create details lambda event in EventBridge
            send_to_details_lambda(response_dict['results'], context)
        else:
            print("Sem token")
            break