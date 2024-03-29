# libraries imports here
import requests as req
import boto3
from botocore.exceptions import ClientError
import os
from time import sleep
from datetime import datetime
import json
import gzip

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
        sleep(4)
        url_loc = f"{endpoint}{response_format}?pagetoken={next_page_token}\
&key={API_KEY}"
        response = req.request("GET", url_loc, headers=headers, data=payload)
        return response

def s3_upload_file(
    bucket_name: str, 
    file_key: str,
    file_path: str
    ):
    """Upload a file to an S3 bucket

    Parameters:
        bucket_name: Bucket to upload to
        file_key: File key in the S3 bucket that the gz will be uploaded
        file_path: temporary file in tmp/ directory
    Return:
        True if file was uploaded, else False
    """
    # Upload the file
    s3_client = boto3.client("s3")
    try:
        response = s3_client.upload_file(
            Filename=file_path,
            Bucket=bucket_name,
            Key=file_key
        )
    except ClientError as e:
        print(e)
        return False
    return True

def send_to_details_lambda(results, context):
    region = "sa-east-1"
    ACC_ID = os.environ["AWS_ACCOUNT_ID"]
    event_bus_name = f"arn:aws:events:{region}:{ACC_ID}:event-bus/default"
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

def gzip_file(input_file, output_file):
    with open(input_file, 'rb') as f_in:
        with gzip.open(output_file, 'wb') as f_out:
            f_out.writelines(f_in)

def delete_schedule_rule(rule_name):
    try:
        event_bridge_client = boto3.client('scheduler')
        event_bridge_client.delete_schedule(
            Name=rule_name
        )
        print(f"Rule {rule_name} deleted succesfully.")
    except Exception as e:
        print(f"Couldn't delete rule: {rule_name}.\n{e}")


# lambda_handler function
def lambda_handler(event, context):
    print(event)
    # Define the initial variables
    schedule_rule_name = event.get("rule_name")
    coordinate = (event["coordinate"][0], event["coordinate"][1])
    radius = event["radius"]

    # requesting the Nearby Places API
    response = nearby_search(
        lat=coordinate[0],
        lon=coordinate[1],
        radius=radius
        )
    response_dict = json.loads(response.text)

    # Upload to S3
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S%f")
    year_month_day = timestamp[0:8]
    coordinate_string = str(coordinate[0])+"_"+str(coordinate[1])
    bucket = "dcpgm-sor"
    prefix = f"gmaps/nearby/{year_month_day}/"
    file_name = f"{coordinate_string}_{timestamp}_1"
    key = f"{prefix}{file_name}.gz"

    # Create a temporary file in the /tmp directory
    tmp_file_path_json = f'/tmp/{file_name}.json'
    tmp_file_path_gz = f'/tmp/{file_name}.gz'
    with open(tmp_file_path_json, 'w') as f:
        json.dump(
            obj=response_dict,
            fp=f,
            ensure_ascii=False
            )

    gzip_file(
        input_file=tmp_file_path_json,
        output_file=tmp_file_path_gz
        )

    s3_upload_file(
        bucket_name=bucket,
        file_key=key,
        file_path=tmp_file_path_gz
        )
    
    send_to_details_lambda(response_dict['results'], context)

    # pagination handler
    for page in [2,3]:
        if "next_page_token" in response_dict:
            print("token", page)
            token = response_dict["next_page_token"]
            response = nearby_search(next_page_token=token)
            response_dict = json.loads(response.text)

            # Upload to S3
            file_name = f"{coordinate_string}_{timestamp}_{page}"
            key = f"{prefix}{file_name}.gz"

            # Create a temporary file in the /tmp directory
            tmp_file_path_json = f'/tmp/{file_name}.json'
            tmp_file_path_gz = f'/tmp/{file_name}.gz'
            with open(tmp_file_path_json, 'w') as f:
                json.dump(
                    obj=response_dict,
                    fp=f,
                    ensure_ascii=False
                    )

            gzip_file(
                input_file=tmp_file_path_json,
                output_file=tmp_file_path_gz
                )

            s3_upload_file(
                bucket_name=bucket,
                file_key=key,
                file_path=tmp_file_path_gz
                )

            # Create details lambda event in EventBridge
            print("pre-sending")
            send_to_details_lambda(response_dict['results'], context)
        else:
            print("Sem token")
            break
    print(f"Deleting schedule rule: {schedule_rule_name}.")
    if schedule_rule_name:
        delete_schedule_rule(rule_name=schedule_rule_name)
    