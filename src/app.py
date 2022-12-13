# libraries imports here
import requests as req
import mysql.connector
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

# get secret from secret manager service
def get_secret():
    """Function that get secrets from the AWS Secret Manager Service
    
    Parameters: None
    
    Returns:
    String: Access key
    """

    ENVIRONMENT = os.environ['environment']    
    SECRET_NAME = f'{ENVIRONMENT}/GMapsAPI'
    REGION = "sa-east-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name = 'secretsmanager',
        region_name = REGION
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId = SECRET_NAME
        )
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_
        # GetSecretValue.html
        raise e

    # Decrypts secret using the associated KMS key.
    SECRET = get_secret_value_response['SecretString']

    return SECRET

# function that queries in the RDS Database
@retry_abort
def query_db(query: str):
    """Function that queries in the RDS Database instance."""

    ENDPOINT = ""
    PORT = ""
    USER = ""
    REGION = "sa-east-1"
    DBNAME = ""
    #os.environ['LIBMYSQL_ENABLE_CLEARTEXT_PLUGIN'] = '1'

    #gets the credentials from .aws/credentials
    session = boto3.session.Session()
    client = session.client(
        service_name = 'rds',
        region_name = REGION
    )

    token = client.generate_db_auth_token(
        DBHostname=ENDPOINT,
        Port=PORT,
        DBUsername=USER,
        Region=REGION
        )
    conn =  mysql.connector.connect(
        host=ENDPOINT,
        user=USER,
        passwd=token,
        port=PORT,
        database=DBNAME,
        ssl_ca='SSLCERTIFICATE'
        )
    cur = conn.cursor()
    cur.execute(query)
    query_results = cur.fetchall()
    return query_results


# function that requests in the nearby search
def nearby_search(
    lat: str,
    lon: str,
    types: str = 'restaurant|bar|meal_delivery|meal_takeaway|cafe',
    radius: str = '400',
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
    API_KEY = get_secret()
    endpoint = 'https://maps.googleapis.com/maps/api/place/nearbysearch/'
    url_loc = f"{endpoint}{response_format}?location={lat}%2c{lon}&radius=\
{radius}&type={types}&key={API_KEY}"

    # request the API
    payload={}
    headers = {}

    try:
        response = req.request("GET", url_loc, headers=headers, data=payload)
    except Exception as e:
        raise e
    else:
        # return OK
        #if response.status_code != 200:
            #retry_abort(func=lambda_handler())
        return response

# function that requests with token
def token_search(
    response_json: str,
    bucket_name: str,
    prefix_name: str,
    response_format: str = 'json'
    ):
    """Function that makes the requests for the Maps API - Nearby Search 
    Additional Results with Token. 
    
    Parameters:
        token (str): Token provided in previous Nearby Search request.
        response_format: Response format (json | xml).

    Returns:
        Response of the request
    """
    for _ in range(1,3):
        if "next_page_token" in response_json:
            token = response_json["next_page_token"]
            API_KEY = get_secret()
            endpoint = "https://maps.googleapis.com/maps/api/place/\
nearbysearch/"
            url_loc = f"{endpoint}{response_format}?pagetoken={token}\
&key={API_KEY}"

            # request the API
            payload={}
            headers = {}

            try:
                response = req.request(
                    "GET",
                    url_loc,
                    headers=headers,
                    data=payload
                    )
            except Exception as e:
                raise e
            else:
                t = datetime.now()
                timestamp = datetime.strftime(t, "%Y%m%d%H%M%S%f")
                
                key = f"{prefix_name}{timestamp}.json"

                s3_put_object(
                    bucket_name=bucket_name,
                    file_key=key,
                    file=response_json
                    )

        else:
            print("Sem token")
            break

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

# lambda_handler function
def lambda_handler():
    query = """SELECT 
            * 
        FROM 
            db.points_to_search 
        LIMIT 1;
        """
    # querying db to get location
    loc_to_search_query = query_db(query=query)

    # variables returned
    loc_search_id = loc_to_search_query["loc_search_id"]
    country = loc_to_search_query["country"]
    state = loc_to_search_query["state"]
    city = loc_to_search_query["city"]

    # requesting the Nearby Places API
    response = nearby_search(
        lat=loc_to_search_query["lat"],
        lon=loc_to_search_query["lon"]
        )
    response_json = json.loads(response.text)

    # Upload to S3
    t = datetime.now()
    timestamp = datetime.strftime(t, "%Y%m%d%H%M%S%f")
    bucket = "SoR"
    prefix = f"gmaps/nearby/{country}/{state}/{city}/"
    file_name = f"{timestamp}.json"
    key = f"{prefix}{file_name}"

    s3_put_object(
        bucket_name=bucket,
        file_key=key,
        file=response_json
        )

    # pagination handler
    token_search(
        response_json=response_json, 
        bucket_name=bucket,
        prefix_name=prefix
        )
    
    # Delete row
    query = f"""DELETE FROM
                db.points_to_search
            WHERE loc_search_id = {loc_search_id};
    """

    # Insert into db - snapshot

lambda_handler()