# libraries imports here
import requests as req
import mysql.connector
import boto3
from botocore.exceptions import ClientError
import os
import time

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


# function that retry and abort on attempts to do something
#def retry_abort(
#    func,
#    params: dict,
#    max_retries: int = 3,
#    attempt: int = 1
#    ):
#    if attempt <= max_retries:
#        attempt += 1
#        func(attempt=attempt)
#    else:
#        pass # abort


# function that queries in the RDS Database
def query_db(query: str, attempt: int = 1):
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

    try:
        conn =  mysql.connector.connect(
            host=ENDPOINT,
            user=USER,
            passwd=token,
            port=PORT,
            database=DBNAME,
            ssl_ca='SSLCERTIFICATE'
            )
        cur = conn.cursor()
        cur.execute(f"""{query}""")
        query_results = cur.fetchall()
    except Exception as e:
        print("Database connection failed due to {}".format(e))
        #retry_abort(func=query_db(query=query), attempt=attempt)
    else:
        return query_results


# function that requests in the nearby search
def nearby_search(
    lat: str,
    lon: str,
    types: str = 'restaurant|bar|meal_delivery|meal_takeaway|cafe',
    radius: str = '400',
    response_format: str = 'json',
    #attempt: int = 1
    ):
    """Function that makes the requests for the Maps API - Nearby Search. 
    
    Parameters:
    lat (str): Latitude value.
    lon (str): Longitude value.
    types (str): Restricts the results to places matching the specified type. 
        Access to see more: 
        URL
    radius (str): Defines the distance (in meters) within which to return place
        results.
    response_format: Response format (json or xml).

    Returns:
    JSON: Response of the request
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

# lambda_handler function
def lambda_handler(attempt):
    lat_lon = query_db(query="SELECT * FROM db.points_to_search LIMIT 1")
    response = nearby_search(
        lat=lat_lon['lat'],
        lon=lat_lon['lon']
        )

    # pagination handler


lambda_handler()