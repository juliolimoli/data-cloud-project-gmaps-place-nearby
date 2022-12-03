# libraries imports here
import requests as req
import mysql.connector
import boto3
from botocore.exceptions import ClientError

# get secret from secret manager service
def get_secret():
    """Function that get secrets from the AWS Secret Manager Service
    
    Parameters: None
    
    Returns:
    String: Access key
    """

    #environment = event['env'] # get environment variable
    # should setup env variable
    secret_name = f'{environment}/GMapsAPI'
    region_name = 'sa-east-1'

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_
        # GetSecretValue.html
        raise e

    # Decrypts secret using the associated KMS key.
    secret = get_secret_value_response['SecretString']

    return secret


# function that retry and abort on attempts to do something
def retry_abort(max_retries: int = 3):
    for attempt in range(1, max_retries + 1):
        if attempt <= max_retries:
            pass # try again
        else:
            pass # abort


# function that queries the locations in the RDS instance
def query_lat_lon():
    """Function that queries the locations in the RDS instance."""

    ENDPOINT=""
    PORT=""
    USER=""
    REGION=""
    DBNAME=""
    #os.environ['LIBMYSQL_ENABLE_CLEARTEXT_PLUGIN'] = '1'

    #gets the credentials from .aws/credentials
    session = boto3.Session(profile_name='default')
    client = session.client('rds')

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
        cur.execute("""SELECT """)
        query_results = cur.fetchall()

    except Exception as e:
        print("Database connection failed due to {}".format(e))      

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
    url = f"{endpoint}{response_format}?location={lat}%2c{lon}&radius={radius}\
&type={types}&key={API_KEY}"

    # request the API
    payload={}
    headers = {}

    try:
        response = req.request("GET", url, headers=headers, data=payload)
    except Exception as e:
        raise e
    
    return response