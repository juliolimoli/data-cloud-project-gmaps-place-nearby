# libraries imports here
import boto3
from botocore.exceptions import ClientError

# get secret from secret manager service
def get_secret():
    
    secret_name = "dev/GMapsAPI"
    region_name = "sa-east-1"

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
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e

    # Decrypts secret using the associated KMS key.
    secret = get_secret_value_response['SecretString']


# function that retry and abort on attempts to do something
def retry_abort(max_retries=3):
    for attempt in range(1, max_retries + 1):
        if attempt <= max_retries:
            pass # try again
        else:
            pass # abort

# function that queries the location used in the nearby search
def query_lat_lon():
    pass