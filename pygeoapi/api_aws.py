"""
This module offers functions to communicate with AWS services
"""

# 3rd party imports
import boto3, json

def get_secret(region: str, service_name: str, secret_key: str):
    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name=service_name,
        region_name=region
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_key
        )
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e

    # Decrypts secret using the associated KMS key.
    return json.loads(get_secret_value_response['SecretString'])
