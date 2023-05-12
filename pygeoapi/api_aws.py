"""
This module offers functions to communicate with AWS services
"""

# 3rd party imports
import os, boto3, botocore, json

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
    except botocore.exceptions.ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e

    # Decrypts secret using the associated KMS key.
    return json.loads(get_secret_value_response['SecretString'])

def connect_s3_send_file(source_file: str, iam_role: str, bucket_name: str, prefix: str, file: str):
    """
    Uploads the given file to an S3 Bucket, given an iam_role and a bucket name.
    """

    try:
        # Create an STS client object that represents a live connection to the
        # STS service
        sts_client = boto3.client('sts')

        # Call the assume_role method of the STSConnection object and pass the role
        # ARN and a role session name.
        assumed_role_object = sts_client.assume_role(
            RoleArn=iam_role,
            RoleSessionName="AssumeRoleECS"
        )

        # From the response that contains the assumed role, get the temporary
        # credentials that can be used to make subsequent API calls
        credentials = assumed_role_object['Credentials']

        # Use the temporary credentials that AssumeRole returns to make a
        # connection to Amazon S3
        s3_resource = boto3.resource(
            's3',
            aws_access_key_id=credentials['AccessKeyId'],
            aws_secret_access_key=credentials['SecretAccessKey'],
            aws_session_token= credentials['SessionToken']
        )

        # Make sure it ends with a "/"
        if not prefix.endswith("/"):
            prefix = prefix + "/"

        # Send the file to the bucket
        s3_resource.Bucket(bucket_name).upload_file(source_file, prefix + os.path.basename(file))

    except botocore.exceptions.ClientError as e:
        print("ERROR UPLOADING FILE TO S3")
        print(str(e))
        raise