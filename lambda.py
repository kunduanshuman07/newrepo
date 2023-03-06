
import boto3
import json
import os
from requests_aws4auth import AWS4Auth 
import requests


# Initialize OpenSearch Service endpoint and credentials
service = 'es'
region = os.environ['us-east-1']
host = 'https://search-fbapidev-gxfelrhtigdqfuexpxybqwtm6m.us-east-1.es.amazonaws.com'
index = 'fbapiindex'
credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)


# Initialize the OpenSearch Service endpoint URL
url = 'https://' + host + '/' + index + '/_doc'


def lambda_handler(event, context):
    for record in event['records']:
        # Decode the base64-encoded record data and convert it to JSON
        payload = json.loads(base64.b64decode(record['data']).decode('utf-8'))


        # Send the JSON document to OpenSearch Service using the requests library
        headers = {'Content-Type': 'application/json' }
        response = requests.post(url, auth=awsauth, headers=headers, json=payload)


        # Log the response from OpenSearch Service
        print(response.json())
        
    # Return the processing result to Firehose
    return {'records': [{'recordId': record['recordId'], 'result': 'Ok' }for record in event['records']] }
 