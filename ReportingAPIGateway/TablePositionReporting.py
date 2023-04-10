import json
import boto3
import os
import datetime

AwsAccessKey = os.environ.get("AWS_ACCESS_KEY_")
AwsSecretAccessKey =  os.environ.get("AWS_SECRET_ACCESS_KEY_")
AwsRegion = os.environ.get("AWS_REGION_")

def get_dynamo_db():
    dynamo_db_client = boto3.client('dynamodb',aws_access_key_id=AwsAccessKey, aws_secret_access_key=AwsSecretAccessKey,region_name=AwsRegion)
    return dynamo_db_client

def lambda_handler(event, context):
    response = {}
    response['isBase64Encoded'] = False
    response['headers'] = {}
    response['headers']['Content-Type'] = 'application/json'
    try:
        date = event['queryStringParameters']['date']
        date_datetime = datetime.datetime.strptime(date,'%Y-%m-%d')
        yesterday_datetime = datetime.datetime.utcnow() + datetime.timedelta(days=-1)
        if date_datetime >= yesterday_datetime:
            date = yesterday_datetime.strftime('%Y-%m-%d')
        dynamo_db_client = get_dynamo_db()
        data = dynamo_db_client.get_item(
                    Key={
                'Date': {
                    'S': date,
                }
            },
            TableName='table_position',
        )
        response_body_message = json.loads(data['Item']['league_table']['S'])
        response['statusCode'] = 200
        response['body'] = json.dumps(response_body_message)
    except Exception as e:
        response['statusCode'] = 400
        response['body'] = str(e)
    finally:
        return response
    