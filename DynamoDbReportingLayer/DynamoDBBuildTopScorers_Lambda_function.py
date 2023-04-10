import json
import boto3
import snowflake.connector
import os

SnowflakeUser = os.environ.get("SNOWFLAKE_USER")
SnowflakePassword = os.environ.get("SNOWFLAKE_PASS")
SnowflakeAccount = os.environ.get("SNOWFLAKE_ACCOUNT")
AwsAccessKey = os.environ.get("AWS_ACCESS_KEY_")
AwsSecretAccessKey =  os.environ.get("AWS_SECRET_ACCESS_KEY_")
AwsRegion = os.environ.get("AWS_REGION_")
SnsTopicArn="arn:aws:sns:eu-west-2:460327182513:FootballAPINotificationChannel"

class Helpers():
    def SnowflakeConnection():
        try:
            connection = snowflake.connector.connect(
                user = SnowflakeUser,
                password = SnowflakePassword,
                account = SnowflakeAccount
            )
        except Exception as e:
            Helpers.SendSNSNotification('ERROR in DynamoDB Build | callsite: SnowflakeConnection', str(e))
        finally:
            return connection
    
    def GetSNSClient():
        try:
            sns_client = boto3.client('sns',aws_access_key_id=AwsAccessKey, aws_secret_access_key=AwsSecretAccessKey,region_name=AwsRegion)
        except Exception as e:
            Helpers.SendSNSNotification('ERROR in DynamoDB Build | callsite: GetSNSClient', str(e))
        finally:
            return sns_client
    
    def GetDynamoDBClient():
        try:
            dynamo_db_client = boto3.client('dynamodb',aws_access_key_id=AwsAccessKey, aws_secret_access_key=AwsSecretAccessKey,region_name=AwsRegion)
        except Exception as e:
            Helpers.SendSNSNotification('ERROR in DynamoDB Build | callsite: GetDynamoDBClient', str(e))
        finally:
            return dynamo_db_client
    
    def SendSNSNotification(Level :str,Message_ :str) -> None:
        try:
            sns_client = Helpers.GetSNSClient()
            sns_client.publish(
                TopicArn = SnsTopicArn,
                Message = Message_,
                Subject=Level
            )
        except Exception as e:
            print(e)

def snowflake_get_obj() -> list:
    items = []
    try:
        query = "CALL FOOTBALL.PUBLIC.get_top_goalscorers()"
        snowflake_connection = Helpers.SnowflakeConnection()
        cursor = snowflake_connection.cursor()
        sql_1 = "USE WAREHOUSE FOOTBALLAPI"
        cursor.execute(sql_1)
        cursor.execute(query)
        goal_scorers = cursor.fetchall()
        for row in goal_scorers:
            items.append([str(row[0]),row[1]])
        snowflake_connection.close()
    except Exception as e:
        Helpers.SendSNSNotification('ERROR in DynamoDB Build | callsite: snowflake_get_obj', str(e))
    finally:
        return items

def put_items(items :list) -> None:
    try:
        client = Helpers.GetDynamoDBClient()
        for item in items:
            date = item[0]
            top_scorers = item[1]
            client.put_item(
                TableName = "top_scorers",
                Item={
                    "Date": {"S": date},
                    "top_scorers": {"S": top_scorers}
                }
            )
    except Exception as e:
        Helpers.SendSNSNotification('ERROR in DynamoDB Build | callsite: put_items', str(e))

def lambda_handler(event, context):
    try:
        items = snowflake_get_obj()
        put_items(items)
    except Exception as e:
        Helpers.SendSNSNotification('ERROR in DynamoDB Build | callsite: lambda_handler', str(e))


    

    