import json
import requests
import boto3
import datetime
import snowflake.connector
import os
from loguru import logger
import time

logger.add('/var/log/DataPipeline.log',rotation='3 MB',diagnose= False, serialize=True)

RapidAPIKey = os.environ.get("RAPID_API_KEY")
Bucket = os.environ.get("S3_BUCKET")
SnowflakeUser = os.environ.get("SNOWFLAKE_USER")
SnowflakePassword = os.environ.get("SNOWFLAKE_PASS")
SnowflakeAccount = os.environ.get("SNOWFLAKE_ACCOUNT")
AwsAccessKey = os.environ.get("AWS_ACCESS_KEY")
AwsSecretAccessKey =  os.environ.get("AWS_SECRET_ACCESS_KEY")
AwsRegion = os.environ.get("AWS_REGION")
SnSTopicARN = os.environ.get("SNS_TOPIC_ARN")


def SendSNSNotification(Level,Message_):
    try:
        sns_client = Helpers.GetSNSClient()
        response = sns_client.publish(
            TopicArn = SnSTopicARN,
            Message = Message_,
            Subject=Level
        )['MessageId']
        logger.info({
            "Level": "Info",
            "Message": "Send Message to SNS MessageId:{}".format(response)
        })
    except Exception as e:
        logger.error({
            "Level": "Error",
            "Message": "Failed To send Error SNS message {}".format(e)
        })
    


class Event():
    def __init__(self,
                api_headers,
                coach_bucket,
                coach_file_prefix,
                coach_api_url,
                teams_bucket,
                teams_file_prefix,
                teams_api_url,
                teams_params,
                venues_bucket,
                venues_file_prefix,
                venues_api_url,
                venues_parmas,
                players_bucket,
                players_file_prefix,
                players_api_url,
                fixture_list_api_url,
                fixture_list_params,
                fixtures_bucket,
                fixtures_file_prefix,
                fixtures_api_url
                ):
                    self.api_headers = api_headers
                    self.coach_bucket = coach_bucket
                    self.coach_file_prefix = coach_file_prefix
                    self.coach_api_url = coach_api_url
                    self.teams_bucket = teams_bucket
                    self.teams_file_prefix = teams_file_prefix
                    self.teams_api_url = teams_api_url
                    self.teams_params = teams_params
                    self.venues_bucket = venues_bucket
                    self.venues_file_prefix = venues_file_prefix
                    self.venues_api_url = venues_api_url
                    self.venues_params = venues_parmas
                    self.players_bucket = players_bucket
                    self.players_file_prefix = players_file_prefix
                    self.players_api_url = players_api_url
                    self.fixture_list_api_url = fixture_list_api_url
                    self.fixture_list_params = fixture_list_params
                    self.fixtures_bucket = fixtures_bucket
                    self.fixtures_file_prefix = fixtures_file_prefix
                    self.fixtures_api_url = fixtures_api_url

class Helpers():
    def GetS3Client():
        s3_client = boto3.client('s3',aws_access_key_id=AwsAccessKey, aws_secret_access_key=AwsSecretAccessKey,region_name=AwsRegion)
        return s3_client

    def GetSNSClient():
        sns_client = boto3.client('sns',aws_access_key_id=AwsAccessKey, aws_secret_access_key=AwsSecretAccessKey,region_name=AwsRegion)
        return sns_client
      
    def SnowflakeConnection():
       connection = snowflake.connector.connect(
           user = SnowflakeUser,
           password = SnowflakePassword,
           account = SnowflakeAccount
       )
       return connection
    

def Venues(payload):
    bucket = payload.venues_bucket
    url = payload.venues_api_url
    params = payload.venues_params
    headers = payload.api_headers
    folder_prefix = payload.venues_file_prefix
    key = folder_prefix + "venues_" + str(datetime.datetime.now().strftime("%Y-%m-%d-%H-%M")) + ".json"
    
    try:
        response = requests.get(url = url, headers = headers, params = params)
        if int(response.headers['X-RateLimit-requests-Remaining']) < 5:
            sleepTime = int(response.headers['X-RateLimit-requests-Reset'])
            time.sleep(sleepTime)

        if response.status_code == 200:
            s3_client = Helpers.GetS3Client()
            s3_client.put_object(Bucket=bucket,
                             Key= key,
                             Body=json.dumps(response.json()))   
            return 'Success'
        else:
            logger.error({
                "Level": "Error",
                "Message": "Non 200 status code for url: {} {}".format(url,response.reason)
            })
            SendSNSNotification("Error","Non 200 status code for url: {} {}".format(url,response.reason))
            return 'Error'
    except Exception as e:
        logger.error({
            "Level": "Error",
            "Message": e
        })
        SendSNSNotification("Error",e)

def Teams(payload,venues_result):
    try:
        if venues_result == 'Success':
            bucket = payload.teams_bucket
            url = payload.teams_api_url
            params = payload.teams_params
            headers = payload.api_headers
            folder_prefix = payload.teams_file_prefix
            key = folder_prefix + "teams_" + str(datetime.datetime.now().strftime("%Y-%m-%d-%H-%M")) + ".json"

            response = requests.get(url = url, headers = headers, params = params)
            if int(response.headers['X-RateLimit-requests-Remaining']) < 5:
                sleepTime = int(response.headers['X-RateLimit-requests-Reset'])
                time.sleep(sleepTime)

            if response.status_code == 200:
                s3_client = Helpers.GetS3Client()
                s3_client.put_object(Bucket=bucket,
                                 Key= key,
                                 Body=json.dumps(response.json()))    
                return response.json()
            else:
                logger.error({
                "Level": "Error",
                "Message": "Non 200 status code for url: {} {}".format(url,response.reason)
                })
                SendSNSNotification("Error","Non 200 status code for url: {} {}".format(url,response.reason))
                return 'Error'
        else:
            return 'Error'
    except Exception as e:
        logger.error({
            "Level": "Error",
            "Message": e
        })
        SendSNSNotification("Error",e)
    
def Coachs(payload,teams_response):
    try:  
        if teams_response != 'Error':
            bucket = payload.coach_bucket
            url = payload.coach_api_url
            headers = payload.api_headers
            folder_prefix = payload.coach_file_prefix

            team_ids = []
            for element in teams_response['response']:
                team_ids.append(element['team']['id'])

            s3_client = Helpers.GetS3Client()

            for team_id in team_ids:
                params = {"team":str(team_id)}
                key = folder_prefix + "Coachs_" + str(team_id) + "_" + str(datetime.datetime.now().strftime("%Y-%m-%d-%H-%M")) + ".json"
                response = requests.get(url = url, headers = headers, params = params)
                if int(response.headers['X-RateLimit-requests-Remaining']) < 5:
                    sleepTime = int(response.headers['X-RateLimit-requests-Reset'])
                    time.sleep(sleepTime)

                if response.status_code == 200:
                    s3_client.put_object(Bucket=bucket,
                                     Key= key,
                                     Body=json.dumps(response.json()))
                else:
                    logger.error({
                        "Level": "Error",
                        "Message": "Non 200 status code for url: {} {}".format(url,response.reason)
                    })
                    SendSNSNotification("Error","Non 200 status code for url: {} {}".format(url,response.reason))
                    break
            if response.status_code == 200:
                return team_ids
            else:
                logger.error({
                        "Level": "Error",
                        "Message": "Non 200 status code for url: {} {}".format(url,response.reason)
                    })
                SendSNSNotification("Error","Non 200 status code for url: {} {}".format(url,response.reason))
                return 'Error'
        else:
            return 'Error'
    except Exception as e:
        logger.error({
            "Level": "Error",
            "Message": e
        })
        SendSNSNotification("Error",e)

def Players(payload,team_ids):
    try:
        if team_ids != 'Error':
            bucket = payload.players_bucket
            url = payload.players_api_url
            headers = payload.api_headers
            folder_prefix = payload.players_file_prefix

            s3_client = Helpers.GetS3Client()

            for team_id in team_ids:
                params = {"team":str(team_id)}
                key = folder_prefix + "Players_" + str(team_id) + "_" + str(datetime.datetime.now().strftime("%Y-%m-%d-%H-%M")) + ".json"

                response = requests.get(url = url, headers = headers, params = params)
                if int(response.headers['X-RateLimit-requests-Remaining']) < 5:
                    sleepTime = int(response.headers['X-RateLimit-requests-Reset'])
                    time.sleep(sleepTime)

                if response.status_code == 200:
                    s3_client.put_object(Bucket=bucket,
                                     Key= key,
                                     Body=json.dumps(response.json()))
                else:
                    logger.error({
                        "Level": "Error",
                        "Message": "Non 200 status code for url: {} {}".format(url,response.reason)
                    })
                    SendSNSNotification("Error","Non 200 status code for url: {} {}".format(url,response.reason))
                    break
            if response.status_code == 200:
                return 'Success'
            else:
                logger.error({
                        "Level": "Error",
                        "Message": "Non 200 status code for url: {} {}".format(url,response.reason)
                    })
                SendSNSNotification("Error","Non 200 status code for url: {} {}".format(url,response.reason))
                return 'Error'
        else:
            return 'Error'
    except Exception as e:
        logger.error({
            "Level": "Error",
            "Message": e
        })
        SendSNSNotification("Error",e)

def Fixtures(payload,coach_result):
    try:
        if coach_result == 'Success':

            current_fixtures = []
            snowflake_connection = Helpers.SnowflakeConnection()
            cursor = snowflake_connection.cursor()
            sql_1 = "USE WAREHOUSE FOOTBALLAPI"
            cursor.execute(sql_1)
            sql_2 = "SELECT FIXTUREID FROM FOOTBALL.STAR.FACTFIXTURE"
            cursor.execute(sql_2)
            fixtures = cursor.fetchall()
            for row in fixtures:
                current_fixtures.append(row[0])

            fixture_list_url = payload.fixture_list_api_url
            headers = payload.api_headers
            fixture_list_params = payload.fixture_list_params
            fixtures_url = payload.fixtures_api_url
            bucket = payload.fixtures_bucket
            folder_prefix = payload.fixtures_file_prefix

            fixture_list_response = requests.get(url = fixture_list_url, headers = headers, params = fixture_list_params)
            if int(fixture_list_response.headers['X-RateLimit-requests-Remaining']) < 5:
                sleepTime = int(fixture_list_response.headers['X-RateLimit-requests-Reset'])
                time.sleep(sleepTime)

            if fixture_list_response.status_code == 200:
                fixture_list = []
                chunked_fixture_list = []
                chunk_size = 20

                for fixture in fixture_list_response.json()['response']:
                    fixture_list.append(fixture['fixture']['id'])

                fixture_list = list(set(fixture_list).difference(current_fixtures))

                for i in range(0,len(fixture_list),chunk_size):
                    chunked_fixture_list.append(fixture_list[i:i+chunk_size])

                count = 1
                for fixtures in chunked_fixture_list:
                    for i in range(len(fixtures)):
                        fixtures[i] = str(fixtures[i])
                    fixtures_params = {"ids":'-'.join(fixtures)}

                    fixtures_response = requests.get(url = fixtures_url, headers = headers, params = fixtures_params)
                    if int(fixtures_response.headers['X-RateLimit-requests-Remaining']) < 5:
                        sleepTime = int(fixtures_response.headers['X-RateLimit-requests-Reset'])
                        time.sleep(sleepTime)

                    if fixtures_response.status_code == 200:
                        key = folder_prefix + "Fixtures_" + str(datetime.datetime.now().strftime("%Y-%m-%d-%H-%M")) + "_" + str(count) + ".json"

                        s3_client = Helpers.GetS3Client()
                        s3_client.put_object(Bucket=bucket,
                                         Key= key,
                                         Body=json.dumps(fixtures_response.json()))

                        count = count + 1
                    else:
                        logger.error({
                            "Level": "Error",
                            "Message": "Non 200 status code for url: {} {}".format(fixtures_url,fixtures_response.reason)
                        })
                        SendSNSNotification("Error","Non 200 status code for url: {} {}".format(fixtures_url,fixtures_response.reason))
                        break
            else:
                logger.error({
                    "Level": "Error",
                    "Message": "Non 200 status code for url: {} {}".format(fixture_list_url,fixture_list_response.reason)
                })
                SendSNSNotification("Error","Non 200 status code for url: {} {}".format(fixture_list_url,fixture_list_response.reason))

        else:
            pass
    except Exception as e:
        logger.error({
            "Level": "Error",
            "Message": e
        })
        SendSNSNotification("Error",e)



def main():
    logger.info({
        "Level": "Info",
        "Message": "Application Start"
    })
    payload = Event(
        {
        "X-RapidAPI-Key": RapidAPIKey,
        "X-RapidAPI-Host": "api-football-v1.p.rapidapi.com"
        },
        Bucket,
        "football-api/Test/Coachs/",
        "https://api-football-v1.p.rapidapi.com/v3/coachs",
        Bucket,
        "football-api/Test/Teams/",
        "https://api-football-v1.p.rapidapi.com/v3/teams",
        {"league":"39","season":"2022"},
        Bucket,
        "football-api/Test/Venues/",
        "https://api-football-v1.p.rapidapi.com/v3/venues",
        {"country":"England"},
        Bucket,
        "football-api/Test/Players/",
        "https://api-football-v1.p.rapidapi.com/v3/players/squads",
        "https://api-football-v1.p.rapidapi.com/v3/fixtures",
        {"league":"39","season":"2022","status":"FT"},
        Bucket,
        "football-api/Test/FixtureTest/",
        "https://api-football-v1.p.rapidapi.com/v3/fixtures"
    )
    venues_result = Venues(payload)
    teams_response = Teams(payload,venues_result)
    team_ids = Coachs(payload,teams_response)
    coach_result = Players(payload,team_ids)
    Fixtures(payload,coach_result)
    logger.info({
        "Level": "Info",
        "Message": "Application End"
    })

if __name__ == "__main__":
    main()