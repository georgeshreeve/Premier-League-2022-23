import json
import boto3 

def lambda_handler(event, context):

    sns_topics=[
        {
            "Build": "top_scorers",
            "sns_topic": "arn:aws:sns:eu-west-2:460327182513:FootballDynamoDBBuildTopScorers"
        },
        {
            "Build": "league_table",
            "sns_topic": "arn:aws:sns:eu-west-2:460327182513:FootballDynamoDBBuildLeagueTable"
        }
    ]

    
    array_to_return = []
    try:
        event_body = event["body"]
        payload = json.loads(event_body)
        rows = payload["data"]
        
        for row in rows:
            row_number = row[0]
            input_value_1 = row[1]
            output_value = ["Message:", input_value_1]
            row_to_return = [row_number, output_value]
            array_to_return.append(row_to_return)
            
        client = boto3.client('sns')
        for obj in sns_topics:
            message = obj['Build']
            arn = obj['sns_topic']
            client.publish(
                TopicArn = arn,
                Message = message
            )
            
        status_code = 200
        return_body = json.dumps({"data" : array_to_return})

    except Exception as e:
        status_code = 400
        array_to_return.append(['Message:','Error {}'.format(str(e))])
        return_body = json.dumps({"data" : array_to_return})
    return {
        'statusCode': status_code,
        'body': return_body
    }