create or replace api integration api_notification
    api_provider= aws_api_gateway
    api_aws_role_arn = 'arn:aws:iam::460327182513:role/snowflake_external_function_user'
    enabled = true
    api_allowed_prefixes = ('https://i4vbu04nzf.execute-api.eu-west-2.amazonaws.com/v1/');

create or replace external function dynamo_db_build(a nvarchar)
    returns variant
    api_integration = api_notification
    as 'https://i4vbu04nzf.execute-api.eu-west-2.amazonaws.com/v1/buildreportinglayer'