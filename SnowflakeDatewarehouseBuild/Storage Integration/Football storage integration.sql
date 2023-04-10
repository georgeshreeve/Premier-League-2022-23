use role accountadmin;

CREATE STORAGE INTEGRATION football_s3
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::460327182513:role/snowflakerole'
  STORAGE_ALLOWED_LOCATIONS = ('s3://football-api-sink-bucket/football-api/Test')
  