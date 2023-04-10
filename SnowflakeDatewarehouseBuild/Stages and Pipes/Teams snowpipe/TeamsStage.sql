create stage public.TeamsStage
    url = 's3://football-api-sink-bucket/football-api/Test/Teams'
    storage_integration = football_s3;
    