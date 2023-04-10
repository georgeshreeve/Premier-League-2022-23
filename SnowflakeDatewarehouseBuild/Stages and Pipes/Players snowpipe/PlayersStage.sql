create stage public.PlayersStage
    url = 's3://football-api-sink-bucket/football-api/Test/Players'
    storage_integration = football_s3;
    