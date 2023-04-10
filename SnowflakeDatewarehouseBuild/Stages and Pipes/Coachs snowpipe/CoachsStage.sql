create stage public.CoachsStage
    url = 's3://football-api-sink-bucket/football-api/Test/Coachs'
    storage_integration = football_s3;
    