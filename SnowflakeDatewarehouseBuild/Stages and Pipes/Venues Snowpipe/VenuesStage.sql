create stage public.VenuesStage
    url = 's3://football-api-sink-bucket/football-api/Test/Venues'
    storage_integration = football_s3;
    