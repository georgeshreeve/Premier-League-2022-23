create stage TESTFIXTURESTAGE
    url = 's3://football-api-sink-bucket/football-api/Test/FixtureTest'
    storage_integration = football_s3;
    