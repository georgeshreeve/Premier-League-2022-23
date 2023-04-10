create pipe FOOTBALL.public.fixture_test_snowpipe auto_ingest=true as
  copy into FOOTBALL.Staging.fixture_unflattened
  (
    json_text
  )
  from @FOOTBALL.public.TestFixtureStage
  file_format = (type = 'JSON');