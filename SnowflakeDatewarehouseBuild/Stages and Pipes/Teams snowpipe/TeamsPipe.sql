create pipe FOOTBALL.public.teams_snowpipe auto_ingest=true as
  copy into FOOTBALL.STAGING.teams_unflattened
  (
    json_text
  )
  from @FOOTBALL.public.TeamsStage
  file_format = (type = 'JSON');