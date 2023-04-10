create pipe FOOTBALL.public.coachs_snowpipe auto_ingest=true as
  copy into FOOTBALL.STAGING.coachs_unflattened
  (
    json_text
  )
  from @FOOTBALL.public.CoachsStage
  file_format = (type = 'JSON');