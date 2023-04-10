create pipe FOOTBALL.public.venues_snowpipe auto_ingest=true as
  copy into FOOTBALL.Staging.venue_unflattened
  (
    json_text
  )
  from @FOOTBALL.public.VenuesStage
  file_format = (type = 'JSON');