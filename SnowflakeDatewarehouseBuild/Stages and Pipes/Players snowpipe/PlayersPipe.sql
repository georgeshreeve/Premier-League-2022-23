create pipe FOOTBALL.public.players_snowpipe auto_ingest=true as
  copy into FOOTBALL.STAGING.players_unflattened
  (
    json_text
  )
  from @FOOTBALL.public.PlayersStage
  file_format = (type = 'JSON');