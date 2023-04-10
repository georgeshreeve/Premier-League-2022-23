create or replace table FOOTBALL.STAGING.venue_unflattened
(
    excecution_instance string default(uuid_string()),
    json_text variant,
    processed int default(0),
    LoadDatetime timestamp default(current_timestamp())
)

