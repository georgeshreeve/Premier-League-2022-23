CREATE or replace procedure staging.usp_merge_dim_position()
returns string
language sql
as
declare
    ex_inst string;
    load_datetime timestamp;
    ex_in_cursor cursor for select EXECUTIONINSTANCE,LOADDATETIME from staging.fixture_lineups_unflattened GROUP by EXECUTIONINSTANCE,LOADDATETIME order by LOADDATETIME;
begin
for record in ex_in_cursor do

ex_inst := record.EXECUTIONINSTANCE;
load_datetime := record.LOADDATETIME;

create or replace temporary table staging.position 
(
    PlayerPosition string,
    PlayerGridPosition string
);

insert into staging.position
(
    PlayerPosition,
    PlayerGridPosition
)
select
    coalesce(PlayerPosition,'Unknown'),
    coalesce(PlayerGridPositon,'Sub')
from staging.fixture_lineups_unflattened
where EXECUTIONINSTANCE = :ex_inst
and LOADDATETIME = :load_datetime
GROUP by coalesce(PlayerPosition,'Unknown'),
coalesce(PlayerGridPositon,'Sub');

delete from staging.position SRC using Star.DimPosition DST
where SRC.PlayerPosition = DST.PlayerPosition
and SRC.PlayerGridPosition = DST.PlayerGridPosition;

INSERT into Star.DimPosition
(
    PlayerPosition,
    PlayerGridPosition
)
select
    PlayerPosition,
    PlayerGridPosition
from staging.position SRC
where not exists (select 1 from Star.DimPosition DST
                  where SRC.PlayerPosition = DST.PlayerPosition
                  and SRC.PlayerGridPosition = DST.PlayerGridPosition);

end for;

return 'Completed staging.usp_merge_dim_position';

end;
