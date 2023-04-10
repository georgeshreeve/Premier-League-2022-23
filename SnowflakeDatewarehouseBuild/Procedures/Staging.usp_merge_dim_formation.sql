create or replace procedure Staging.usp_merge_dim_formation()
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

create or replace temporary table staging.formation
(
    Formation string
);

insert into staging.formation 
(
    Formation
)
select
    Formation
from Staging.fixture_lineups_unflattened
where EXECUTIONINSTANCE = :ex_inst
and LOADDATETIME = :load_datetime
GROUP by Formation;

delete from staging.formation SRC using Star.DimFormation DST
where SRC.Formation = DST.Formation;

insert into Star.DimFormation
(
    Formation
)
select
    Formation
from staging.Formation SRC
where not exists (select 1 from Star.DimFormation DST where SRC.Formation = DST.Formation);

end for;

return 'Completed Staging.usp_merge_dim_formation';

end;