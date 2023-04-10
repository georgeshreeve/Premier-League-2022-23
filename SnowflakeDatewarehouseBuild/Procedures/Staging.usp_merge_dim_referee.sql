CREATE or replace procedure Staging.usp_merge_dim_referee()
returns string not null
language sql
as
declare
    ex_inst string;
    load_datetime timestamp;
    ex_in_cursor cursor for select ExecutionInstance,LOADDATETIME from staging.fixture_flattened GROUP BY ExecutionInstance,LOADDATETIME order by LOADDATETIME;
begin
for record in ex_in_cursor do

ex_inst := record.ExecutionInstance;
load_datetime := record.LOADDATETIME;

create or replace temporary table staging.referee
(
    RefereeName string,
    RefereeNationality string
);

insert into staging.referee
(
    RefereeName,
    RefereeNationality
)
select
    substr(referee,1,charindex(',',referee) - 1),
    substr(referee,charindex(',',referee) + 1)
from staging.fixture_flattened 
where ExecutionInstance = :ex_inst
and LOADDATETIME = :load_datetime;

delete from staging.referee SRC using Star.DimReferee DST where SRC.RefereeName = DST.RefereeName and SRC.RefereeNationality = DST.RefereeNationality;

UPDATE Star.DimReferee DST
SET DST.RefereeNationality = SRC.RefereeNationality,
DST.UpdateDatetime = current_timestamp()
from staging.referee SRC    
where SRC.RefereeName = DST.RefereeName;

insert into Star.DimReferee
(
    RefereeName,
    RefereeNationality
)
select
    RefereeName,
    RefereeNationality
from staging.referee SRC
WHERE NOT exists (select 1 FROM Star.DimReferee DST where SRC.RefereeName = DST.RefereeName);

end for;

return 'Completed staging.usp_merge_dim_referee';

end;