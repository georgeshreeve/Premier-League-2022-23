create or replace procedure Staging.usp_source_sql_teams()
returns string not null
language sql 
as
declare
    ex_inst string;
    ex_in_cursor cursor for select excecution_instance from Staging.teams_unflattened where processed = 0 order by LOADDATETIME;
begin

for record in ex_in_cursor do

ex_inst := record.excecution_instance;

create or replace temporary table Staging.teams (execution_instance string,json_text variant);

insert into Staging.teams
(
    execution_instance,
    json_text
)
select
    excecution_instance,
    json_text
from STAGING.teams_unflattened
where excecution_instance = :ex_inst;

insert into Staging.teams_flattened
(
    ExecutionInstance,
    TeamCode,
    TeamCountry,
    Founded,
    TeamId,
    TeamName,
    National,
    LeagueId
)
select 
    s.execution_instance,
    response.value:team:code,
    response.value:team:country,
    response.value:team:founded,
    response.value:team:id,
    response.value:team:name,
    response.value:team:national,
    s.json_text:parameters:league
from teams as s,
lateral flatten(input => s.json_text:response) as response;

update Staging.teams_unflattened
set processed = 1
where excecution_instance = :ex_inst;

end for;

return 'Completed Staging.usp_source_sql_teams';

end;