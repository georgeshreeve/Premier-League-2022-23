create or replace procedure Staging.usp_source_sql_players()
returns string not null
language sql 
as
declare
    ex_inst string;
    ex_in_cursor cursor for select excecution_instance from Staging.players_unflattened where processed = 0 order by LOADDATETIME;
begin

for record in ex_in_cursor do

ex_inst := record.excecution_instance;

create or replace temporary table Staging.players (execution_instance string,json_text variant);

insert into Staging.players
(
    execution_instance,
    json_text
)
select
    excecution_instance,
    json_text
from STAGING.players_unflattened
where excecution_instance = :ex_inst;

insert into staging.players_flattened
(
    ExecutionInstance,
    TeamId,
    TeamName,
    PlayerAge,
    PlayerId,
    PlayerName,
    PlayerNumber,
    PlayerPosition
)
select
    s.execution_instance,
    response.value:team:id,
    response.value:team:name,
    players.value:age,
    players.value:id,
    players.value:name,
    players.value:number,
    players.value:position
from players as s,
lateral flatten(input => s.json_text:response) as response,
lateral flatten(input => response.value:players) as players;

update Staging.players_unflattened
set processed = 1
where excecution_instance = :ex_inst;

end for;

return 'Completed Staging.usp_source_sql_players';

end;
