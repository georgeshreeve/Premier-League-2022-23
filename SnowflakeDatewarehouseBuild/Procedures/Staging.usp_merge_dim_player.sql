create or replace procedure staging.usp_merge_dim_player()
returns string
language sql
as 
declare
    ex_inst string;
    load_datetime timestamp;
    ex_in_cursor cursor for select EXECUTIONINSTANCE,LOADDATETIME from staging.players_flattened GROUP by EXECUTIONINSTANCE,LOADDATETIME order by LOADDATETIME;
begin
for record in ex_in_cursor do

ex_inst := record.EXECUTIONINSTANCE;
load_datetime := record.LOADDATETIME;

create or replace temporary table staging.players 
(
    PlayerId bigint,
    PlayerName string,
    PlayerNumber int,
    PlayerPosition string,
    TeamId int,
    TeamKey int default(1)
);

insert into staging.players
(
    PlayerId,
    PlayerName,
    PlayerNumber,
    PlayerPosition,
    TeamId
)
select
    PlayerId,
    PlayerName,
    PlayerNumber,
    PlayerPosition,
    TeamId
from staging.players_flattened
where EXECUTIONINSTANCE = :ex_inst
and LOADDATETIME = :load_datetime;

UPDATE Staging.players P
    set P.TeamKey = T.TeamKey
from Star.DimTeam T
where P.TeamId = T.TeamId;

delete from staging.players SRC using Star.DimPlayer DST
where SRC.PlayerId = DST.PlayerId
and SRC.PlayerName = DST.PlayerName
and SRC.PlayerNumber = DST.PlayerNumber
and SRC.PlayerPosition = DST.PlayerPosition
and SRC.TeamKey = DST.TeamKey;

UPDATE Star.DimPlayer DST
    set DST.PlayerName = SRC.PlayerName,
    DST.PlayerNumber = SRC.PlayerNumber,
    DST.PlayerPosition = SRC.PlayerPosition,
    DST.TeamKey = SRC.TeamKey,
    DST.UpdateDatetime = current_timestamp()
from staging.players SRC
where SRC.PlayerId = DST.PlayerId;

insert into Star.DimPlayer
(
    PlayerId,
    PlayerName,
    PlayerNumber,
    PlayerPosition,
    TeamKey
)
select
    PlayerId,
    PlayerName,
    PlayerNumber,
    PlayerPosition,
    TeamKey
from staging.players SRC
where not exists (select 1 from Star.DimPlayer DST where SRC.PlayerId = DST.PlayerId);

delete from staging.players_flattened where EXECUTIONINSTANCE = :ex_inst and LOADDATETIME = :load_datetime;

end for;

return 'Completed staging.usp_merge_dim_player';

end;