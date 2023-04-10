create or replace procedure staging.usp_merge_fact_fixture_lineup()
returns string
language sql
as
declare
    ex_inst string;
    load_datetime timestamp;
    ex_in_cursor cursor for select ExecutionInstance,LoadDatetime from Staging.fixture_lineups_unflattened group by ExecutionInstance,LoadDatetime order by LoadDatetime;
begin
for record in ex_in_cursor do

ex_inst := record.EXECUTIONINSTANCE;
load_datetime := record.LOADDATETIME;

create or replace temporary table staging.fixturelineup 
(
    FixtureId int,
    FixtureKey int,
    TeamId int,
    TeamKey int default(1),
    CoachId int,
    CoachKey int default(1),
    Formation string,
    FormationKey int default(1),
    PlayerId int,
    PlayerKey int default(1),
    PlayerPosition string,
    PlayerGridPosition string,
    PositionKey int default(1),
    StartOrSub string
);

insert into staging.fixturelineup
(
    FixtureId,
    TeamId,
    CoachId,
    Formation,
    PlayerId,
    PlayerPosition,
    PlayerGridPosition,
    StartOrSub
)
select
    FixtureId,
    TeamId,
    CoachId,
    Formation,
    PlayerId,
    PlayerPosition,
    PlayerGridPositon,
    StartOrSub
from Staging.fixture_lineups_unflattened
where ExecutionInstance = :ex_inst
and LoadDatetime = :load_datetime;

update staging.fixturelineup FL
    set FL.FixtureKey = FF.FixtureKey
from Star.FactFixture FF
where FL.FixtureId = FF.FixtureId;

update staging.fixturelineup FL
    set FL.CoachKey = DC.CoachKey
from Star.DimCoach DC 
where FL.CoachId = DC.CoachId;

update staging.fixturelineup FL
    set FL.FormationKey = DF.FormationKey
from Star.DimFormation DF
where FL.Formation = DF.Formation;

update staging.fixturelineup FL
    set FL.TeamKey = DT.TeamKey
from Star.DimTeam DT
where FL.TeamId = DT.TeamId;

UPDATE staging.fixturelineup FL
    set FL.PlayerKey = DP.PlayerKey
from Star.DimPlayer DP
where FL.PlayerId = DP.PlayerId;

UPDATE staging.fixturelineup FL
    set FL.PositionKey = DP.PositionKey
from Star.DimPosition DP
where FL.PlayerPosition = DP.PlayerPosition
and FL.PlayerGridPosition = DP.PlayerGridPosition;

delete from staging.fixturelineup SRC using Star.FactFixtureLineup DST
where SRC.FixtureKey = DST.FixtureKey
and SRC.TeamKey = DST.TeamKey
and SRC.CoachKey = DST.CoachKey
and SRC.PlayerKey = DST.PlayerKey
and SRC.FormationKey = DST.FormationKey
and SRC.PositionKey = DST.PositionKey
and SRC.StartOrSub = DST.StartOrSub;

UPDATE Star.FactFixtureLineup DST
    set DST.StartOrSub = SRC.StartOrSub
    ,DST.UpdateDatetime = current_timestamp()
from staging.fixturelineup SRC
where SRC.FixtureKey = DST.FixtureKey
and SRC.TeamKey = DST.TeamKey
and SRC.CoachKey = DST.CoachKey
and SRC.PlayerKey = DST.PlayerKey
and SRC.FormationKey = DST.FormationKey
and SRC.PositionKey = DST.PositionKey;

insert into Star.FactFixtureLineup
(
    FixtureKey,
    TeamKey,
    CoachKey,
    PlayerKey,
    FormationKey,
    PositionKey,
    StartOrSub
)
select
    FixtureKey,
    TeamKey,
    CoachKey,
    PlayerKey,
    FormationKey,
    PositionKey,
    StartOrSub
from staging.fixturelineup SRC
where not exists (Select 1 from Star.FactFixtureLineup DST
                  where SRC.FixtureKey = DST.FixtureKey
                  and SRC.TeamKey = DST.TeamKey
                  and SRC.CoachKey = DST.CoachKey
                  and SRC.PlayerKey = DST.PlayerKey
                  and SRC.FormationKey = DST.FormationKey
                  and SRC.PositionKey = DST.PositionKey);

delete from staging.fixture_lineups_unflattened where ExecutionInstance = :ex_inst and LoadDatetime = :load_datetime;

end for;

return 'Completed Staging.usp_merge_fact_fixture_lineup';

end;