create or replace procedure staging.usp_merge_fact_fixture_team_statistics()
returns string
language sql
as
declare
    ex_inst string;
    load_datetime timestamp;
    ex_in_cursor cursor for select ExecutionInstance,LoadDatetime from staging.fixture_team_statistics group by ExecutionInstance,LoadDatetime order by LoadDatetime;
begin
for record in ex_in_cursor do

ex_inst := record.EXECUTIONINSTANCE;
load_datetime := record.LOADDATETIME;

create or replace temporary table staging.fixtureteamstatistics 
(
    FixtureId int,
    FixtureKey int,
    TeamId int,
    TeamKey int default(1),
    StatisticType string,
    StatisticTypeKey int default(1),
    StatisticValue string
);

insert into staging.fixtureteamstatistics
(
    FixtureId,
    TeamId,
    StatisticType,
    StatisticValue
)
select
    FixtureId,
    TeamId,
    Type,
    value
from Staging.fixture_team_statistics
where ExecutionInstance = :ex_inst
and LoadDatetime = :load_datetime;

update staging.fixtureteamstatistics FTS
    set FTS.FixtureKey = FF.FixtureKey
from Star.FactFixture FF
where FTS.FixtureId = FF.FixtureId;

update staging.fixtureteamstatistics FTS
    set FTS.TeamKey = DT.TeamKey
from Star.DimTeam DT
where FTS.TeamId = DT.TeamId;

update staging.fixtureteamstatistics FTS
    set FTS.StatisticTypeKey = DST.StatisticTypeKey
from Star.DimStatisticType DST
where FTS.StatisticType = DST.StatisticType;


delete from staging.fixtureteamstatistics SRC using Star.FactFixtureTeamStatistics DST
where SRC.FixtureKey = DST.FixtureKey
and SRC.TeamKey = DST.TeamKey
and SRC.StatisticTypeKey = DST.StatisticTypeKey
and SRC.StatisticValue = DST.StatisticValue;

update Star.FactFixtureTeamStatistics DST
    set DST.StatisticValue = SRC.StatisticValue
    ,DST.UpdateDatetime = current_timestamp()
from staging.fixtureteamstatistics SRC
where SRC.FixtureKey = DST.FixtureKey
and SRC.TeamKey = DST.TeamKey
and SRC.StatisticTypeKey = DST.StatisticTypeKey;

insert into Star.FactFixtureTeamStatistics
(
    FixtureKey,
    TeamKey,
    StatisticTypeKey,
    StatisticValue
)
select
    FixtureKey,
    TeamKey,
    StatisticTypeKey,
    StatisticValue
from staging.fixtureteamstatistics SRC
where not exists (select 1 from Star.FactFixtureTeamStatistics DST
                  where SRC.FixtureKey = DST.FixtureKey
                  and SRC.TeamKey = DST.TeamKey
                  and SRC.StatisticTypeKey = DST.StatisticTypeKey);

delete from staging.fixture_team_statistics where ExecutionInstance = :ex_inst and LoadDatetime = :load_datetime;

end for;

return 'Completed staging.usp_merge_fact_fixture_team_statistics';

end;