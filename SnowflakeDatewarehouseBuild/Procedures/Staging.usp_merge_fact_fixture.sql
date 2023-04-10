create or replace procedure staging.usp_merge_fact_fixture()
returns string
language sql
as
declare
    ex_inst string;
    load_datetime timestamp;
    ex_in_cursor cursor for select ExecutionInstance,LoadDatetime from staging.fixture_flattened GROUP by ExecutionInstance,LoadDatetime order by LoadDatetime;
begin
for record in ex_in_cursor do

ex_inst := record.ExecutionInstance;
load_datetime := record.LoadDatetime;

CREATE or replace temporary table staging.fixture 
(
    FixtureId bigint,
    FixtureDate datetime,
    LeagueCountry string,
    LeagueName string,
    LeagueRound string,
    LeagueSeason int,
    LeagueRoundKey int default(1),
    Referee string,
    RefereeKey int default(1),
    VenueId bigint,
    VenueKey int default(1),
    HomeTeamId int,
    HomeTeamKey int default(1),
    AwayTeamId int,
    AwayTeamKey int default(1),
    Status string,
    TimeZone string,
    HalftimeHomeGoals int,
    HalftimeAwayGoals int,
    FulltimeHomeGoals int,
    FulltimeAwayGoals int,
    ExtratimeHomeGoals int,
    ExtratimeAwayGoals int,
    PenaltiesHomeGoals int,
    PenaltiesAwayGoals int,
    HomeTeamWinner boolean,
    AwayTeamWinner boolean
);

insert into staging.fixture
(
    FixtureId,
    FixtureDate,
    LeagueCountry,
    LeagueName,
    LeagueRound,
    LeagueSeason,
    Referee,
    VenueId,
    HomeTeamId,
    AwayTeamId,
    Status,
    TimeZone,
    HalftimeHomeGoals,
    HalftimeAwayGoals,
    FulltimeHomeGoals,
    FulltimeAwayGoals,
    ExtratimeHomeGoals,
    ExtratimeAwayGoals,
    PenaltiesHomeGoals,
    PenaltiesAwayGoals,
    HomeTeamWinner,
    AwayTeamWinner
)
select
    FixtureId,
    FixtureDate,
    LeagueCountry,
    LeagueName,
    LeagueRound,
    LeagueSeason,
    Referee,
    VenueId,
    HomeTeamId,
    AwayTeamId,
    StatusShort,
    Timezone,
    coalesce(HalftimeHomeGoals,0),
    coalesce(HalftimeAwayGoals,0),
    coalesce(FulltimeHomeGoals,0),
    coalesce(FulltimeAwayGoals,0),
    coalesce(ExtratimeHomeGoals,0),
    coalesce(ExtratimeAwayGoals,0),
    coalesce(PenaltyHomeGoals,0),
    coalesce(PenaltyAwayGoals,0),
    to_boolean(HomeTeamWinner),
    to_boolean(AwayTeamWinner)
from staging.fixture_flattened
where ExecutionInstance = :ex_inst
and LoadDatetime = :load_datetime;

update staging.fixture F
    SET F.LeagueRoundKey = DLR.LeagueRoundKey
from Star.DimLeagueRound DLR
where F.LeagueName = DLR.LeagueName
and F.LeagueCountry = DLR.LeagueCountry
and F.LeagueSeason = DLR.LeagueSeason
and F.LeagueRound = DLR.LeagueRound;

update staging.fixture F 
    set F.RefereeKey = DR.RefereeKey
from Star.DimReferee DR 
where substr(F.Referee,1,charindex(',',F.Referee) - 1) = DR.RefereeName;

update staging.fixture F 
    set F.VenueKey = DV.VenueKey
from Star.DimVenue DV 
where F.VenueId = DV.VenueId;

update staging.fixture F
    set F.HomeTeamKey = DT.TeamKey
from Star.DimTeam DT 
where F.HomeTeamId = DT.TeamId;

update staging.fixture F 
    set F.AwayTeamKey = DT.TeamKey
from Star.DimTeam DT 
where F.AwayTeamId = DT.TeamId;

delete from staging.fixture SRC using Star.FactFixture DST
where SRC.FixtureId = DST.FixtureId
and SRC.FixtureDate = DST.FixtureDate
and SRC.LeagueRoundKey = DST.LeagueRoundKey
and SRC.RefereeKey = DST.RefereeKey
and SRC.VenueKey = DST.VenueKey
and SRC.HomeTeamKey = DST.HomeTeamKey
and SRC.AwayTeamKey = DST.AwayTeamKey
and SRC.Status = DST.Status
and SRC.TimeZone = DST.TimeZone
and SRC.HalftimeHomeGoals = DST.HalftimeHomeGoals
and SRC.HalftimeAwayGoals = DST.HalftimeAwayGoals
and SRC.FulltimeHomeGoals = DST.FulltimeHomeGoals
and SRC.FulltimeAwayGoals = DST.FulltimeAwayGoals
and SRC.ExtratimeHomeGoals = DST.ExtratimeHomeGoals
and SRC.ExtratimeAwayGoals = DST.ExtratimeAwayGoals
and SRC.PenaltiesHomeGoals = DST.PenaltiesHomeGoals
and SRC.PenaltiesAwayGoals = DST.PenaltiesAwayGoals
and SRC.HomeTeamWinner = DST.HomeTeamWinner
and SRC.AwayTeamWinner = DST.AwayTeamWinner;

update Star.FactFixture DST 
set  DST.FixtureDate = SRC.FixtureDate
,DST.LeagueRoundKey = SRC.LeagueRoundKey
,DST.RefereeKey = SRC.RefereeKey
,DST.VenueKey = SRC.VenueKey
,DST.HomeTeamKey = SRC.HomeTeamKey
,DST.AwayTeamKey = SRC.AwayTeamKey
,DST.Status = SRC.Status
,DST.TimeZone = SRC.TimeZone
,DST.HalftimeHomeGoals = SRC.HalftimeHomeGoals
,DST.HalftimeAwayGoals = SRC.HalftimeAwayGoals
,DST.FulltimeHomeGoals = SRC.FulltimeHomeGoals
,DST.FulltimeAwayGoals = SRC.FulltimeAwayGoals
,DST.ExtratimeHomeGoals = SRC.ExtratimeHomeGoals
,DST.ExtratimeAwayGoals = SRC.ExtratimeAwayGoals
,DST.PenaltiesHomeGoals = SRC.PenaltiesHomeGoals
,DST.PenaltiesAwayGoals = SRC.PenaltiesAwayGoals
,DST.HomeTeamWinner = SRC.HomeTeamWinner
,DST.AwayTeamWinner = SRC.AwayTeamWinner
,DST.UpdateDatetime = current_timestamp()
from staging.fixture SRC 
where SRC.FixtureId = DST.FixtureId;

insert into Star.FactFixture
(
    FixtureId,
    FixtureDate,
    LeagueRoundKey,
    RefereeKey,
    VenueKey,
    HomeTeamKey,
    AwayTeamKey,
    Status,
    TimeZone,
    HalftimeHomeGoals,
    HalftimeAwayGoals,
    FulltimeHomeGoals,
    FulltimeAwayGoals,
    ExtratimeHomeGoals,
    ExtratimeAwayGoals,
    PenaltiesHomeGoals,
    PenaltiesAwayGoals,
    HomeTeamWinner,
    AwayTeamWinner
)
select
    FixtureId,
    FixtureDate,
    LeagueRoundKey,
    RefereeKey,
    VenueKey,
    HomeTeamKey,
    AwayTeamKey,
    Status,
    TimeZone,
    HalftimeHomeGoals,
    HalftimeAwayGoals,
    FulltimeHomeGoals,
    FulltimeAwayGoals,
    ExtratimeHomeGoals,
    ExtratimeAwayGoals,
    PenaltiesHomeGoals,
    PenaltiesAwayGoals,
    HomeTeamWinner,
    AwayTeamWinner
from staging.fixture SRC
where not exists (select 1 from Star.FactFixture DST where SRC.FixtureId = DST.FixtureId);

delete from staging.fixture_flattened where ExecutionInstance = :ex_inst and LoadDatetime = :load_datetime;

end for;

return 'Completed Staging.usp_merge_fact_fixture';

end;