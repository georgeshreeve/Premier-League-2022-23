create or replace procedure Staging.usp_source_sql_fixtures()
returns string not null
language sql 
as
declare
    ex_inst string;
    ex_in_cursor cursor for select excecution_instance from Staging.fixture_unflattened where processed = 0 order by LOADDATETIME;
begin

for record in ex_in_cursor do

ex_inst := record.excecution_instance;

create or replace temporary table Staging.fixture (execution_instance string,json_text variant);

insert into Staging.fixture
(
    execution_instance,
    json_text
)
select
    excecution_instance,
    json_text
from STAGING.fixture_unflattened
where excecution_instance = :ex_inst;

insert into Staging.fixture_flattened
(
    ExecutionInstance,
    FixtureId,
    FixtureDate,
    Referee,
    StatusShort,
    Timezone,
    VenueCity,
    VenueId,
    VenueName,
    AwayGoals,
    HomeGoals,
    LeagueCountry,
    LeagueName,
    LeagueRound,
    LeagueSeason,
    ExtratimeAwayGoals,
    ExtratimeHomeGoals,
    FulltimeAwayGoals,
    FulltimeHomeGoals,
    HalftimeAwayGoals,
    HalftimeHomeGoals,
    PenaltyHomeGoals,
    PenaltyAwayGoals,
    AwayTeamId,
    AwayTeamName,
    AwayTeamWinner,
    HomeTeamId,
    HomeTeamName,
    HomeTeamWinner
)
select
    f.execution_instance,    
    r.value:fixture:id,
    r.value:fixture:date,
    r.value:fixture:referee,
    r.value:fixture:status:short,
    r.value:fixture:timezone,
    r.value:fixture:venue:city,
    r.value:fixture:venue:id,
    r.value:fixture:venue:name,
    r.value:goals:away,
    r.value:goals:home,
    r.value:league:country,
    r.value:league:name,
    r.value:league:round,
    r.value:league:season,
    r.value:score:extratime:away,
    r.value:score:extratime:home,
    r.value:score:fulltime:away,
    r.value:score:fulltime:home,
    r.value:score:halftime:away,
    r.value:score:halftime:home,
    r.value:score:penalty:away,
    r.value:score:penalty:home,
    r.value:teams:away:id,
    r.value:teams:away:name,
    r.value:teams:away:winner,
    r.value:teams:home:id,
    r.value:teams:home:name,
    r.value:teams:home:winner
from fixture as f,
lateral flatten(input => f.json_text:response) as r;

insert into Staging.fixture_team_statistics
(
    ExecutionInstance,
    FixtureId,
    TeamId,
    TeamName,
    Type,
    value
)
SELECT
    s.execution_instance,
    response.value:fixture:id,
    statisticsouter.value:team:id,
    statisticsouter.value:team:name,
    statisticsinner.value:type,
    statisticsinner.value:value
from fixture as s,
lateral flatten(input => s.json_text:response) as response,
lateral flatten(input => response.value:statistics) as statisticsouter,
lateral flatten(input => statisticsouter.value:statistics) as statisticsinner;

insert into Staging.fixture_players_statistics_unflattened
(
    ExecutionInstance,
    FixtureId,
    PlayerId,
    TeamId,
    TeamName,
    PlayerName,
    CardsRed,
    CardsYellow,
    DribbleAttempts,
    DribblePast,
    DribbleSuccess,
    DuelsTotal,
    DuelsWon,
    FoulsCommited,
    FoulsDrawn,
    Captain,
    MinutesPlayed,
    Number,
    Position,
    Rating,
    Substitute,
    GoalAssists,
    GoalsConceded,
    GoalSaves,
    GoalsTotal,
    Offsides,
    PassAccuracy,
    KeyPasses,
    TotalPass,
    PenaltiesCommited,
    PenaltiesMissed,
    PenaltiesSaved,
    PenaltiesScored,
    PenaltiesWon,
    ShotsOnTarget,
    ShotsTotal,
    Blocks,
    Interceptions,
    TotalTackles
)
SELECT
    s.execution_instance,
    response.value:fixture:id,
    playersinner.value:player:id,
    playersouter.value:team:id,
    playersouter.value:team:name,
    playersinner.value:player:name,
    playerstatistics.value:cards:red,
    playerstatistics.value:cards:yellow,
    playerstatistics.value:dribbles:attempts,
    playerstatistics.value:dribbles:past,
    playerstatistics.value:dribbles:success,
    playerstatistics.value:duels:total,
    playerstatistics.value:duels:won,
    playerstatistics.value:fouls:committed,
    playerstatistics.value:fouls:drawn,
    playerstatistics.value:games:captain,
    playerstatistics.value:games:minutes,
    playerstatistics.value:games:number,
    playerstatistics.value:games:position,
    playerstatistics.value:games:rating,
    playerstatistics.value:games:substitute,
    playerstatistics.value:goals:assists,
    playerstatistics.value:goals:conceded,
    playerstatistics.value:goals:saves,
    playerstatistics.value:goals:total,
    playerstatistics.value:offsides,
    playerstatistics.value:passes:accuracy,
    playerstatistics.value:passes:key,
    playerstatistics.value:passes:total,
    playerstatistics.value:penalty:commited,
    playerstatistics.value:penalty:missed,
    playerstatistics.value:penalty:saved,
    playerstatistics.value:penalty:scored,
    playerstatistics.value:penalty:won,
    playerstatistics.value:shots:on,
    playerstatistics.value:shots:total,
    playerstatistics.value:tackles:blocks,
    playerstatistics.value:tackles:interceptions,
    playerstatistics.value:tackles:total
from fixture s,
lateral flatten(input => s.json_text:response) as response,
lateral flatten(input => response.value:players) as playersouter,
lateral flatten(input => playersouter.value:players) as playersinner,
lateral flatten(input => playersinner.value:statistics) as playerstatistics;


insert into Staging.fixture_events_unflattened
(
    ExecutionInstance,
    FixtureId,
    AssistId,
    AssistName,
    Comments,
    Detail,
    PlayerId,
    PlayerName,
    TeamId,
    TeamName,
    TimeElapsed,
    TimeExtra,
    EventType
)
SELECT
 s.execution_instance,
 response.value:fixture:id,
 events.value:assist:id,
 events.value:assist:name,
 events.value:comments,
 events.value:detail,
 events.value:player:id,
 events.value:player:name,
 events.value:team:id,
 events.value:team:name,
 events.value:time:elapsed,
 events.value:time:extra,
 events.value:type
from fixture as s,
lateral flatten(input => s.json_text:response) as response,
lateral flatten (input => response.value:events) as events;

insert into Staging.fixture_lineups_unflattened
(
    ExecutionInstance,
    FixtureId,
    TeamId,
    TeamName,
    CoachId,
    CoachName,
    Formation,
    StartOrSub,
    PlayerGridPositon,
    PlayerId,
    PlayerName,
    PlayerNumber,
    PlayerPosition
)
SELECT
    s.execution_instance,
    response.value:fixture:id,
    lineups.value:team:id,
    lineups.value:team:name,
    lineups.value:coach:id,
    lineups.value:coach:name,
    lineups.value:formation,
    'Start',
    startXI.value:player:grid,
    startXI.value:player:id,
    startXI.value:player:name,
    startXI.value:player:number,
    startXI.value:player:pos
from fixture as s,
lateral flatten(input => s.json_text:response) as response,
lateral flatten (input => response.value:lineups) as lineups,
lateral flatten (input => lineups.value:startXI) as startXI

union 

SELECT
    s.execution_instance,
    response.value:fixture:id,
    lineups.value:team:id,
    lineups.value:team:name,
    lineups.value:coach:id,
    lineups.value:coach:name,
    lineups.value:formation,
    'Sub',
    substitutes.value:player:grid,
    substitutes.value:player:id,
    substitutes.value:player:name,
    substitutes.value:player:number,
    substitutes.value:player:pos
from fixture as s,
lateral flatten(input => s.json_text:response) as response,
lateral flatten (input => response.value:lineups) as lineups,
lateral flatten (input => lineups.value:substitutes) as substitutes;

update Staging.fixture_unflattened
set processed = 1 
where excecution_instance = :ex_inst;

end for;

return 'Completed Staging.usp_source_sql_fixtures';
end;
