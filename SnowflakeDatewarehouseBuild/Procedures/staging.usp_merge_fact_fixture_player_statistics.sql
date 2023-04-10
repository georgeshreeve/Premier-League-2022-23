create or replace procedure staging.usp_merge_fact_fixture_player_statistics()
returns string
language sql
as
declare
    ex_inst string;
    load_datetime timestamp;
    ex_in_cursor cursor for select ExecutionInstance,LoadDatetime from Staging.fixture_players_statistics_unflattened group by ExecutionInstance,LoadDatetime order by LoadDatetime;
begin
for record in ex_in_cursor do

ex_inst := record.ExecutionInstance;
load_datetime := record.LoadDatetime;

create or replace temporary table staging.fixtureplayerstatistics 
(
    FixtureId int,
    FixtureKey int,
    PlayerId int,
    PlayerKey int default(1),
    TeamId int,
    TeamKey int default(1),
    Position string,
    PositionKey int default(1),
    CardsRed int,
    CardsYellow int,
    DribbleAttempts int,
    DribblePast int,
    DribbleSuccess int,
    DuelsTotal int,
    DuelsWon int,
    FoulsCommited int,
    FoulsDrawn int,
    Captain boolean,
    MinutesPlayed int,
    Rating numeric,
    Substitute boolean,
    GoalAssists int,
    GoalsConceded int,
    GoalSaves int,
    GoalsTotal int,
    Offsides int,
    PassAccuracy int,
    KeyPasses int,
    TotalPass int,
    PenaltiesCommited int,
    PenaltiesMissed int,
    PenaltiesSaved int,
    PenaltiesScored int,
    PenaltiesWon int,
    ShotsOnTarget int,
    ShotsTotal int,
    Blocks int,
    Interceptions int,
    TotalTackles int
);

insert into staging.fixtureplayerstatistics 
(
    FixtureId,
    PlayerId,
    TeamId,
    Position,
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
select
    FixtureId,
    PlayerId,
    TeamId,
    Position,
    coalesce(CardsRed,0),
    coalesce(CardsYellow,0),
    coalesce(DribbleAttempts,0),
    coalesce(DribblePast,0),
    coalesce(DribbleSuccess,0),
    coalesce(DuelsTotal,0),
    coalesce(DuelsWon,0),
    coalesce(FoulsCommited,0),
    coalesce(FoulsDrawn,0),
    to_boolean(Captain),
    coalesce(MinutesPlayed,0),
    coalesce(Rating,0),
    to_boolean(Substitute),
    coalesce(GoalAssists,0),
    coalesce(GoalsConceded,0),
    coalesce(GoalSaves,0),
    coalesce(GoalsTotal,0),
    coalesce(Offsides,0),
    coalesce(PassAccuracy,0),
    coalesce(KeyPasses,0),
    coalesce(TotalPass,0),
    coalesce(PenaltiesCommited,0),
    coalesce(PenaltiesMissed,0),
    coalesce(PenaltiesSaved,0),
    coalesce(PenaltiesScored,0),
    coalesce(PenaltiesWon,0),
    coalesce(ShotsOnTarget,0),
    coalesce(ShotsTotal,0),
    coalesce(Blocks,0),
    coalesce(Interceptions,0),
    coalesce(TotalTackles,0)
from Staging.fixture_players_statistics_unflattened
where ExecutionInstance = :ex_inst
and LoadDatetime = :load_datetime;

update staging.fixtureplayerstatistics FPS 
    set FPS.FixtureKey = FF.FixtureKey
from Star.FactFixture FF 
WHERE FPS.FixtureId = FF.FixtureId;

update staging.fixtureplayerstatistics FPS
    set FPS.PlayerKey = DP.PlayerKey
from Star.DimPlayer DP
where FPS.PlayerId = DP.PlayerId;

update staging.fixtureplayerstatistics FPS
    set FPS.TeamKey = DT.TeamKey
from Star.DimTeam DT    
where FPS.TeamId = DT.TeamId;

update staging.fixtureplayerstatistics FPS
    set FPS.PositionKey =   DP.PositionKey
from Star.DimPosition DP 
where FPS.Position = DP.PlayerPosition
and DP.PlayerGridPosition is null;

delete from staging.fixtureplayerstatistics SRC using Star.FactFixturePlayerStatistics DST
where SRC.FixtureKey = DST.FixtureKey
and SRC.PlayerKey = DST.PlayerKey
and SRC.TeamKey = DST.TeamKey
and SRC.PositionKey = DST.PositionKey
and SRC.CardsRed = DST.CardsRed
and SRC.CardsYellow = DST.CardsYellow
and SRC.DribbleAttempts = DST.DribbleAttempts
and SRC.DribblePast = DST.DribblePast
and SRC.DribbleSuccess = DST.DribbleSuccess
and SRC.DuelsTotal = DST.DuelsTotal
and SRC.DuelsWon = DST.DuelsWon
and SRC.FoulsCommited = DST.FoulsCommited
and SRC.FoulsDrawn = DST.FoulsDrawn
and SRC.Captain = DST.Captain
and SRC.MinutesPlayed = DST.MinutesPlayed
and SRC.Rating = DST.Rating
and SRC.Substitute = DST.Substitute
and SRC.GoalAssists = DST.GoalAssists
and SRC.GoalsConceded = DST.GoalsConceded
and SRC.GoalSaves = DST.GoalSaves
and SRC.GoalsTotal = DST.GoalsTotal
and SRC.Offsides = DST.Offsides
and SRC.PassAccuracy = DST.PassAccuracy
and SRC.KeyPasses = DST.KeyPasses
and SRC.TotalPass = DST.TotalPass
and SRC.PenaltiesCommited = DST.PenaltiesCommited
and SRC.PenaltiesMissed = DST.PenaltiesMissed
and SRC.PenaltiesSaved = DST.PenaltiesSaved
and SRC.PenaltiesScored = DST.PenaltiesScored
and SRC.PenaltiesWon = DST.PenaltiesWon
and SRC.ShotsOnTarget = DST.ShotsOnTarget
and SRC.ShotsTotal = DST.ShotsTotal
and SRC.Blocks = DST.Blocks
and SRC.Interceptions = DST.Interceptions
and SRC.TotalTackles = DST.TotalTackles;

update Star.FactFixturePlayerStatistics DST
    set DST.CardsRed = SRC.CardsRed
    ,DST.CardsYellow = SRC.CardsYellow
    ,DST.DribbleAttempts = SRC.DribbleAttempts
    ,DST.DribblePast = SRC.DribblePast
    ,DST.DribbleSuccess = SRC.DribbleSuccess
    ,DST.DuelsTotal = SRC.DuelsTotal
    ,DST.DuelsWon = SRC.DuelsWon
    ,DST.FoulsCommited = SRC.FoulsCommited
    ,DST.FoulsDrawn = SRC.FoulsDrawn
    ,DST.Captain = SRC.Captain
    ,DST.MinutesPlayed = SRC.MinutesPlayed
    ,DST.Rating = SRC.Rating
    ,DST.Substitute = SRC.Substitute
    ,DST.GoalAssists = SRC.GoalAssists
    ,DST.GoalsConceded = SRC.GoalsConceded
    ,DST.GoalSaves = SRC.GoalSaves
    ,DST.GoalsTotal = SRC.GoalsTotal
    ,DST.Offsides = SRC.Offsides
    ,DST.PassAccuracy = SRC.PassAccuracy
    ,DST.KeyPasses = SRC.KeyPasses
    ,DST.TotalPass = SRC.TotalPass
    ,DST.PenaltiesCommited = SRC.PenaltiesCommited
    ,DST.PenaltiesMissed = SRC.PenaltiesMissed
    ,DST.PenaltiesSaved = SRC.PenaltiesSaved
    ,DST.PenaltiesScored = SRC.PenaltiesScored
    ,DST.PenaltiesWon = SRC.PenaltiesWon
    ,DST.ShotsOnTarget = SRC.ShotsOnTarget
    ,DST.ShotsTotal = SRC.ShotsTotal
    ,DST.Blocks = SRC.Blocks
    ,DST.Interceptions = SRC.Interceptions
    ,DST.TotalTackles = SRC.TotalTackles
    ,DST.UpdateDatetime = current_timestamp()
from Staging.fixtureplayerstatistics SRC
where SRC.FixtureKey = DST.FixtureKey
and SRC.PlayerKey = DST.PlayerKey
and SRC.TeamKey = DST.TeamKey
and SRC.PositionKey = DST.PositionKey;

insert into Star.FactFixturePlayerStatistics
(
    FixtureKey,
    PlayerKey,
    TeamKey,
    PositionKey,
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
select
    FixtureKey,
    PlayerKey,
    TeamKey,
    PositionKey,
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
from staging.fixtureplayerstatistics SRC
where not exists (select 1 from Star.FactFixturePlayerStatistics DST
                  where SRC.FixtureKey = DST.FixtureKey
                  and SRC.PlayerKey = DST.PlayerKey
                  and SRC.TeamKey = DST.TeamKey
                  and SRC.PositionKey = DST.PositionKey);

delete from staging.fixture_players_statistics_unflattened where ExecutionInstance = :ex_inst and LoadDatetime = :load_datetime;

end for;

return 'Completed staging.usp_merge_fact_fixture_player_statistics';

end;