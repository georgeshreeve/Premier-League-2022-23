create or replace table Staging.fixture_lineups_unflattened
(
    ExecutionInstance string,
    FixtureId BIGINT,
    TeamId int,
    TeamName nvarchar(30),
    CoachId int,
    CoachName nvarchar(30),
    Formation nvarchar(20),
    StartOrSub nvarchar(10),
    PlayerGridPositon nvarchar(4),
    PlayerId bigint,
    PlayerName nvarchar(30),
    PlayerNumber int,
    PlayerPosition nvarchar(3),
    LoadDatetime timestamp default (current_timestamp())
)