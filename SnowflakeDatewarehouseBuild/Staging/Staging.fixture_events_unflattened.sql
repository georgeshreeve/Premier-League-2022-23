create or replace table Staging.fixture_events_unflattened
(
    ExecutionInstance string,
    FixtureId BIGINT,
    AssistId int,
    AssistName nvarchar(50),
    Comments nvarchar(100),
    Detail nvarchar(100),
    PlayerId BIGINT,
    PlayerName nvarchar(50),
    TeamId int,
    TeamName nvarchar(30),
    TimeElapsed int,
    TimeExtra int,
    EventType nvarchar(30),
    LoadDatetime TIMESTAMP default(current_timestamp())

)