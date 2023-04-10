create or replace table Staging.fixture_team_statistics
(
    ExecutionInstance string,
    FixtureId bigint,
    TeamId BIGINT,
    TeamName nvarchar(30),
    Type nvarchar(20),
    value nvarchar(20),
    LoadDatetime TIMESTAMP default(current_timestamp())
)