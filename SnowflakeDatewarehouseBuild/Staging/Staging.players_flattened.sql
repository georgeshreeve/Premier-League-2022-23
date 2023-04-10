create table staging.players_flattened
(
    ExecutionInstance string,
    TeamId int,
    TeamName nvarchar(50),
    PlayerAge int,
    PlayerId bigint,
    PlayerName nvarchar(100),
    PlayerNumber int,
    PlayerPosition nvarchar(100),
    LoadDatetime TIMESTAMP default(current_timestamp())
)