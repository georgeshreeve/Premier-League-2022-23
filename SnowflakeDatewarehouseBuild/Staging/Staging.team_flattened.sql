create table Staging.teams_flattened 
(
    ExecutionInstance string,
    TeamCode nvarchar(5),
    TeamCountry nvarchar(30),
    Founded int,
    TeamId int,
    TeamName nvarchar(50),
    National nvarchar(6),
    LeagueId int,
    LoadDatetime TIMESTAMP default(current_timestamp())
)