create table Star.DimPlayer
(
    PlayerKey int not null identity(1,1) primary key,
    PlayerId bigint UNIQUE,
    PlayerName string,
    PlayerNumber int,
    PlayerPosition string,
    TeamKey int not null,
    LoadDatetime timestamp default (current_timestamp()),
    UpdateDatetime timestamp default (current_timestamp()),
    foreign key (TeamKey) references Star.DimTeam(TeamKey)
)