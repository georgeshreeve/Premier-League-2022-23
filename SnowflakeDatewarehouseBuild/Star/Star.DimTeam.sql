create table Star.DimTeam
(
    TeamKey int not null identity(1,1) primary key,
    TeamId int UNIQUE,
    TeamCode string,
    TeamName string,
    TeamCountry string,
    NationalTeam boolean,
    YearFounded int,
    LoadDatetime timestamp default (current_timestamp()),
    UpdateDatetime timestamp default (current_timestamp())
)