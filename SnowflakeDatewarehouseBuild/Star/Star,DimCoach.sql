create table Star.DimCoach
(
    CoachKey int not null identity(1,1) primary key,
    CoachId int unique,
    Name string,
    FirstName string,
    LastName string,
    Height string,
    Age int,
    Weight string,
    Nationality string,
    BirthDate date,
    BirthCountry string,
    BirthPlace string,
    TeamKey int not null,
    LoadDatetime timestamp default (current_timestamp()),
    UpdateDatetime timestamp default (current_timestamp()),
    foreign key (TeamKey) references Star.DimTeam(TeamKey)
)