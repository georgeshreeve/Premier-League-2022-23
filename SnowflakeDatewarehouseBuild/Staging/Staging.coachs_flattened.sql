create table Staging.coachs_flattened
(
    ExecutionInstance string,
    Age int,
    BirthCountry nvarchar(50),
    BirthDate date,
    BirthPlace nvarchar(50),
    FirstName nvarchar(30),
    LastName nvarchar(30),
    Height string,
    Id int,
    Name nvarchar(30),
    Nationality nvarchar(30),
    TeamId int,
    TeamName nvarchar(30),
    Weight nvarchar(10),
    LoadDatetime TIMESTAMP default(current_timestamp())
)