CREATE table Staging.venue_flattened
(
    ExecutionInstance string,
    Address nvarchar,
    Capacity bigint,
    City nvarchar(50),
    Country nvarchar(50),
    Id int,
    Name nvarchar(50),
    Surface nvarchar(30),
    LoadDatetime TIMESTAMP default(current_timestamp())
)