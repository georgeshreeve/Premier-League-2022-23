create table Star.DimReferee
(
    RefereeKey INT NOT NULL identity(1,1) primary key,
    RefereeName string UNIQUE,
    RefereeNationality string,
    LoadDatetime timestamp default (current_timestamp()),
    UpdateDatetime timestamp default (current_timestamp())
)