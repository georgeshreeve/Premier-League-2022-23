CREATE table Star.DimFormation
(
    FormationKey int not null identity(1,1) primary key,
    Formation string,
    LoadDatetime timestamp default (current_timestamp()),
    UpdateDatetime timestamp default (current_timestamp())
)