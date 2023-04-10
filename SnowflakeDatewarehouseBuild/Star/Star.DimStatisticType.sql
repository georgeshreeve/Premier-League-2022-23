create table Star.DimStatisticType
(
    StatisticTypeKey int not null identity(1,1) primary key,
    StatisticType string unique,
    LoadDatetime timestamp default (current_timestamp()),
    UpdateDatetime timestamp default (current_timestamp())
)
