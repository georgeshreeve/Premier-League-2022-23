create table Star.DimPosition
(
    PositionKey int not null identity(1,1) primary key,
    PlayerPosition string,
    PlayerGridPosition string,
    LoadDatetime timestamp default (current_timestamp()),
    UpdateDatetime timestamp default (current_timestamp())
)