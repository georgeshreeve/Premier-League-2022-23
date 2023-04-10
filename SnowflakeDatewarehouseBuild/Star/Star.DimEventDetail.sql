create table Star.DimEventDetail
(
    EventDetailKey int not null identity(1,1) primary key,
    EventDetail string,
    EventType string,
    EventComment string,
    LoadDatetime timestamp default (current_timestamp()),
    UpdateDatetime timestamp default (current_timestamp())
)