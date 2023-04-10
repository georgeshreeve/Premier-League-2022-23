create table Star.FactFixtureEvent
(
    FactFixtureEventKey int not null identity(1,1) primary key,
    FixtureKey int not null,
    AssistPlayerKey int not null,
    EventPlayerKey int not null,
    EventDetailKey int not null,
    TimeElapsed int,
    TimeExtra int,
    LoadDatetime timestamp default (current_timestamp()),
    UpdateDatetime timestamp default (current_timestamp()),
    foreign key (FixtureKey) references Star.FactFixture(FixtureKey),
    foreign key (AssistPlayerKey) references Star.DimPlayer(PlayerKey),
    foreign key (EventPlayerKey) references Star.DimPlayer(PlayerKey),
    foreign key (EventDetailKey) references Star.DimEventDetail(EventDetailKey)
)