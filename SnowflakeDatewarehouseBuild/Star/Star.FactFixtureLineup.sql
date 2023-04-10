create table Star.FactFixtureLineup
(
    FactFixtureLineupKey int not null identity(1,1) primary key,
    FixtureKey int not null,
    TeamKey int not null,
    CoachKey int not null,
    PlayerKey int not null,
    FormationKey int not null,
    PositionKey int not null,
    StartOrSub string,
    LoadDatetime timestamp default (current_timestamp()),
    UpdateDatetime timestamp default (current_timestamp()),
    foreign key (FixtureKey) references Star.FactFixture(FixtureKey),
    foreign key (TeamKey) references Star.DimTeam(TeamKey),
    foreign key (CoachKey) references Star.DimCoach(CoachKey) 
)