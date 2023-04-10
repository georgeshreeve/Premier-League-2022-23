create table Star.FactFixtureTeamStatistics
(
    FactFixtureTeamStatisticsKey int not null identity(1,1) primary key,
    FixtureKey int not null,
    TeamKey int not null,
    StatisticTypeKey int not null,
    StatisticValue string,
    LoadDatetime timestamp default (current_timestamp()),
    UpdateDatetime timestamp default (current_timestamp()),
    foreign key (FixtureKey) references Star.FactFixture(FixtureKey),
    foreign key (TeamKey) references Star.DimTeam(TeamKey),
    foreign key (StatisticTypeKey) references Star.DimStatisticType(StatisticTypeKey)
)