create table Star.DimLeagueRound
(
    LeagueRoundKey int not null identity(1,1) primary key,
    LeagueName string,
    LeagueCountry string,
    LeagueSeason int,
    LeagueRound string,
    LoadDatetime timestamp default (current_timestamp()),
    UpdateDatetime timestamp default (current_timestamp())
)