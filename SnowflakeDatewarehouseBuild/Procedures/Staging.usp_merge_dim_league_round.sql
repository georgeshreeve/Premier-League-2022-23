create or replace procedure staging.usp_merge_dim_league_round()
returns string
language sql
as
declare
    ex_inst string;
    load_datetime timestamp;
    ex_in_cursor cursor for select EXECUTIONINSTANCE,LOADDATETIME from staging.fixture_flattened GROUP by EXECUTIONINSTANCE,LOADDATETIME order by LOADDATETIME;
begin
for record in ex_in_cursor do

ex_inst := record.EXECUTIONINSTANCE;
load_datetime := record.LOADDATETIME;

create or replace temporary table staging.leagueround 
(
    LeagueName string,
    LeagueCountry string,
    LeagueSeason int,
    LeagueRound string
);

insert into staging.leagueround
(
    LeagueName,
    LeagueCountry,
    LeagueSeason,
    LeagueRound
)
select
    coalesce(LeagueName,'Unknown'),
    coalesce(LeagueCountry,'Unknown'),
    coalesce(LeagueSeason,-1),
    coalesce(LeagueRound,'Unknown')
from staging.fixture_flattened
where EXECUTIONINSTANCE = :ex_inst
and LOADDATETIME = :load_datetime
group by coalesce(LeagueName,'Unknown'),
    coalesce(LeagueCountry,'Unknown'),
    coalesce(LeagueSeason,-1),
    coalesce(LeagueRound,'Unknown');

delete from staging.leagueround SRC using Star.DimLeagueRound DST
where SRC.LeagueName = DST.LeagueName
and SRC.LeagueCountry = DST.LeagueCountry
and SRC.LeagueSeason = DST.LeagueSeason
and SRC.LeagueRound = DST.LeagueRound;

insert into Star.DimLeagueRound
(
    LeagueName,
    LeagueCountry,
    LeagueSeason,
    LeagueRound
)
select
    LeagueName,
    LeagueCountry,
    LeagueSeason,
    LeagueRound
from staging.leagueround SRC
where not exists (select 1 from Star.DimLeagueRound DST 
                  where SRC.LeagueName = DST.LeagueName
                  and SRC.LeagueCountry = DST.LeagueCountry
                  and SRC.LeagueSeason = DST.LeagueSeason
                  and SRC.LeagueRound = DST.LeagueRound);

end for;

return 'Completed staging.usp_merge_dim_league_round';

end;