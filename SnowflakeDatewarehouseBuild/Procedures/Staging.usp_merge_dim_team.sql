create or replace procedure Staging.usp_merge_dim_team()
returns string not null
language sql
as
declare
    ex_inst string;
    load_datetime timestamp;
    ex_in_cursor cursor for select EXECUTIONINSTANCE,LOADDATETIME from staging.teams_flattened GROUP by EXECUTIONINSTANCE,LOADDATETIME order by LOADDATETIME;
begin
for record in ex_in_cursor do

ex_inst := record.EXECUTIONINSTANCE;
load_datetime := record.LOADDATETIME;

create or replace temporary table staging.teams
(
    TeamId int,
    TeamCode string,
    TeamName string,
    TeamCountry string,
    NationalTeam boolean,
    YearFounded int
);

insert into staging.teams
(
    TeamId,
    TeamCode,
    TeamName,
    TeamCountry,
    NationalTeam,
    YearFounded
)
select
    TEAMID,
    TEAMCODE,
    TEAMNAME,
    TEAMCOUNTRY,
    to_boolean(NATIONAL),
    FOUNDED
from staging.teams_flattened
where EXECUTIONINSTANCE = :ex_inst
and LOADDATETIME = :load_datetime;

delete from staging.teams SRC using Star.DimTeam DST
where SRC.TeamId = DST.TeamId
AND SRC.TeamCode = DST.TeamCode
AND SRC.TeamName = DST.TeamName
AND SRC.TeamCountry = DST.TeamCountry
AND SRC.NationalTeam = DST.NationalTeam
AND SRC.YearFounded = DST.YearFounded;

UPDATE Star.DimTeam DST
    set DST.TeamCode = SRC.TeamCode,
    DST.TeamName = SRC.TeamName,
    DST.TeamCountry = SRC.TeamCountry,
    DST.NationalTeam = SRC.NationalTeam,
    DST.YearFounded = SRC.YearFounded,
    DST.UpdateDatetime = current_timestamp()
from staging.teams SRC
where SRC.TeamId = DST.TeamId;

insert into Star.DimTeam
(
    TeamId,
    TeamCode,
    TeamName,
    TeamCountry,
    NationalTeam,
    YearFounded
)
select
    TeamId,
    TeamCode,
    TeamName,
    TeamCountry,
    NationalTeam,
    YearFounded
from staging.teams SRC
where not exists (select 1 from Star.DimTeam DST where SRC.TeamId = DST.TeamId);

delete from staging.teams_flattened where EXECUTIONINSTANCE = :ex_inst and LOADDATETIME = :load_datetime;

end for;

return 'Completed Staging.usp_merge_dim_team';

end;