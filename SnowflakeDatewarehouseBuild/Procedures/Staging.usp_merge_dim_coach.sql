create or replace procedure staging.usp_merge_dim_coach()
returns string
language sql
as
declare
    ex_inst string;
    load_datetime timestamp;
    ex_in_cursor cursor for select ExecutionInstance,LoadDatetime from staging.coachs_flattened GROUP by ExecutionInstance,LoadDatetime order by LoadDatetime;
begin
for record in ex_in_cursor do

ex_inst := record.EXECUTIONINSTANCE;
load_datetime := record.LOADDATETIME;

create or replace temporary table staging.coach 
(
    CoachId int,
    Name string,
    FirstName string,
    LastName string,
    Height string,
    Age int,
    Weight string,
    Nationality string,
    BirthDate date,
    BirthCountry string,
    BirthPlace string,
    TeamId int,
    TeamKey int default(1)
);

insert into staging.coach
(
    CoachId,
    Name,
    FirstName,
    LastName,
    Height,
    Age,
    Weight,
    Nationality,
    BirthDate,
    BirthCountry,
    BirthPlace,
    TeamId
)
select
    Id,
    Name,
    FirstName,
    LastName,
    coalesce(Height,'Unknown'),
    coalesce(Age,-1),
    coalesce(Weight,'Unknown'),
    coalesce(Nationality,'Unknown'),
    coalesce(BirthDate,'1900-01-01'),
    coalesce(BirthCountry,'Unknown'),
    coalesce(BirthPlace,'Unknown'),
    TeamId
from staging.coachs_flattened
where ExecutionInstance = :ex_inst
and LoadDatetime = :load_datetime;

UPDATE Staging.coach C
    set C.TeamKey = T.TeamKey
from Star.DimTeam T
where C.TeamId = T.TeamId;

delete from Staging.coach SRC using Star.DimCoach DST
where SRC.CoachId = DST.CoachId
and SRC.Name = DST.Name
and SRC.FirstName = DST.FirstName
and SRC.LastName = DST.LastName
and SRC.Height = DST.Height
and SRC.Age = DST.Age
and SRC.Weight = DST.Weight
and SRC.Nationality = DST.Nationality
and SRC.BirthDate = DST.BirthDate
and SRC.BirthCountry = DST.BirthCountry
and SRC.BirthPlace = DST.BirthPlace
and SRC.TeamKey = DST.TeamKey;

UPDATE Star.DimCoach DST
    set DST.Name = SRC.NAME
    ,DST.FirstName = SRC.FirstName
    ,DST.LastName = SRC.LastName
    ,DST.Height = SRC.Height
    ,DST.Age = SRC.Age
    ,DST.Weight = SRC.Weight
    ,DST.Nationality = SRC.Nationality
    ,DST.BirthDate = SRC.BirthDate
    ,DST.BirthCountry = SRC.BirthCountry
    ,DST.BirthPlace = SRC.BirthPlace
    ,DST.TeamKey = SRC.TeamKey
    ,DST.UpdateDatetime = current_timestamp()
FROM staging.coach SRC
where SRC.CoachId = DST.CoachId;

insert into Star.DimCoach
(
    CoachId,
    Name,
    FirstName,
    LastName,
    Height,
    Age,
    Weight,
    Nationality,
    BirthDate,
    BirthCountry,
    BirthPlace,
    TeamKey
)
select
    CoachId,
    Name,
    FirstName,
    LastName,
    Height,
    Age,
    Weight,
    Nationality,
    BirthDate,
    BirthCountry,
    BirthPlace,
    TeamKey
from staging.coach SRC
where not exists (select 1 from Star.DimCoach DST where SRC.CoachId = DST.CoachId);

delete from staging.coachs_flattened where ExecutionInstance = :ex_inst and LoadDatetime = :load_datetime;

end for;

return 'Completed staging.usp_merge_dim_coach';

end;