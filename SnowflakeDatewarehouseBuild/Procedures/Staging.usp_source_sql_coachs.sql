create or replace procedure Staging.usp_source_sql_coachs()
returns string not null
language sql 
as
declare
    ex_inst string;
    ex_in_cursor cursor for select excecution_instance from Staging.coachs_unflattened where processed = 0 order by LOADDATETIME;
begin

for record in ex_in_cursor do

ex_inst := record.excecution_instance;

create or replace temporary table Staging.coachs (execution_instance string,json_text variant);

insert into Staging.coachs
(
    execution_instance,
    json_text
)
select
    excecution_instance,
    json_text
from STAGING.coachs_unflattened
where excecution_instance = :ex_inst;

insert into staging.coachs_flattened
(
    ExecutionInstance,
    Age,
    BirthCountry,
    BirthDate,
    BirthPlace,
    FirstName,
    LastName,
    Height,
    Id,
    Name,
    Nationality,
    TeamId,
    TeamName,
    Weight
)

select
    s.execution_instance,
    response.value:age,
    response.value:birth:country,
    response.value:birth:date,
    response.value:birth:place,
    response.value:firstname,
    response.value:lastname,
    response.value:height,
    response.value:id,
    response.value:name,
    response.value:nationality,
    response.value:team:id,
    response.value:team:name,
    response.value:weight
from coachs as s,
lateral flatten(input => s.json_text:response) as response;

update staging.coachs_unflattened
set processed = 1
where excecution_instance = :ex_inst;

end for;

return 'Completed Staging.usp_source_sql_coachs';

end;