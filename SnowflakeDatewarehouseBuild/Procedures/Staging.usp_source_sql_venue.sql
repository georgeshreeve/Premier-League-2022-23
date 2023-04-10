create or replace procedure Staging.usp_source_sql_venues()
returns string not null
language sql 
as
declare
    ex_inst string;
    ex_in_cursor cursor for select excecution_instance from Staging.venue_unflattened where processed = 0 order by LOADDATETIME;
begin
for record in ex_in_cursor do

ex_inst := record.excecution_instance;

create or replace temporary table Staging.venue (execution_instance string,json_text variant);

insert into Staging.venue
(
    execution_instance,
    json_text
)
select
    excecution_instance,
    json_text
from staging.venue_unflattened
where excecution_instance = :ex_inst;

insert into Staging.venue_flattened
(
    ExecutionInstance,
    Address,
    Capacity,
    City,
    Country,
    Id,
    Name,
    Surface
)
SELECT
    s.execution_instance,
    response.value:address,
    response.value:capacity,
    response.value:city,
    response.value:country,
    response.value:id,
    response.value:name,
    response.value:surface
from venue as s,
lateral flatten(input => s.json_text:response) as response;

update Staging.venue_unflattened
set processed = 1
where excecution_instance = :ex_inst;

end for;

return 'Completed Staging.usp_source_sql_venue';
end;