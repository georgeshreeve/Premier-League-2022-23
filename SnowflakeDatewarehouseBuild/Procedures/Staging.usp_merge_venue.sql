create or replace procedure Staging.usp_merge_venues()
returns string not null
language sql
as
declare 
    ex_inst string;
    ex_in_cursor cursor for select ExecutionInstance,LOADDATETIME from staging.venue_flattened GROUP BY ExecutionInstance,LOADDATETIME order by LOADDATETIME;
begin
for record in ex_in_cursor do

ex_inst := record.ExecutionInstance;

create or replace temporary table staging.venue
(
    VenueId int,
    VenueName string,
    VenueSurface string,
    VenueCountry string,
    VenueCity string,
    VenueAddress string,
    VenueCapacity INT
);

insert into staging.venue
(
    VenueId,
    VenueName,
    VenueSurface,
    VenueCountry,
    VenueCity,
    VenueAddress,
    VenueCapacity
)
select
    ID,
    NAME,
    SURFACE,
    COUNTRY,
    CITY,
    ADDRESS,
    CAPACITY
from staging.venue_flattened
where ExecutionInstance = :ex_inst;

update Star.DimVenue DST 
SET DST.VenueName = SRC.VenueName,
DST.VenueSurface = SRC.VenueSurface,
DST.VenueCountry = SRC.VenueCountry,
DST.VenueCity = SRC.VenueCity,
DST.VenueAddress = SRC.VenueAddress,
DST.VenueCapacity = SRC.VenueCapacity,
DST.UpdateDatetime = current_timestamp()
from staging.venue SRC
where  DST.VenueId = SRC.VenueId;

insert into Star.DimVenue
(
    VenueId,
    VenueName,
    VenueSurface,
    VenueCountry,
    VenueCity,
    VenueAddress,
    VenueCapacity
)
select 
    VenueId,
    VenueName,
    VenueSurface,
    VenueCountry,
    VenueCity,
    VenueAddress,
    VenueCapacity
from staging.venue SRC
where not exists (select 1 from Star.DimVenue DST where SRC.VenueId = DST.VenueId);

delete  from staging.venue_flattened  where ExecutionInstance = :ex_inst;

end for;

return 'Completed Staging.usp_merge_venues';
end;


