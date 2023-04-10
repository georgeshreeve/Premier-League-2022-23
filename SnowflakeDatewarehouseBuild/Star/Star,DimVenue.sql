CREATE table Star.DimVenue
(
    VenueKey INT not null identity(1,1) primary key,
    VenueId INT UNIQUE,
    VenueName string,
    VenueSurface string,
    VenueCountry string,
    VenueCity string,
    VenueAddress string,
    VenueCapacity INT,
    LoadDatetime timestamp default (current_timestamp()),
    UpdateDatetime timestamp default (current_timestamp())
)