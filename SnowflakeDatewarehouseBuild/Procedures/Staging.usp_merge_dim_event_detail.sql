create or replace procedure Staging.usp_merge_dim_event_detail()
returns string
language sql
as
declare
    ex_inst string;
    load_datetime timestamp;
    ex_in_cursor cursor for select EXECUTIONINSTANCE,LOADDATETIME from staging.fixture_events_unflattened GROUP by EXECUTIONINSTANCE,LOADDATETIME order by LOADDATETIME;
begin
for record in ex_in_cursor do

ex_inst := record.EXECUTIONINSTANCE;
load_datetime := record.LOADDATETIME;

create or replace temporary table staging.eventdetail 
(
    EventDetail string,
    EventType string,
    EventComment string default('No Comment')
);

insert into staging.eventdetail
(
    EventDetail,
    EventType,
    EventComment
)
select
    DETAIL,
    EVENTTYPE,
    coalesce(COMMENTS,'No Comment')
from staging.fixture_events_unflattened
where EXECUTIONINSTANCE = :ex_inst
and LOADDATETIME = :load_datetime
GROUP by 
    DETAIL,
    EVENTTYPE,
    coalesce(COMMENTS,'No Comment');

delete from staging.eventdetail SRC using Star.DimEventDetail DST
where SRC.EventDetail = DST.EventDetail
and SRC.EventType = DST.EventType
and SRC.EventComment = DST.EventComment;

INSERT into Star.DimEventDetail
(
    EventDetail,
    EventType,
    EventComment
)
select
    EventDetail,
    EventType,
    EventComment
from staging.eventdetail SRC
where not exists (select 1 from Star.DimEventDetail DST
                  where SRC.EventDetail = DST.EventDetail
                  and SRC.EventType = DST.EventType
                  and SRC.EventComment = DST.EventComment);

end for;

return 'Completed staging.usp_merge_dim_event_detail';

end;