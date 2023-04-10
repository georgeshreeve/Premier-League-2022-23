create or replace procedure staging.usp_merge_fact_fixture_event()
returns string
language sql
as
declare
    ex_inst string;
    load_datetime timestamp;
    ex_in_cursor cursor for select ExecutionInstance,LoadDatetime from staging.fixture_events_unflattened GROUP by ExecutionInstance,LoadDatetime order by LoadDatetime;
begin
for record in ex_in_cursor do

ex_inst := record.ExecutionInstance;
load_datetime := record.LoadDatetime;

create or replace temporary table staging.fixtureevent 
(
    FixtureId int,
    FixtureKey int,
    AssistPlayerId int,
    AssistPlayerKey int default(1),
    EventPlayerId int,
    EventPlayerKey int default(1),
    EventDetail string,
    EventType string,
    EventComment string,
    EventDetailKey int default(1),
    TimeElapsed int,
    TimeExtra int
);

insert into staging.fixtureevent
(
    FixtureId,
    AssistPlayerId,
    EventPlayerId,
    EventDetail,
    EventType,
    EventComment,
    TimeElapsed,
    TimeExtra
)
select
    FixtureId,
    AssistId,
    PlayerId,
    Detail,
    EventType,
    coalesce(Comments,'No Comment'),
    coalesce(TimeElapsed,0),
    coalesce(TimeExtra,0)
from Staging.fixture_events_unflattened
where ExecutionInstance = :ex_inst
and LoadDatetime = :load_datetime;

update staging.fixtureevent FE
    set FE.FixtureKey = FF.FixtureKey
from Star.FactFixture FF 
where FE.FixtureId = FF.FixtureId;

update staging.fixtureevent FE
    set FE.AssistPlayerKey = DP.PlayerKey
from Star.DimPlayer DP 
where FE.AssistPlayerId = DP.PlayerId;

update staging.fixtureevent FE 
    set FE.EventPlayerKey = DP.PlayerKey
from Star.DimPlayer DP 
where FE.EventPlayerId = DP.PlayerId;

update staging.fixtureevent FE
    set FE.EventDetailKey = DED.EventDetailKey
from Star.DimEventDetail DED 
where FE.EventDetail = DED.EventDetail
and FE.EventType = DED.EventType
and FE.EventComment = DED.EventComment;

delete from staging.fixtureevent SRC using Star.FactFixtureEvent DST
where SRC.FixtureKey = DST.FixtureKey
and SRC.AssistPlayerKey = DST.AssistPlayerKey
and SRC.EventPlayerKey = DST.EventPlayerKey
and SRC.EventDetailKey = DST.EventDetailKey
and SRC.TimeElapsed = DST.TimeElapsed
and SRC.TimeExtra = DST.TimeExtra;

update Star.FactFixtureEvent DST
    set DST.TimeElapsed = SRC.TimeElapsed
    ,DST.TimeExtra = SRC.TimeExtra
    ,DST.UpdateDatetime = current_timestamp()
from staging.fixtureevent SRC
where SRC.FixtureKey = DST.FixtureKey
and SRC.AssistPlayerKey = DST.AssistPlayerKey
and SRC.EventPlayerKey = DST.EventPlayerKey
and SRC.EventDetailKey = DST.EventDetailKey;

insert into Star.FactFixtureEvent
(
    FixtureKey,
    AssistPlayerKey,
    EventPlayerKey,
    EventDetailKey,
    TimeElapsed,
    TimeExtra
)
select
    FixtureKey,
    AssistPlayerKey,
    EventPlayerKey,
    EventDetailKey,
    TimeElapsed,
    TimeExtra
from staging.fixtureevent SRC 
where not exists (select 1 from Star.FactFixtureEvent DST
                  where SRC.FixtureKey = DST.FixtureKey
                  and SRC.AssistPlayerKey = DST.AssistPlayerKey
                  and SRC.EventPlayerKey = DST.EventPlayerKey
                  and SRC.EventDetailKey = DST.EventDetailKey);

delete from staging.fixture_events_unflattened where ExecutionInstance = :ex_inst and LoadDatetime = :load_datetime;

end for;

return 'Completed staging.usp_merge_fact_fixture_event';

end;