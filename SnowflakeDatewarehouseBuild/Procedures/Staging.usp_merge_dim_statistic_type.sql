create or replace procedure Staging.usp_merge_dim_statistic_type()
returns string not null
language sql
as 
declare
    ex_inst string;
    load_datetime timestamp;
    ex_in_cursor cursor for select ExecutionInstance,LOADDATETIME from Staging.fixture_team_statistics GROUP BY ExecutionInstance,LOADDATETIME order by LOADDATETIME;
begin
for record in ex_in_cursor do

ex_inst := record.ExecutionInstance;
load_datetime := record.LOADDATETIME;

create or replace temporary table staging.statistictype 
(
    StatisticType string
);

insert into staging.statistictype
(
    StatisticType
)
select
    coalesce(Type,'Unknown')
from staging.fixture_team_statistics
where ExecutionInstance = :ex_inst
and LOADDATETIME = :load_datetime
GROUP by Type;

delete from staging.statistictype SRC using Star.DimStatisticType DST
where SRC.StatisticType = DST.StatisticType;

INSERT into Star.DimStatisticType
(
    StatisticType
)
select
    StatisticType
from staging.statistictype SRC
WHERE NOT exists (select 1 FROM Star.DimStatisticType DST
                  where SRC.StatisticType = DST.StatisticType);

end for;

return 'Completed Staging.usp_merge_dim_statistic_type';

end;