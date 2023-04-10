create or replace procedure public.get_table_position()
returns table(as_of_date date, obj varchar)
language sql
as
declare
    res RESULTSET;
	query varchar;
    fixture_date date;
	fixture_date_cursor cursor for
    select 
        DD.my_date as fixture_date
    from star.dimdate DD
    where DD.my_date >=
    (
        select min(cast(fixturedate as date)) from star.factfixture
    )
    and DD.MY_DATE <= current_date();
begin

create or replace temporary table star.table_position
(
    as_of_date date,
    obj object 
);

for record in fixture_date_cursor do

    fixture_date := record.fixture_date;

    insert into star.table_position
    (
    	as_of_date,
    	obj
    )
    select
        :fixture_date,
        object_construct(
            'Table', array_agg
                    (
                        object_construct
                        (
                            'As_Of_Date',:fixture_date,
                            'Team_Name',Team,
                            'GP',GP,
                            'P',P,
                            'GD',GD,
                            'GF',GF,
                            'GA',GA,
                            'W',W,
                            'D',D,
                            'L',L
                        )
                    )
                    
        )
    from
    (
        select
            Team,
            count(*) as GP,
            SUM(coalesce(P,0)) as P,
            SUM(coalesce(GD,0)) as GD,
            SUM(coalesce(GF,0)) as GF,
            SUM(coalesce(GA,0)) as GA,
            SUM(coalesce(W,0)) as W,
            SUM(coalesce(D,0)) as D,
            SUM(coalesce(L,0)) as L
        from
        (
            select
                CAST(FF.FIXTUREDATE AS DATE) as FixtureDate,
                DT.TEAMNAME as Team,
                sum(case when hometeamwinner = true then 3 when hometeamwinner = false then 0 else 1 end) as P,
                sum(fulltimehomegoals - fulltimeawaygoals) as GD,
                sum(fulltimehomegoals) as GF,
                sum(fulltimeawaygoals) as GA,
                sum(case when hometeamwinner = true then 1 else 0 end) as W,
                sum(case when hometeamwinner is null then 1 else 0 end) as D,
                sum(case when hometeamwinner = false then 1 else 0 end) as L
            from star.factfixture FF
            inner join star.dimteam DT ON FF.HOMETEAMKEY = DT.TEAMKEY
            where CAST(FF.FIXTUREDATE AS DATE) <= :fixture_date
            group by FixtureDate, Team

            union

            select
                CAST(FF.FIXTUREDATE AS DATE) as FixtureDate,
                DT.TEAMNAME as Team,
                sum(case when awayteamwinner = true then 3 when awayteamwinner = false then 0 else 1 end) as P,
                sum(fulltimeawaygoals - fulltimehomegoals) as GD,
                sum(fulltimeawaygoals) as GF,
                sum(fulltimehomegoals) as GA,
                sum(case when awayteamwinner = true then 1 else 0 end) as W,
                sum(case when awayteamwinner is null then 1 else 0 end) as D,
                sum(case when awayteamwinner = false then 1 else 0 end) as L
            from star.factfixture FF
            inner join Star.DIMTEAM DT on FF.AWAYTEAMKEY = DT.TEAMKEY
            where CAST(FF.FIXTUREDATE AS DATE) <= :fixture_date
            group by FixtureDate, Team
        )
        GROUP BY Team 
        ORDER BY P DESC,GD,GF
    );
end for;

query := 'select as_of_date,TO_JSON(TO_VARIANT(obj)) from star.table_position';
res := (EXECUTE IMMEDIATE :query);
RETURN TABLE (res);

end;