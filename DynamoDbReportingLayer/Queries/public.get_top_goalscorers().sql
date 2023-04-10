
create or replace procedure public.get_top_goalscorers()
returns table(as_of_date date,obj varchar)
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

create or replace temporary table star.top_scorers
(
	as_of_date date,
	obj object
);

for record in fixture_date_cursor do

	fixture_date := record.fixture_date;

	insert into star.top_scorers
    (
    	as_of_date,
    	obj
    )
    SELECT
    :fixture_date,
    object_construct(
    		'Top_GoalScorers', array_agg
            				   (
                               	object_construct
                                	(
                                    	'As_Of_Date',:fixture_date,
            							'Player_Name',PLAYERNAME,
                                        'Team_Name',TEAMNAME,
                                        'Goals',GOALS
            						)

            					)
    	)
	from 
	(
		select
    		PLAYERNAME,
        	TEAMNAME,	
    		sum(goalstotal) as GOALS
    	from star.factfixtureplayerstatistics FFPS 
    	inner join star.dimteam DT ON FFPS.TEAMKEY = DT.TEAMKEY
    	INNER JOIN star.dimplayer DP ON FFPS.PLAYERKEY = DP.PLAYERKEY
        where exists
        (
        	select
				1
            from star.factfixture FF
            where FF.FIXTUREKEY = FFPS.FIXTUREKEY
            and CAST(FF.FIXTUREDATE AS date) <= :fixture_date
        )
    	group by playername,teamname
    	order by GOALS DESC
    	LIMIT 10
	);
end for;

query := 'select as_of_date,TO_JSON(TO_VARIANT(obj)) from star.top_scorers';
res := (EXECUTE IMMEDIATE :query);
RETURN TABLE (res);

end;
