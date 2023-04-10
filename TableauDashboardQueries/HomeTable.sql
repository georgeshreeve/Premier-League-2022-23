select
    Home.HomeTeam as Team,
    count(Home.HomeTeam) as GP,
    SUM(Home.GoalsFor) as GF,
    SUM(Home.GoalsAgainst) AS GA,
    SUM(Home.HomeTeamWin) as W,
    SUM(Home.Draw) AS D,
    SUM(Home.HomeTeamLoss) as L,
    SUM(Home.GoalsFor) - SUM(Home.GoalsAgainst) AS GD,
    SUM(Home.HomeTeamPoints) AS P
from 
(    
    select 
        DT.TEAMNAME AS HomeTeam,
        FF.fulltimehomegoals as GoalsFor,
        FF.fulltimeawaygoals As GoalsAgainst,
        CASE WHEN hometeamwinner  = true then 1 else 0 end as HomeTeamWin,
        CASE WHEN hometeamwinner is null then 1 else 0 end as Draw,
        CASE when hometeamwinner = false then 1 else 0 end as HomeTeamLoss,
        CASE WHEN hometeamwinner = true then 3 when hometeamwinner = FALSE then 0 else 1 end as HomeTeamPoints
    from Star.FACTFIXTURE FF
    INNER JOIN Star.DIMTEAM DT ON FF.HOMETEAMKEY = DT.TEAMKEY
)Home
group by Home.HomeTeam
order by P DESC
