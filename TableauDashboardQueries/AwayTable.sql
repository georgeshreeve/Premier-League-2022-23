select
    Away.AwayTeam as Team,
    count(Away.AwayTeam) as GP,
    SUM(Away.GoalsFor) as GF,
    SUM(Away.GoalsAgainst) AS GA,
    SUM(Away.AwayTeamWin) as W,
    SUM(Away.Draw) AS D,
    SUM(Away.AwayTeamLoss) as L,
    SUM(Away.GoalsFor) - SUM(Away.GoalsAgainst) AS GD,
    SUM(Away.AwayTeamPoints) AS P
from 
(    
    select 
        DT.TEAMNAME AS AwayTeam,
        FF.FULLTIMEAWAYGOALS as GoalsFor,
        FF.FULLTIMEHOMEGOALS As GoalsAgainst,
        CASE WHEN AWAYTEAMWINNER = true then 1 else 0 end as AwayteamWin,
        CASE WHEN AWAYTEAMWINNER is null then 1 else 0 end as Draw,
        CASE when AWAYTEAMWINNER = false then 1 else 0 end as AwayTeamLoss,
        CASE WHEN AWAYTEAMWINNER = true then 3 when AWAYTEAMWINNER = FALSE then 0 else 1 end as AwayTeamPoints
    from Star.FACTFIXTURE FF
    INNER JOIN Star.DIMTEAM DT ON FF.AWAYTEAMKEY = DT.TEAMKEY
)Away
group by Away.AwayTeam
order by P DESC
