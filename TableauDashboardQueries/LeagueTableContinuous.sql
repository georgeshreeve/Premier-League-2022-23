with cte_1 as 
(
    select 
        DLR.LEAGUEROUND as MatchDay,
        DT.TEAMNAME as Team
    from star.dimleagueround DLR 
    LEFT JOIN Star.DIMTEAM DT
    where DT.TEAMNAME <> 'Unknown' and DLR.LEAGUEROUND <> 'Unknown'
)
,
cte_2 as 
(
    select
        cast(ltrim(right(c.MatchDay,2)) as int) as MatchDay,
        c.Team,
        coalesce(a.P,0) as P,
        coalesce(a.GD,0) as GD,
        coalesce(a.GF,0) as GF,
        coalesce(a.GA,0) as GA,
        coalesce(a.W,0) as W,
        coalesce(a.D,0) as D,
        coalesce(a.L,0) as L
    from cte_1 c left join
    (
        select
            DLR.LEAGUEROUND as MatchDay,
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
        inner join star.DIMLEAGUEROUND DLR ON FF.LEAGUEROUNDKEY = DLR.LEAGUEROUNDKEY
        group by MatchDay, Team

        union

        select
            DLR.LEAGUEROUND as MatchDay,
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
        inner join Star.DIMLEAGUEROUND DLR ON FF.LEAGUEROUNDKEY = DLR.LEAGUEROUNDKEY
        group by MatchDay, Team
    )a on c.MatchDay = a.MatchDay and c.Team = a.Team
)
,
cte_3 as 
(
    select
        MatchDay,
        Team,
        sum(P) over (partition by Team order by MatchDay rows between unbounded preceding and current row) as CumulativePoints,
        sum(GD) over (partition by Team order by MatchDay rows between unbounded preceding and current row) as CumulativeGoalDifference,
        sum(GF) over (partition by Team order by MatchDay rows between unbounded preceding and current row) as CumulativeGoalFor,
        sum(GA) over (partition by Team order by MatchDay rows between unbounded preceding and current row) as CumulativeGoalAgainst,
        sum(W) over (partition by Team order by MatchDay rows between unbounded preceding and current row) as CumulativeWins,
        sum(D) over (partition by Team order by MatchDay rows between unbounded preceding and current row) as CumulativeDraw,
        sum(L) over (partition by Team order by MatchDay rows between unbounded preceding and current row) as CumulativeLoss
    from cte_2
    order by MatchDay,CumulativePoints desc, CumulativeGoalDifference desc,CumulativeGoalFor desc
)
select 
    MatchDay,
    Team,
    CumulativePoints,
    CumulativeGoalFor,
    CumulativeGoalAgainst,
    CumulativeGoalDifference,
    CumulativeWins,
    CumulativeDraw,
    CumulativeLoss,
    RANK() OVER (PARTITION BY MatchDay order by CumulativePoints desc,CumulativeGoalDifference desc,CumulativeGoalFor desc,Team) as Position
from cte_3
order by MatchDay,Position