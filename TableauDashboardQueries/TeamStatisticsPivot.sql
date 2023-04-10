select
    p.*
from 
(
    select
        DT.TEAMNAME AS Team,
        DTS.STATISTICTYPE as StatisticType,
        coalesce(avg(replace(FTS.STATISTICVALUE,'%')),0) AS AverageValue
    from star.FACTFIXTURETEAMSTATISTICS FTS
    inner join Star.DIMTEAM DT ON FTS.TEAMKEY = DT.TEAMKEY
    INNER JOIN Star.DIMSTATISTICTYPE DTS ON FTS.STATISTICTYPEKEY = DTS.STATISTICTYPEKEY
    group by Team,StatisticType
)A
pivot (avg(AverageValue) for StatisticType in ('Shots insidebox',
                                               'Ball Possession',
                                               'Total passes',
                                               'Yellow Cards',
                                               'Red Cards',
                                               'Corner Kicks',
                                               'Shots on Goal',
                                               'Passes accurate',
                                               'Shots off Goal',
                                               'Total Shots',
                                               'Offsides',
                                               'Blocked Shots',
                                               'Shots outsidebox',
                                               'Goalkeeper Saves',
                                               'Passes %',
                                               'Fouls'
                                              )) as p 
                                              (
                                                Team,
                                                ShotsInsideBox,
                                                BallPosession,
                                                TotalPasses,
                                                YellowCards,
                                                RedCards,
                                                CornerKicks,
                                                ShotsOnGoal,
                                                PassesAccurate,
                                                ShotsOffGoal,
                                                TotalShots,
                                                Offsides,
                                                BlockedShots,
                                                ShotsOutsidebox,
                                                GoalKeeperSaves,
                                                PassesPercentage,
                                                Fouls
                                              )
