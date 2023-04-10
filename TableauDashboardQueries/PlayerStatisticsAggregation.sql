select
    DP.PLAYERNAME as PlayerName,
    DP.PLAYERPOSITION as PlayerPostion,
    DT.TEAMNAME as PlayerTeam,
    SUM(GOALSTOTAL) as Goals,
    sum(case when minutesplayed <> 0 then 1 else 0 end) as GP,
    coalesce(sum(GOALSTOTAL)/GP,0) as GoalsPerGame,
    coalesce(sum(minutesplayed)/nullif(sum(GOALSTOTAL),0),0) as MinutesPerGoal,
    sum(goalassists) as Assists,
    sum(goalassists)/GP as AssistsPerGame,
    coalesce(sum(minutesplayed)/nullif(sum(goalassists),0),0) as MinutesPerAssist,
    Goals + Assists as GoalContributions,
    GoalContributions/GP as GoalContributionsPerGame,
    coalesce(sum(minutesplayed)/nullif(GoalContributions,0),0) as MinutesPerGoalContribution,
    sum(CARDSRED) as RedCards,
    sum(CARDSYELLOW) as YellowCards,
    sum(DRIBBLEATTEMPTS) as DribblesAttempts,
    sum(dribblesuccess) as SuccessFulDribbles,
    coalesce(coalesce(SuccessFulDribbles,0)/nullif(DribblesAttempts,0),0) as DribbleSuccessRate,
    sum(FOULSDRAWN) as FoulsDrawn,
    sum(duelstotal) as TotalDuels,
    sum(duelswon) as DuelWon,
    coalesce(coalesce(DuelWon,0)/nullif(TotalDuels,0),0) as DuelSuccessRate,
    avg(TOTALTACKLES) as TacklesPerGame,
    avg(INTERCEPTIONS) as InterceptionsPerGame,
    avg(blocks) as BlocksPerGame,
    sum(blocks) as BlocksTotal,
    sum(TOTALTACKLES) as TacklesTotal,
    sum(INTERCEPTIONS) as InterceptionsTotal,
    avg(SHOTSONTARGET) as AverageShotsOnTarget,
    avg(SHOTSTOTAL) as ShotsPerGame,
    sum(SHOTSTOTAL) as TotalShots,
    coalesce(coalesce(Goals,0)/nullif(TotalShots,0),0) as ShotConversionRate,
    sum(FOULSCOMMITED) as Fouls,
    avg(MINUTESPLAYED) as AverageMinutesPlayed,
    sum(minutesplayed) AS TotalMinutesPlayed,
    avg(RATING) as AverageRating,
    Avg(passaccuracy) as AveragePassAccuracy,
    sum(KEYPASSES) as KeyPasses,
    sum(totalpass) as TotalPasses,
    avg(TOTALPASS) as PassesPerGame,
    sum(PENALTIESSCORED) as PenaltiesScored,
    sum(PENALTIESMISSED) as PenaltiesMissed,
    sum(PENALTIESWON) as PenaltiesDrawn
from star.factfixtureplayerstatistics FFPS
INNER JOIN Star.DIMPLAYER DP ON FFPS.PLAYERKEY = DP.PLAYERKEY
INNER JOIN Star.DIMTEAM DT ON DT.TEAMKEY = DP.TEAMKEY
group by PlayerName,
        PlayerPostion,
        PlayerTeam
having GP <> 0

