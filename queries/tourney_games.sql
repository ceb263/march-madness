select distinct alias, market, season, opp_alias, opp_market, win
from `bigquery-public-data.ncaa_basketball.mbb_teams_games_sr`
where tournament_type in ('National Championship','Final Four','Midwest Regional','South Regional','West Regional','East Regional')
order by season desc
