# raw efficiency
# pace
# conference (one-hot encoded)
# adjusted efficiency
# adjusted pace
# record
# opponents' mean adjusted efficiency
# consistency (SD of adjusted offensive/defensive efficiency)

# target: number of wins in the NCAA tournament

with
  games as (
    select alias, market, season, opp_alias, opp_market, conf_alias, tournament_type, home_team, neutral_site,
      CAST(win as int64) as wins,
      CAST(NOT win AS int64) as losses,
      field_goals_att-offensive_rebounds+turnovers+(0.4*free_throws_att) as off_possessions,
      opp_field_goals_att-opp_offensive_rebounds+opp_turnovers+(0.4*opp_free_throws_att) as def_possessions,
      points_game as off_points,
      opp_points_game as def_points
    from `bigquery-public-data.ncaa_basketball.mbb_teams_games_sr`
  ),

  ref as (
    select alias, market, season,
      safe_divide(sum(case when off_possessions>0 then off_points else 0 end),sum(off_possessions)) as off_ptsperposs,
      safe_divide(sum(case when def_possessions>0 then def_points else 0 end),sum(def_possessions)) as def_ptsperposs,
      safe_divide(sum(off_possessions),sum(case when off_possessions>0 then 1 else 0 end)) as off_possperg
    from games
    where (tournament_type not in ('First Four','National Championship','Final Four','Midwest Regional','South Regional','West Regional','East Regional')
      or tournament_type is null)
    group by alias, market, season
  ),

  games2 as (
    select games.alias, games.market, games.season, opp_alias, opp_market, conf_alias, tournament_type, home_team, neutral_site,
      wins,
      losses,
      off_possessions,
      def_possessions,
      off_points,
      def_points,
      ref1.off_ptsperposs,
      ref1.def_ptsperposs,
      ref1.off_possperg,
      ref2.off_ptsperposs as opp_off_ptsperposs,
      ref2.def_ptsperposs as opp_def_ptsperposs,
      ref2.off_possperg as opp_off_possperg
    from games
    left join ref as ref1
    on games.alias=ref1.alias and games.market=ref1.market and games.season=ref1.season
    left join ref as ref2
    on games.opp_alias=ref2.alias and games.opp_market=ref2.market and games.season=ref2.season
    where (tournament_type not in ('First Four','National Championship','Final Four','Midwest Regional','South Regional','West Regional','East Regional')
      or tournament_type is null)
  ),

  agg as (
    select alias, market, season, conf_alias,
      sum(wins) as wins,
      sum(losses) as losses,
      safe_divide(sum(wins),count(*)) as win_pct,
      max(off_ptsperposs) as off_ptsperposs,
      max(def_ptsperposs) as def_ptsperposs,
      max(off_possperg) as possperg
    from games2
    group by alias, market, season, conf_alias
  ),

  homeawaydiff as (
    select season, sum(case when home_team then off_points else 0 end)/sum(case when home_team then off_possessions else 0 end)
      - sum(case when not home_team then off_points else 0 end)/sum(case when not home_team then off_possessions else 0 end) as a
    from games2
    where (neutral_site is null or not neutral_site)
    and (off_possessions>0 and def_possessions>0)
    group by season
  ),

  adj_games as (
    select alias, market, games2.season,
      (off_points/off_possessions)/
        ((off_ptsperposs+opp_def_ptsperposs+
          (case when neutral_site then 0 when home_team then a else a*-1 end)
          )/2)
        *off_ptsperposs
        as adj_off_ptsperposs,
      (def_points/def_possessions)/
        ((def_ptsperposs+opp_off_ptsperposs+
          (case when neutral_site then 0 when home_team then a*-1 else a end)
          )/2)
        *def_ptsperposs
        as adj_def_ptsperposs,
      off_possessions/((off_possperg+opp_off_possperg)/2)
        *off_possperg
        as adj_poss
    from games2
    left join homeawaydiff
    on games2.season=homeawaydiff.season
    where (off_possessions>0 and def_possessions>0)
  ),

  adj_agg as (
    select alias, market, season,
      avg(adj_off_ptsperposs) as adj_off_ptsperposs,
      avg(adj_def_ptsperposs) as adj_def_ptsperposs,
      avg(adj_poss) as adj_possperg,
      stddev(adj_off_ptsperposs) as off_sd,
      stddev(adj_def_ptsperposs) as def_sd
    from adj_games
    group by alias, market, season
  ),

  sos_agg as (
    select games2.alias, games2.market, games2.season,
      avg(adj_off_ptsperposs-adj_def_ptsperposs) as opp_adj_net_ptsperposs
    from games2
    left join adj_agg
    on games2.opp_alias=adj_agg.alias and games2.opp_market=adj_agg.market and games2.season=adj_agg.season
    group by games2.alias, games2.market, games2.season
  ),

  seeds as (
    select distinct alias, market, season, seed
    from (
      select alias, market, season, seed
      from (
        select distinct win_alias as alias, win_market as market, academic_year as season, cast(win_seed as int64) as seed
        from `bigquery-public-data.ncaa_basketball.mbb_historical_tournament_games`
      )
      union all (
        select distinct lose_alias as alias, lose_market as market, academic_year as season, cast(lose_seed as int64) as seed
        from `bigquery-public-data.ncaa_basketball.mbb_historical_tournament_games`
      )
    )
  ),

  tourney as (
    select alias, market, season,
      sum(wins) as tournament_wins
    from games
    where tournament_type in ('National Championship','Final Four','Midwest Regional','South Regional','West Regional','East Regional')
    group by alias, market, season
  ),

  combined as (
    select agg.alias, agg.market, agg.season, conf_alias,
      wins,
      losses,
      win_pct,
      off_ptsperposs,
      def_ptsperposs,
      off_ptsperposs-def_ptsperposs as net_ptsperposs,
      possperg,
      adj_off_ptsperposs,
      adj_def_ptsperposs,
      adj_off_ptsperposs-adj_def_ptsperposs as adj_net_ptsperposs,
      adj_possperg,
      off_sd,
      def_sd,
      opp_adj_net_ptsperposs,
      seed,
      tournament_wins
    from agg
    left join adj_agg
    on agg.alias=adj_agg.alias and agg.market=adj_agg.market and agg.season=adj_agg.season
    left join sos_agg
    on agg.alias=sos_agg.alias and agg.market=sos_agg.market and agg.season=sos_agg.season
    left join seeds
    on agg.alias=seeds.alias and agg.market=seeds.market and agg.season=seeds.season
    left join tourney
    on agg.alias=tourney.alias and agg.market=tourney.market and agg.season=tourney.season
  )

select *
from combined
where tournament_wins is not null
order by season desc, tournament_wins desc
