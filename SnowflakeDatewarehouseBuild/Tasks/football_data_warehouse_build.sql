
create or replace task public.football_data_warehouse_build
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
    SCHEDULE = 'USING CRON 0 8 * * * UTC'
as 
begin

    use database football;

    use warehouse footballapi;

    call staging.usp_source_sql_fixtures();

    call staging.usp_source_sql_coachs();

    call staging.usp_source_sql_players();

    call staging.usp_source_sql_venues();

    call staging.usp_source_sql_teams();

    call staging.usp_merge_dim_referee();

    call staging.usp_merge_dim_formation();

    call staging.usp_merge_dim_position();

    call staging.usp_merge_dim_league_round();

    call staging.usp_merge_dim_event_detail();

    call staging.usp_merge_dim_statistic_type();

    call staging.usp_merge_venues();

    call staging.usp_merge_dim_coach();

    call staging.usp_merge_dim_player();

    call staging.usp_merge_dim_team();

    call staging.usp_merge_fact_fixture();

    call staging.usp_merge_fact_fixture_event();

    call staging.usp_merge_fact_fixture_lineup();

    call staging.usp_merge_fact_fixture_player_statistics();

    call staging.usp_merge_fact_fixture_team_statistics();

    select staging.dynamo_db_build('Begin Build of Reporting Layer');

end;

--execute task football_data_warehouse_build