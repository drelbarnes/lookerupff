view: daily_spend {
  derived_table: {
    sql:  with customers_analytics as (
        select
        "timestamp",
        existing_free_trials,
        existing_paying,
        free_trial_churn,
        free_trial_converted,
        free_trial_created,
        paused_created,
        paying_churn,
        paying_created,
        total_free_trials,
        total_paying
        from ${analytics_v2.SQL_TABLE_NAME}
      ),

      apple_perf as (
        select start_date as date_start,
        'Apple' as channel,
        sum(total_local_spend_amount) as spend,
        null::integer as impressions,
        null::integer as clicks,
        null::integer as installs,
        null::integer as conversions
         from php.get_apple_search_ads_campaigns
         group by 1,2
      ),
      fb_perf as (
        select i.date_start,
        'Facebook' as channel,
        sum(i.spend) as spend,
        sum(i.impressions) as impressions,
        sum(i.clicks) as clicks,
        sum(i.installs) as installs,
        sum(i.purchases_28d) as conversions
        from (
          select date_start,
          spend,
          impressions,
          clicks,
          null::integer as installs,
          COALESCE(actions_1d_view_app_custom_event_fb_mobile_purchase, 0) +
          COALESCE(actions_1d_view_offsite_conversion_fb_pixel_purchase, 0) +
          COALESCE(actions_28d_click_app_custom_event_fb_mobile_purchase, 0) +
          COALESCE(actions_28d_click_offsite_conversion_fb_pixel_purchase, 0) AS purchases_28d
          from facebook_ads.insights
        ) as i
        group by 1,2
      ),
      google_perf as (
        select apr.date_start,
          'Google' as channel,
          sum(campaigncost) as spend,
          sum(impressions) as impressions,
          sum(clicks) as clicks,
          sum(installs) as installs,
          sum(conversions) as conversions
          from (
            select  apr.date_start,
            sum((apr.cost/1000000)) as campaigncost,
            sum(impressions) as impressions,
            sum(clicks) as clicks,
            sum(null::integer) as installs,
            sum(conversions) as conversions
            from adwords.campaign_performance_reports as apr
            group by 1
          ) as apr
          inner join (
            select date_start
            , sum(COALESCE((cost/1000000),0 )) as spend
            from adwords.ad_performance_reports
            group by date_start
          ) as b
          on apr.date_start=b.date_start
          group by 1,2
      ),
      others_perf as (
        with p0 as (
          select
          other_marketing_spend_date as date_start
          ,other_marketing_spend_channel as channel
          , original_timestamp
          , other_marketing_spend_spend  as spend
          , 0 as impressions
          , 0 as clicks
          , 0 as installs
          ,0 as conversions
          , row_number() over (partition by other_marketing_spend_date , channel order by original_timestamp desc) as rn
          , count(*) as n

          FROM looker.get_channel_spend
          group by 1,2,3,4,5,6,7,8 order by 1 desc,2,3 desc,4
        )
        select
        date_start
        , channel
        , sum(spend) as spend
        , sum(impressions) as impressions
        , sum(clicks) as clicks
        , sum(installs) as installs
        , sum(conversions) as conversions
        from p0
        where rn = 1
        and date_start is not null
        group by 1,2
        order by date_start desc
      ),

      t1 as (
      -- manually adding spend to historical google adwords record
        select date_start,
        case
          when TO_CHAR(DATE_TRUNC('month', date_start), 'YYYY-MM') = '2018-07' then spend+(1440/31)
          when TO_CHAR(DATE_TRUNC('month', date_start), 'YYYY-MM') = '2018-06' then spend+(19000/30)
          when TO_CHAR(DATE_TRUNC('month', date_start ), 'YYYY-MM') = '2018-05' then spend+(10000/31)
          when TO_CHAR(DATE_TRUNC('month', date_start ), 'YYYY-MM') = '2018-04' then spend+(0/30)
          when TO_CHAR(DATE_TRUNC('month', date_start ), 'YYYY-MM') = '2018-03' then spend+(22018/31)
          when TO_CHAR(DATE_TRUNC('month', date_start ), 'YYYY-MM') = '2018-02' then spend+(21565/28)
          when TO_CHAR(DATE_TRUNC('month', date_start ), 'YYYY-MM') = '2018-01' then spend+(21570/31)
          when date(date_start) between timestamp '2018-08-11' and timestamp '2018-09-08' then spend+((288.37+87.27)/28)
          else spend
          end as spend,
        impressions,
        clicks,
        installs,
        conversions,
        channel
        from google_perf
        union all
        select date_start,
        spend,
        impressions,
        clicks,
        installs,
        conversions,
        channel
        from fb_perf
        union all
        select date_start,
        spend,
        impressions,
        clicks,
        installs,
        conversions,
        channel
        from apple_perf
        union all
        select date_start,
        spend,
        impressions,
        clicks,
        installs,
        conversions,
        channel
        from others_perf
      )
      -- pivot then unpivot on the channel to 'create' the records for channels that dont't have any reported spend so it can be forecasted
      , spend_pivot as (
        select *
        from (select spend, channel, date_start from t1)
        PIVOT (sum(spend) FOR channel IN ('Apple Search Ads', 'Facebook','OLV','Display', 'Bing Ads', 'Google', 'Google Campaign Manager', 'MNTN', 'TikTok', 'Viant', 'Tapjoy', 'Samsung', 'iHeart', 'Pinterest', 'Radio'))
      )
      , spend_unpivot as (
        select *
        from spend_pivot
        UNPIVOT include nulls (
            channel_spend for channel in ("Apple Search Ads", "Facebook","OLV","Display", "Bing Ads", "Google", "Google Campaign Manager", "MNTN", "TikTok", "Viant", "Tapjoy", "Samsung", "iHeart", "Pinterest", "Radio")
        )
      )
      , conversions_pivot as (
        select *
        from (select conversions, channel, date_start from t1)
        PIVOT (sum(conversions) FOR channel IN ('Apple Search Ads', 'Facebook','OLV','Display', 'Bing Ads', 'Google', 'Google Campaign Manager', 'MNTN', 'TikTok', 'Viant', 'Tapjoy', 'Samsung', 'iHeart', 'Pinterest', 'Radio'))
      )
      , conversions_unpivot as (
        select *
        from conversions_pivot
        UNPIVOT include nulls (
            channel_conversions for channel in ("Apple Search Ads", "Facebook","OLV","Display", "Bing Ads", "Google", "Google Campaign Manager", "MNTN", "TikTok", "Viant", "Tapjoy", "Samsung", "iHeart", "Pinterest", "Radio")
        )
      )
      -- Pivot and unpivot for impressions
      , impressions_pivot AS (
        SELECT *
        FROM (SELECT impressions, channel, date_start FROM t1)
        PIVOT (SUM(impressions) FOR channel IN ('Apple Search Ads', 'Facebook','OLV','Display', 'Bing Ads', 'Google', 'Google Campaign Manager', 'MNTN', 'TikTok', 'Viant', 'Tapjoy', 'Samsung', 'iHeart', 'Pinterest', 'Radio'))
      )
      , impressions_unpivot AS (
        SELECT *
        FROM impressions_pivot
        UNPIVOT include nulls (
          channel_impressions for channel in ("Apple Search Ads", "Facebook","OLV","Display", "Bing Ads", "Google", "Google Campaign Manager", "MNTN", "TikTok", "Viant", "Tapjoy", "Samsung", "iHeart", "Pinterest", "Radio")
        )
      )

      -- Pivot and unpivot for clicks
      , clicks_pivot AS (
        SELECT *
        FROM (SELECT clicks, channel, date_start FROM t1)
        PIVOT (SUM(clicks) FOR channel IN ('Apple Search Ads', 'Facebook','OLV','Display', 'Bing Ads', 'Google', 'Google Campaign Manager', 'MNTN', 'TikTok', 'Viant', 'Tapjoy', 'Samsung', 'iHeart', 'Pinterest', 'Radio'))
      )
      , clicks_unpivot AS (
        SELECT *
        FROM clicks_pivot
        UNPIVOT include nulls (
          channel_clicks for channel in ("Apple Search Ads", "Facebook","OLV","Display", "Bing Ads", "Google", "Google Campaign Manager", "MNTN", "TikTok", "Viant", "Tapjoy", "Samsung", "iHeart", "Pinterest", "Radio")
        )
      )

      -- Pivot and unpivot for installs
      , installs_pivot AS (
        SELECT *
        FROM (SELECT installs, channel, date_start FROM t1)
        PIVOT (SUM(installs) FOR channel IN ('Apple Search Ads', 'Facebook','OLV','Display', 'Bing Ads', 'Google', 'Google Campaign Manager', 'MNTN', 'TikTok', 'Viant', 'Tapjoy', 'Samsung', 'iHeart', 'Pinterest','Radio'))
      )
      , installs_unpivot AS (
        SELECT *
        FROM installs_pivot
        UNPIVOT include nulls (
          channel_installs for channel in ("Apple Search Ads", "Facebook","OLV","Display", "Bing Ads", "Google", "Google Campaign Manager", "MNTN", "TikTok", "Viant", "Tapjoy", "Samsung", "iHeart", "Pinterest", "Radio")
        )
      )

      -- Modify the combined_metrics CTE to include the new metrics
      , combined_metrics AS (
        SELECT
          s.date_start,
          s.channel,
          s.channel_spend,
          c.channel_conversions,
          i.channel_impressions,
          cl.channel_clicks,
          ins.channel_installs
        FROM spend_unpivot s
        LEFT JOIN conversions_unpivot c
          ON s.date_start = c.date_start AND s.channel = c.channel
        LEFT JOIN impressions_unpivot i
          ON s.date_start = i.date_start AND s.channel = i.channel
        LEFT JOIN clicks_unpivot cl
          ON s.date_start = cl.date_start AND s.channel = cl.channel
        LEFT JOIN installs_unpivot ins
          ON s.date_start = ins.date_start AND s.channel = ins.channel
      )
      -- we then create a spend_partition column that keeps track of the last non null spend value per channel
      , spend_partitioning as (
          select
          date_start,
          channel,
          channel_spend,
          channel_conversions,
          channel_impressions,
          channel_clicks,
          channel_installs,
          sum(case when channel_spend is null then 0 else 1 end) over (partition by channel order by date_start asc rows between unbounded preceding and current row) as spend_partition
          FROM combined_metrics
          where date_start is not null
      )

      -- we use the spend_partition column to project the first known value into all records with null spend
      , forecast as (
        select
          date_start,
          channel,
          channel_spend,
          channel_conversions,
          channel_impressions,
          channel_clicks,
          channel_installs,
          spend_partition,
          first_value(channel_spend) over (partition by channel, spend_partition order by date_start asc rows between unbounded preceding and current row) as spend_forecast
        from spend_partitioning
      )
      -- NOTE: we discontinued use of the spend forecasting logic.
      , outer_query as (
        select
          date_start,
          free_trial_created,
          paying_created,
          free_trial_created+paying_created as total_conversions,
          channel,
          channel_spend,
          channel_conversions,
          channel_impressions,
          channel_clicks,
          channel_installs,
          channel_spend/nullif(channel_conversions,0) as channel_cpa,
          sum(channel_spend) over (partition by date_start) as spend,
          sum(channel_conversions) over (partition by date_start) as attributed_conversions,
          sum(channel_impressions) over (partition by date_start) as impressions,
          sum(channel_clicks) over (partition by date_start) as clicks,
          sum(channel_installs) over (partition by date_start) as installs,
          total_conversions/nullif(attributed_conversions,0) as platform_attribution_ratio
        from forecast
        inner join customers_analytics
        on date(date_start) = timestamp
        group by 1,2,3,4,5,6,7,8,9,10
        order by date_start desc
      )
      select * from outer_query where channel_spend is not null order by date_start desc ;;
    datagroup_trigger: upff_acquisition_reporting
    distribution_style: all
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension_group: timestamp {
    type: time
    timeframes: [raw, time, date, day_of_week, week, month]
    sql: ${TABLE}.date_start ;;
  }

  dimension: channel {
    type: string
    sql: ${TABLE}.channel ;;
  }

  dimension_group: week_start_tuesday {
    type: time
    timeframes: [raw, date, day_of_week, week, month]
    sql: CASE
      WHEN ${timestamp_day_of_week} = 'Tuesday' THEN ${timestamp_date}
      WHEN ${timestamp_day_of_week} = 'Wednesday' THEN dateadd(days, -1, ${timestamp_date})
      WHEN ${timestamp_day_of_week} = 'Thursday' THEN dateadd(days, -2, ${timestamp_date})
      WHEN ${timestamp_day_of_week} = 'Friday' THEN dateadd(days, -3,  ${timestamp_date})
      WHEN ${timestamp_day_of_week} = 'Saturday' THEN dateadd(days, -4, ${timestamp_date})
      WHEN ${timestamp_day_of_week} = 'Sunday' THEN dateadd(days, -5, ${timestamp_date})
      WHEN ${timestamp_day_of_week} = 'Monday' THEN dateadd(days, -6, ${timestamp_date})
      END;;
    datatype: date
  }
  ## filter determining time range for all "A" measures
  filter: time_a {
    type: date_time
  }

  ## flag for "A" measures to only include appropriate time range
  dimension: group_a {
    hidden: yes
    type: yesno
    sql: {% condition time_a %} ${timestamp_raw} {% endcondition %} ;;
  }

  ## filtered measure A
  measure: spend_a {
    type: sum_distinct
    sql_distinct_key: ${timestamp_date} ;;
    sql: ${TABLE}.spend ;;
    value_format_name: usd
    filters: [group_a: "yes"]
  }

  measure: free_trial_created_a {
    type: sum_distinct
    sql_distinct_key: ${timestamp_date} ;;
    sql: ${TABLE}.free_trial_created ;;
    filters: [group_a: "yes"]
  }

  measure: paying_created_a {
    type: sum_distinct
    sql_distinct_key: ${timestamp_date} ;;
    sql: ${TABLE}.paying_created ;;
    filters: [group_a: "yes"]
  }

  measure: total_conversions_a {
    type: number
    sql: ${free_trial_created_a}+${paying_created_a} ;;
  }

  measure: CPFT_a {
    type: number
    sql: ${spend_a}/nullif(${free_trial_created_a},0) ;;
    value_format_name: usd
  }

  ## filter determining time range for all "B" measures
  filter: time_b {
    type: date_time
  }

  ## flag for "B" measures to only include appropriate time range
  dimension: group_b {
    hidden: yes
    type: yesno
    sql: {% condition time_b %} ${timestamp_raw} {% endcondition %} ;;
  }

  measure: spend_b {
    type: sum_distinct
    sql_distinct_key: ${timestamp_date} ;;
    sql: ${TABLE}.spend ;;
    value_format_name: usd
    filters: [group_b: "yes"]
  }

  measure: free_trial_created_b {
    type: sum_distinct
    sql_distinct_key: ${timestamp_date} ;;
    sql: ${TABLE}.free_trial_created ;;
    filters: [group_b: "yes"]
  }

  measure: paying_created_b {
    type: sum_distinct
    sql_distinct_key: ${timestamp_date} ;;
    sql: ${TABLE}.paying_created ;;
    filters: [group_b: "yes"]
  }

  measure: total_conversions_b {
    type: number
    sql: ${free_trial_created_b}+${paying_created_b} ;;
  }

  measure: CPFT_b {
    type: number
    sql: ${spend_b}/nullif(${free_trial_created_b},0) ;;
    value_format_name: usd
  }

  measure: channel_spend {
    type: sum
    sql: ${TABLE}.channel_spend ;;
    value_format_name: usd
  }

  measure: channel_spend_a {
    type: sum
    sql: ${TABLE}.channel_spend ;;
    value_format_name: usd
    filters: [group_a: "yes"]
  }

  measure: channel_spend_b {
    type: sum
    sql: ${TABLE}.channel_spend ;;
    value_format_name: usd
    filters: [group_b: "yes"]
  }

  measure: spend {
    type: sum_distinct
    sql_distinct_key: ${timestamp_date} ;;
    sql: ${TABLE}.spend ;;
    value_format_name: usd
  }

  measure: channel_conversions {
    type: sum
    sql: ${TABLE}.channel_conversions ;;
    value_format: "0"
  }

  measure: channel_conversions_a {
    type: sum
    sql: ${TABLE}.channel_conversions ;;
    filters: [group_a: "yes"]
  }

  measure: channel_conversions_b {
    type: sum
    sql: ${TABLE}.channel_conversions ;;
    filters: [group_b: "yes"]
  }

  measure: channel_impressions {
    type: sum
    sql: ${TABLE}.channel_impressions ;;
    value_format: "0"
  }

  measure: channel_impressions_a {
    type: sum
    sql: ${TABLE}.channel_impressions ;;
    filters: [group_a: "yes"]
  }

  measure: channel_impressions_b {
    type: sum
    sql: ${TABLE}.channel_impressions ;;
    filters: [group_b: "yes"]
  }

  measure: channel_clicks {
    type: sum
    sql: ${TABLE}.channel_clicks ;;
    value_format: "0"
  }

  measure: channel_clicks_a {
    type: sum
    sql: ${TABLE}.channel_clicks ;;
    filters: [group_a: "yes"]
  }

  measure: channel_clicks_b {
    type: sum
    sql: ${TABLE}.channel_clicks ;;
    filters: [group_b: "yes"]
  }

  measure: channel_installs {
    type: sum
    sql: ${TABLE}.channel_installs ;;
    value_format: "0"
  }

  measure: channel_installs_a {
    type: sum
    sql: ${TABLE}.channel_installs ;;
    filters: [group_a: "yes"]
  }

  measure: channel_installs_b {
    type: sum
    sql: ${TABLE}.channel_installs ;;
    filters: [group_b: "yes"]
  }

  measure: attributed_conversions {
    type: sum_distinct
    sql_distinct_key: ${timestamp_date} ;;
    sql: ${TABLE}.attributed_conversions ;;
    value_format: "0"
  }

  measure: free_trial_created {
    type: sum_distinct
    sql_distinct_key: ${timestamp_date} ;;
    sql: ${TABLE}.free_trial_created ;;
  }

  measure: paying_created {
    type: sum_distinct
    sql_distinct_key: ${timestamp_date} ;;
    sql: ${TABLE}.paying_created ;;
  }

  measure: total_conversions {
    type: number
    sql: ${free_trial_created}+${paying_created} ;;
  }

  measure: CPFT {
    type: number
    sql: ${spend}/nullif(${free_trial_created},0) ;;
    value_format_name: usd
  }

  measure: channel_cpa {
    type: number
    sql: ${channel_spend}/nullif(${channel_conversions},0) ;;
    value_format_name: usd
  }

  measure: platform_attribution_ratio {
    type: number
    sql: ${total_conversions}/nullif(${attributed_conversions},0) ;;
  }

  set: detail {
    fields: [timestamp_time, spend]
  }

  ## PoP dimensions and measures created for H16 analysis

  ## ------------------ HIDDEN HELPER DIMENSIONS  ------------------ ##

  dimension: days_from_start_a {
    hidden: no
    group_label: "Time Comparison Filters"
    type: number
    sql: DATEDIFF('day',  {% date_start time_a %}, ${timestamp_date}) ;;
  }

  dimension: days_from_start_b {
    hidden: no
    group_label: "Time Comparison Filters"
    type: number
    sql: DATEDIFF('day',  {% date_start time_b %}, ${timestamp_date}) ;;
  }


  ## ------------------ DIMENSIONS TO PLOT ------------------ ##

  dimension: days_from_first_period {
    label: "Day of Period"
    description: "Select for Grouping (Rows)"
    group_label: "Time Comparison Filters"
    type: number
    sql:
            CASE
            WHEN ${days_from_start_b} >= 0
            THEN ${days_from_start_b}
            WHEN ${days_from_start_a} >= 0
            THEN ${days_from_start_a}
            END;;
  }


  dimension: period_selected {
    label: "Period"
    description: "Select for Comparison (Pivot)"
    group_label: "Time Comparison Filters"
    type: string
    sql:
            CASE
                WHEN {% condition time_a %}${timestamp_raw} {% endcondition %}
                THEN 'Period A'
                WHEN {% condition time_b %}${timestamp_raw} {% endcondition %}
                THEN 'Period B'
                END ;;
  }

  dimension: period_selected_2 {
    label: "Period 2"
    description: "Select for Comparison (Pivot)"
    group_label: "Time Comparison Filters"
    type: string
    sql:
            CASE
            WHEN ${days_from_start_b} >= 0
                THEN 'Period B'
            WHEN ${days_from_start_a} >= 0
                THEN 'Period A'
            END;;
  }

}
