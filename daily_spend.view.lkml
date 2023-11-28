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
        sum(total_local_spend_amount) as spend,
        'Apple' as channel
         from php.get_apple_search_ads_campaigns
         group by 1,3
      ),

      fb_perf as (
        select i.date_start,
          sum(i.spend) as spend,
          'Facebook' as channel
          from facebook_ads.insights as i
          group by  1,3
      ),

      google_perf as (
        select apr.date_start,
          sum(campaigncost) as spend,
          'Google' as channel
          from (
            select  apr.date_start,
            sum((apr.cost/1000000)) as campaigncost
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
          group by  1,3
      ),

      others_perf as (
        with p0 as (
          select date_start
          , channel
          , original_timestamp
          , spend
          , row_number() over (partition by date_start, channel order by original_timestamp desc) as rn
          , count(*) as n
          FROM looker.get_other_marketing_spend
          group by 1,2,3,4 order by 1 desc,2,3 desc,4
        )
        select
        date_start
        , channel
        , sum(spend) as spend
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
        channel
        from google_perf
        union all
        select date_start,
        spend,
        channel
        from fb_perf
        union all
        select date_start,
        spend,
        channel
        from apple_perf
        union all
        select date_start
        , spend
        , channel
        from others_perf
      )
      -- pivot then unpivot on the channel to 'create' the records for channels that dont't have any reported spend so it can be forecasted
      , channel_pivot as (
        select *
        from (select spend, channel, date_start from t1)
        PIVOT (sum(spend) FOR channel IN ('Apple Search Ads', 'Facebook', 'Bing Ads', 'Google', 'Google Campaign Manager', 'MNTN', 'TikTok', 'Viant', 'Tapjoy', 'Samsung'))
      )
      , channel_unpivot as (
        select *
        from channel_pivot
        UNPIVOT include nulls (
            channel_spend for channel in ("Apple Search Ads", "Facebook", "Bing Ads", "Google", "Google Campaign Manager", "MNTN", "TikTok", "Viant", "Tapjoy", "Samsung")
        )
      )
      -- we then create a spend_partition column that keeps track of the last non null spend value per channel
      , spend_partitioning as (
          select
          date_start,
          channel,
          channel_spend,
          sum(case when channel_spend is null then 0 else 1 end) over (partition by channel order by date_start asc rows between unbounded preceding and current row) as spend_partition
          FROM channel_unpivot
          -- the previous unpivot includes null date_starts so we filter them out
          where date_start is not null
      )
      -- we use the spend_partition column to project the first known value into all records with null spend
      , forecast as (
        select
          date_start,
          channel,
          channel_spend,
          spend_partition,
          first_value(channel_spend) over (partition by channel, spend_partition order by date_start asc rows between unbounded preceding and current row) as spend_forecast
        from spend_partitioning
      )
      select date_start,
      free_trial_created,
      channel,
      -- spend_forecast as channel_spend,
      channel_spend,
      -- sum(spend_forecast) over (partition by date_start) as spend,
      sum(channel_spend)over (partition by date_start) as spend
      from forecast
      inner join customers_analytics
      on date(date_start)=timestamp
      group by 1,2,3,4
      order by date_start desc ;;
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

  measure: CPFT_a {
    type: number
    sql: ${spend_a}/${free_trial_created_a} ;;
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

  measure: CPFT_b {
    type: number
    sql: ${spend_b}/${free_trial_created_b} ;;
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

  measure: free_trial_created {
    type: sum_distinct
    sql_distinct_key: ${timestamp_date} ;;
    sql: ${TABLE}.free_trial_created ;;
  }

  measure: CPFT {
    type: number
    sql: ${spend}/${free_trial_created} ;;
    value_format_name: usd
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
