view: daily_spend {
  derived_table: {
    sql:  with get_analytics as (
      select analytics_timestamp as timestamp,
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
      from php.get_analytics
      where date(sent_at)=current_date
    )
    , distinct_events as (
      select distinct user_id, action, status, event_created_at, report_date
      from customers.all_customers
    )
    , paying as (
      select report_date
      , count(distinct user_id) as total_paying
      from distinct_events
      where action = 'subscription'
      and status = 'enabled'
      group by report_date
    )
    , trials as (
      select report_date
      , count(distinct user_id) as total_free_trials
      from distinct_events
      where action = 'subscription'
      and status = 'free_trial'
      group by report_date
    )
    , customers_analytics as (
    select get_analytics.timestamp,
    get_analytics.existing_free_trials,
    get_analytics.existing_paying,
    get_analytics.free_trial_churn,
    get_analytics.free_trial_converted,
    get_analytics.free_trial_created,
    get_analytics.paused_created,
    get_analytics.paying_churn,
    get_analytics.paying_created,
    COALESCE(trials.total_free_trials, get_analytics.total_free_trials) as total_free_trials,
    COALESCE(paying.total_paying, get_analytics.total_paying) as total_paying
    from get_analytics
    full join paying
    on paying.report_date = trunc(get_analytics.timestamp)
    full join trials
    on trials.report_date = trunc(get_analytics.timestamp)
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
      select date_start
      , channel
      , sum(spend) as spend
      from looker.get_other_marketing_spend
      where date(sent_at) = (select max(date(sent_at)) from looker.get_other_marketing_spend)
      and date_start is not null
      group by 1, 2
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
    -- pivot then unpivot on the channel to 'create' the records for channels that didn't have any reported spend so that I could forecast into them
    , channel_pivot as (
      select *
      from (select spend, channel, date_start from t1)
      PIVOT (sum(spend) FOR channel IN ('Apple Search Ads', 'Facebook', 'Bing Ads', 'Google', 'Google Campaign Manager', 'MNTN', 'TikTok'))
    )
    , channel_unpivot as (
      select *
      from channel_pivot
      UNPIVOT include nulls (
          channel_spend for channel in ("Apple Search Ads", "Facebook", "Bing Ads", "Google", "Google Campaign Manager", "MNTN", "TikTok")
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
    spend_forecast as channel_spend,
    sum(spend_forecast) over (partition by date_start) as spend
    from forecast
    inner join customers_analytics
    on date(date_start)=timestamp
    group by 1,2,3,4
    order by date_start desc ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension_group: timestamp {
    type: time
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

  measure: channel_spend {
    type: sum
    sql: ${TABLE}.channel_spend ;;
    value_format_name: usd
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
}
