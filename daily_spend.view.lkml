view: daily_spend {
  derived_table: {
    sql:  with get_analytics_p0 as (
        select
        analytics_timestamp as timestamp
        , existing_free_trials
        , existing_paying
        , free_trial_churn
        , free_trial_converted
        , free_trial_created
        , paused_created
        , paying_churn
        , paying_created
        , total_free_trials
        , total_paying
        , row_number() over (partition by analytics_timestamp order by sent_at) as n
        from php.get_analytics
        where date(sent_at)=current_date
      )
      , apple_subs as (
        select
        report_date
        , app_store_connect_subscribers_total
        , vimeo_ott_subscribers_total
        from
        looker.get_app_store_connect_subs
        where date(sent_at)=current_date
      )
      , get_analytics_p1 as (
        select
        a.timestamp
        , a.existing_free_trials
        , a.existing_paying
        , a.free_trial_churn
        , a.free_trial_converted
        , a.free_trial_created
        , a.paused_created
        , a.paying_churn
        , a.paying_created
        , b.total_free_trials
        -- , b.total_paying
        , (b.total_paying-c.vimeo_ott_subscribers_total+c.app_store_connect_subscribers_total) as total_paying
        from (select * from get_analytics_p0 where n=1) as a
        inner join (select * from get_analytics_p0 where n=2) as b
        on date(a.timestamp) = date(b.timestamp)
        inner join (
          select
          report_date
          , app_store_connect_subscribers_total
          , vimeo_ott_subscribers_total
          from
          looker.get_app_store_connect_subs
          where date(sent_at)=current_date
        ) c
        on date(a.timestamp) = date(c.report_date)
      )
      -- BACK UP SOURCE INCASE OF API OUTAGE
      , distinct_events as (
        select distinct user_id
        , action
        , status
        , frequency
        , to_timestamp(event_created_at, 'YYYY-MM-DD HH24:MI:SS') as event_created_at
        , to_date(event_created_at, 'YYYY-MM-DD') as event_date
        , to_date(report_date, 'YYYY-MM-DD') as report_date
        from customers.all_customers
        where action = 'subscription'
      )
      , total_counts as (
        select report_date
        , count(distinct case when status = 'enabled' then user_id end) as total_paying
        , lag(total_paying, 1) over (order by report_date) as existing_paying
        , count(distinct case when status = 'free_trial' then user_id end) as total_free_trials
        , lag(total_free_trials, 1) over (order by report_date) as existing_free_trials
        from distinct_events
        group by 1
      )
      , customers_analytics as (
        with p0 as (
          select get_analytics_p1.timestamp,
          coalesce(get_analytics_p1.existing_free_trials, total_counts.existing_free_trials) as existing_free_trials,
          coalesce(get_analytics_p1.existing_paying, total_counts.existing_paying) as existing_paying,
          get_analytics_p1.free_trial_churn,
          get_analytics_p1.free_trial_converted,
          get_analytics_p1.free_trial_created,
          get_analytics_p1.paused_created,
          get_analytics_p1.paying_churn,
          get_analytics_p1.paying_created,
          coalesce(get_analytics_p1.total_free_trials, total_counts.total_free_trials) as total_free_trials,
          coalesce(get_analytics_p1.total_paying, total_counts.total_paying) as total_paying
          from get_analytics_p1
          full join total_counts
          on total_counts.report_date = trunc(get_analytics_p1.timestamp)
        )
        select * from p0 where date(timestamp) < current_date
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
      -- pivot then unpivot on the channel to 'create' the records for channels that didn't have any reported spend so that I could forecast into them
      , channel_pivot as (
        select *
        from (select spend, channel, date_start from t1)
        PIVOT (sum(spend) FOR channel IN ('Apple Search Ads', 'Facebook', 'Bing Ads', 'Google', 'Google Campaign Manager', 'MNTN', 'TikTok', 'Viant'))
      )
      , channel_unpivot as (
        select *
        from channel_pivot
        UNPIVOT include nulls (
            channel_spend for channel in ("Apple Search Ads", "Facebook", "Bing Ads", "Google", "Google Campaign Manager", "MNTN", "TikTok", "Viant")
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
