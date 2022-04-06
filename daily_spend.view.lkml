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
        , 'others' as channel
        , sum(spend) as spend
        from looker.get_other_marketing_spend
        where sent_at = (select max(sent_at) from looker.get_other_marketing_spend)
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

      select date_start as timestamp,
      free_trial_created,
      sum(spend) as spend,
      sum(spend)/free_trial_created
      from t1
      inner join customers_analytics
      on date(date_start)=timestamp
      group by 1,2
      order by 1 desc ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension_group: timestamp {
    type: time
    sql: ${TABLE}.timestamp ;;
  }

  measure: spend {
    type: sum
    sql: ${TABLE}.spend ;;
    value_format_name: usd
  }

  measure: free_trial_created {
    type: sum
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
