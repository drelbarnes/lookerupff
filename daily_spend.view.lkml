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
      where date(sent_at)=current_date),
      -- Bolting on Vimeo OTT active customer report as of 2021-12-23 to try to get more accurate sub counts
      active_customer_report as (
        -- The active_customers table only has accurate data from 2021-12-23 onwards.
      with customers as (
      select
      trunc(report_date) as r_date
      , trunc(dateadd(day,-1,trunc(report_date))) as date_offset
      , user_id
      , email
      , subscriptions_in_free_trial
      , RANK() over (PARTITION BY user_id ORDER BY trunc(report_date) ASC)
      , tickets_is_subscriptions
      , ticket_status
      , tickets_subscription_frequency
      , customer_video_notifications
      from customers.active_customers
      where trunc(report_date) >= '2021-12-23'
      )
      , trialists as (
      select *
      from customers
      where subscriptions_in_free_trial = 'Yes' and tickets_is_subscriptions = 'Yes' and ticket_status = 'enabled'
      )
      , new_trials as (
      select
      count(user_id) as free_trial_created,
      date_offset
      from trialists
      where rank = 1 AND date_offset > '2021-12-24'
      group by date_offset
      order by date_offset desc
      )
      , t as (
      select
      count(user_id) as free_trials,
      date_offset
      from trialists
      group by date_offset
      order by date_offset desc
      )
      , paying as (
      select *
      from customers
      where subscriptions_in_free_trial = 'No' and tickets_is_subscriptions = 'Yes' and ticket_status = 'enabled'
      )
      , p as (
      select
      count(user_id) as paying_subs,
      date_offset
      from paying
      where subscriptions_in_free_trial = 'No' and tickets_is_subscriptions = 'Yes' and ticket_status = 'enabled'
      group by date_offset
      order by date_offset desc
      )
      select p.date_offset,
      lag(free_trials, 1) OVER(ORDER BY t.date_offset asc) AS existing_free_trials,
      lag(paying_subs, 1) OVER(ORDER BY p.date_offset asc) AS existing_paying,
      new_trials.free_trial_created,
      t.free_trials,
      p.paying_subs
      from p
      inner join t
      on t.date_offset = p.date_offset
      inner join new_trials
      on new_trials.date_offset = p.date_offset
      )
      , customers_analytics as (
        select get_analytics.timestamp,
        get_analytics.id,
        CASE
          when get_analytics.timestamp < '2021-12-23' then get_analytics.existing_free_trials
          else active_customer_report.existing_free_trials
          end as existing_free_trials,
        CASE
          when get_analytics.timestamp < '2021-12-23' then get_analytics.existing_paying
          else active_customer_report.existing_paying
          end as existing_paying,
        get_analytics.free_trial_churn, -- not used in derived table calcs
        get_analytics.free_trial_converted, -- not used in derived table calcs
        CASE
          when get_analytics.timestamp < '2021-12-23' then get_analytics.free_trial_created
          else active_customer_report.free_trial_created
          end as free_trial_created,
        get_analytics.paused_created, -- not used in derived table calcs
        get_analytics.paying_churn,
        get_analytics.paying_created,
        CASE
          when get_analytics.timestamp < '2021-12-23' then get_analytics.total_free_trials
          else active_customer_report.free_trials
          end as total_free_trials,
        CASE
          when get_analytics.timestamp < '2021-12-23' then get_analytics.total_paying
          else active_customer_report.paying_subs
          end as total_paying
        from get_analytics
        inner join active_customer_report
        on active_customer_report.date_offset = get_analytics.timestamp
      ),
      apple_perf as (select start_date as date_start,
                            sum(total_local_spend_amount) as spend,
                            'Apple' as channel
                     from php.get_apple_search_ads_campaigns
                     group by 1,3),

      fb_perf as (select
                i.date_start,
                sum(i.spend) as spend,
                'Facebook' as channel
          from  facebook_ads.insights as i
      group by  1,3
      ),
      google_perf as (
        select  apr.date_start,
                sum(campaigncost) as spend,
                'Google' as channel
          from  (select  apr.date_start,
                sum((apr.cost/1000000)) as campaigncost
          from  adwords.campaign_performance_reports as apr
          group by  1) as apr
          inner join
          (select date_start,
  sum(COALESCE((cost/1000000),0 )) as spend from adwords.ad_performance_reports
  group by date_Start) as b on apr.date_start=b.date_start
          group by  1,3
      ),
        t1 as (select date_start,
case when TO_CHAR(DATE_TRUNC('month', date_start), 'YYYY-MM') = '2018-07' then spend+(1440/31)
     when TO_CHAR(DATE_TRUNC('month', date_start), 'YYYY-MM') = '2018-06' then spend+(19000/30)
     when TO_CHAR(DATE_TRUNC('month', date_start ), 'YYYY-MM') = '2018-05' then spend+(10000/31)
     when TO_CHAR(DATE_TRUNC('month', date_start ), 'YYYY-MM') = '2018-04' then spend+(0/30)
     when TO_CHAR(DATE_TRUNC('month', date_start ), 'YYYY-MM') = '2018-03' then spend+(22018/31)
     when TO_CHAR(DATE_TRUNC('month', date_start ), 'YYYY-MM') = '2018-02' then spend+(21565/28)
     when TO_CHAR(DATE_TRUNC('month', date_start ), 'YYYY-MM') = '2018-01' then spend+(21570/31)
     when date(date_start) between timestamp '2018-08-11' and timestamp '2018-09-08' then spend+((288.37+87.27)/28)
     else spend end as spend,
     channel
                from google_perf
      union all
        select  date_start,
                spend,
                channel
        from fb_perf
      union all
        select date_start,
               spend,
               channel
        from apple_perf)

select date_start as timestamp,
       free_trial_created,
       sum(spend) as spend,
       sum(spend)/free_trial_created
from t1 inner join customers_analytics on date(date_start)=timestamp
group by 1,2
order by 1 desc
 ;;
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
