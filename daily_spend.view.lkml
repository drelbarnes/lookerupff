view: daily_spend {
  derived_table: {
    sql: with customers_analytics as (select analytics_timestamp as timestamp,
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

      fb_perf as (select
                i.date_start,
                sum(i.spend) as spend
          from  facebook_ads.insights as i
      group by  1
      ),
      google_perf as (
        select  apr.date_start,
                sum(campaigncost+spend) as spend
          from  (select  apr.date_start,
                sum((apr.cost/1000000)) as campaigncost
          from  adwords.campaign_performance_reports as apr
          group by  1) as apr
          inner join
          (select date_start,
  sum(COALESCE((cost/1000000),0 )) as spend from adwords.ad_performance_reports
  group by date_Start) as b on apr.date_start=b.date_start
          group by  1
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
     else spend end as spend
                from google_perf
      union all
        select  date_start,
                spend
        from fb_perf)

        (select date_start as timestamp, sum(spend) as spend from t1 group by date_start)
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

  dimension: spend {
    type: number
    sql: ${TABLE}.spend ;;
  }

  set: detail {
    fields: [timestamp_time, spend]
  }
}
