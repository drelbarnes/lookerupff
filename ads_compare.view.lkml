view: ads_compare {
  derived_table: {
    sql: with
      fb_perf as (
        select
                i.date_start,
                ROW_NUMBER() OVER(ORDER BY date_start desc) AS Row,
                sum(i.spend) as spend,
                'Facebook Ads'::text as source
          from  facebook_ads.insights i
          where date(date_start)<dateadd(day,-14,date(getdate()))
      group by  1
      ),
      subscribers as (
        select (a.timestamp),
       ROW_NUMBER() OVER(ORDER BY a.timestamp desc) AS Row,
       free_trial_converted as paid_gains
       from customers.analytics as a
      ),
      google_perf as (        select a.date_start,ROW_NUMBER() OVER(ORDER BY a.date_start desc) AS Row,
        sum(campaigncost+spend) as spend, 'Google Ad Words'::text as source
        from (select  apr.date_start,
                sum((apr.cost/1000000)) as campaigncost
          from  adwords.campaign_performance_reports as apr
          where date(date_start)<dateadd(day,-14,date(getdate()))
          group by  1) as a inner join
  (select date_start,ROW_NUMBER() OVER(ORDER BY date_start desc) AS Row,
  sum(COALESCE((cost/1000000),0 )) as spend from adwords.ad_performance_reports
  where date(date_start)<dateadd(day,-14,date(getdate()))
  group by date_Start) as b
  on a.date_start=b.date_start
  group by 1)

      select b.timestamp,source,paid_gains/2 as gains,sum(spend) as spend
        from (select date_start, row,
                spend,
                source
                from google_perf
      union all
        select  date_start, row,
                spend,
                source
        from fb_perf) as a inner join subscribers as b on a.row=b.row
        group by 1,2,3;;
  }

dimension: paid_gains {
  type: number
  sql: ${TABLE}.gains ;;
}

  measure: paid_gains_total {
    type: sum
    sql: ${paid_gains} ;;
  }

  dimension_group: timestamp {
    type: time
    timeframes: [
      raw,
      time,
      date,
      week,
      month,
      quarter,
      year
    ]
    sql: ${TABLE}.timestamp ;;
  }

  measure: spend_14_days_prior {
    type: sum
    value_format_name: usd
    sql: ${TABLE}.spend ;;
  }


  dimension: source {
    type: string
    sql: ${TABLE}.source ;;
  }


  measure: cost_per_acquisition {
    type: number
    sql: ${spend_14_days_prior}/${paid_gains_total} ;;
    value_format_name: usd
  }

}
