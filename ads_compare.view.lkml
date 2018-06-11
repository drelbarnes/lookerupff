view: ads_compare {
  derived_table: {
    sql: with fb_perf as (select
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
case when TO_CHAR(DATE_TRUNC('month', date_start), 'YYYY-MM') = '2018-06' then spend+(19000/30)
     when TO_CHAR(DATE_TRUNC('month', date_start ), 'YYYY-MM') = '2018-05' then spend+(10000/31)
     when TO_CHAR(DATE_TRUNC('month', date_start ), 'YYYY-MM') = '2018-04' then spend+(0/30)
     when TO_CHAR(DATE_TRUNC('month', date_start ), 'YYYY-MM') = '2018-03' then spend+(22018/31)
     when TO_CHAR(DATE_TRUNC('month', date_start ), 'YYYY-MM') = '2018-02' then spend+(21565/28)
     when TO_CHAR(DATE_TRUNC('month', date_start ), 'YYYY-MM') = '2018-01' then spend+(21570/31) end as spend
                from google_perf
      union all
        select  date_start,
                spend
        from fb_perf),

       t2 as (select date_start as timestamp, sum(spend) as spend from t1 group by date_start),

       t3 as (select a1.timestamp, a1.spend+sum(coalesce(a2.spend,0)) as spend_30_days
from t2 as a1
left join t2 as a2 on datediff(day,a2.timestamp,a1.timestamp)<=30 and datediff(day,a2.timestamp,a1.timestamp)>0
group by a1.timestamp,a1.spend),

t4 as (select *,ROW_NUMBER() OVER(ORDER BY t3.timestamp desc) AS Row
from t3
where (t3.timestamp  < (DATEADD(day,-14, DATE_TRUNC('day',GETDATE()) )))),

t5 as (select a1.timestamp,ROW_NUMBER() OVER(ORDER BY a1.timestamp desc) AS Row, a1.free_trial_converted+sum(coalesce(a2.free_trial_converted,0)) as conversions_30_days
from customers.analytics as a1
left join customers.analytics as a2 on datediff(day,a2.timestamp,a1.timestamp)<=30 and datediff(day,a2.timestamp,a1.timestamp)>0
group by a1.timestamp,a1.free_trial_converted)

select t5.timestamp,
spend_30_days, conversions_30_days,cast(spend_30_days as decimal)/cast(conversions_30_days as decimal) as CPA
from t4 inner join t5 on t4.row=t5.row;;
  }

dimension: paid_gains {
  type: number
  sql: ${TABLE}.conversions_30_days;;
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
    sql: ${TABLE}.spend_30_days ;;
  }


  measure: cost_per_acquisition {
    type: sum
    sql: ${TABLE}.CPA ;;
    value_format_name: usd
  }

}
