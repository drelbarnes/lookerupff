view: ltv_cpa{
    derived_table: {
      sql:
      with fb_perf as (select
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
                spend
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
group by a1.timestamp,a1.free_trial_converted),

t6 as (select t5.timestamp, spend_30_days, conversions_30_days,cast(spend_30_days as decimal)/cast(conversions_30_days as decimal) as CPA
from t4 inner join t5 on t4.row=t5.row),

t7 as (select a.*,prior_31_days_subs, 5.99/(cast(churn_30_days as decimal)/cast(prior_31_days_subs as decimal)) as LTV
from
(select a1.timestamp, a1.paying_churn+sum(coalesce(a2.paying_churn,0)) as churn_30_days
from customers.analytics as a1
left join customers.analytics as a2 on datediff(day,a2.timestamp,a1.timestamp)<=30 and datediff(day,a2.timestamp,a1.timestamp)>0
group by a1.timestamp,a1.paying_churn) as a
inner join
(select a.timestamp,total_paying as prior_31_days_subs
from
(select a.timestamp, ROW_NUMBER() OVER(ORDER BY a.timestamp desc) AS Row from customers.analytics as a) as a
inner join
(select a.timestamp,total_paying, ROW_NUMBER() OVER(ORDER BY a.timestamp desc) AS Row from customers.analytics as a where (a.timestamp  < (DATEADD(day,-32, DATE_TRUNC('day',GETDATE()) )))) as b
on a.row=b.row) as b
on a.timestamp=b.timestamp)

select t6.timestamp, CPA, LTV, cast(LTV as decimal)/cast(CPA as decimal) as LTV_CPA_Ratio
from t6 inner join t7 on t6.timestamp=t7.timestamp
;;}

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
    sql: ${TABLE}.timestamp ;;}

          dimension: CPA{
            type: number
            sql: ${TABLE}.CPA ;;

          }

          measure: CPA_1 {
            type: sum
            sql: ${CPA} ;;
            value_format_name: usd
            }

          dimension: LTV {
            type: number
            sql: ${TABLE}.LTV ;;
          }

          measure: LTV_1 {
            type: sum
            sql: ${LTV} ;;
            value_format_name: usd
          }


          dimension: LTV_CPA_Ratio {
            type: number
            sql: ${LTV}/${CPA} ;;
            value_format_name: percent_0
          }

          dimension: Goal{
            type: number
            sql: ${TABLE}.Goal ;;
          }

          dimension: LTV_Goal {
            type: number
            sql: (${CPA}*1.1)-${CPA};;
            value_format_name: usd
          }

          dimension: CPA_Goal{
            type: number
            sql: (${LTV}/1.1)-${LTV} ;;
            value_format_name: usd
          }
          }
