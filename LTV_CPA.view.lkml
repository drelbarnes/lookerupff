view: ltv_cpa{
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
  group by 1),

      ads_compare as (select b.timestamp,source,paid_gains/2 as gains,sum(spend) as spend
        from (select date_start, row,
                spend,
                source
                from google_perf
      union all
        select  date_start, row,
                spend,
                source
        from fb_perf) as a inner join subscribers as b on a.row=b.row
        group by 1,2,3),

    CPA_ as (SELECT
  (COALESCE(SUM(ads_compare.spend ), 0))/(COALESCE(SUM(ads_compare.gains ), 0))  AS cpa,
  1 as matching
FROM ads_compare
WHERE
  (((ads_compare.timestamp ) >= ((DATEADD(day,-29, DATE_TRUNC('day',GETDATE()) ))) AND (ads_compare.timestamp ) < ((DATEADD(day,30, DATEADD(day,-29, DATE_TRUNC('day',GETDATE()) ) )))))
HAVING
  (COALESCE(SUM(ads_compare.gains ), 0) > 0)),

lifetime_value AS (select cast(churn_30_days as decimal) as churn_30_days, cast(total_paying as decimal) as total_paying_31_days_prior
from
(select sum(paying_churn) as churn_30_days, 1 as matching
from customers.analytics
      where   (((analytics.timestamp ) >= ((DATEADD(day,-29, DATE_TRUNC('day',GETDATE()) ))) AND (analytics.timestamp ) < ((DATEADD(day,30, DATEADD(day,-29, DATE_TRUNC('day',GETDATE()) ) )))))) as a
inner join
(select analytics.timestamp, total_paying, 1 as matching from customers.analytics where timestamp= ((DATEADD(day,-30, DATE_TRUNC('day',GETDATE()) )))) as b
on a.matching=b.matching),

LTV_ as (SELECT
  5.99/(lifetime_value.churn_30_days/lifetime_value.total_paying_31_days_prior) AS LTV,
  1 as matching
FROM lifetime_value)

select CPA, LTV,1.1 as Goal
from CPA_ as a inner join LTV_ as b on a.matching=b.matching;;}

          dimension: CPA{
            type: number
            sql: ${TABLE}.CPA ;;
            value_format_name: usd
          }

          dimension: LTV {
            type: number
            sql: ${TABLE}.LTV ;;
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
