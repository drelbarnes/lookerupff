view: cpft {
  derived_table: {
    sql:
    with c as (

select id
, name
, received_at
, status
from google_ads_gaithertv.campaigns

),

g as (

select
 groups.id as ad_group_id
, campaign_id
, groups.received_at
, c.name
, c.received_at
, c.status
from google_ads_gaithertv.ad_groups as groups
INNER JOIN c
ON groups.campaign_id = c.id


),

ad as (

select
  g.campaign_id
, g.name
, ads.ad_group_id
, g.status
, ads.received_at
, ads.cost
, ads.date_start
from google_ads_gaithertv.ad_performance_reports as ads
INNER JOIN g ON ads.ad_group_id = g.ad_group_id


),
     customers_analytics as (
        SELECT
      COUNT(*) AS free_trial_created
      ,date(report_date)as timestamp
    FROM ${gaither_analytics_v2.SQL_TABLE_NAME}
    WHERE
      status = 'in_trial'
    GROUP BY 2
      ),


      fb_perf as (
      SELECT
      i.date_start,
      CAST('Facebook' AS VARCHAR) as channel,
      sum(i.spend) as spend,
      sum(i.impressions) as impressions,
      sum(i.clicks) as clicks

      from (
      select date_start,
      spend,
      impressions,
      clicks
      from facebook_ads_gtv.insights
      ) as i
      group by 1,2
      ),


      google_perf as (
      select apr.date_start,
      b.name as channel,
      b.spend as spend,
      sum(impressions) as impressions,
      sum(clicks) as clicks
      from (
      select  apr.date_start,
      sum((apr.cost/1000000)) as campaigncost,
      sum(impressions) as impressions,
      sum(clicks) as clicks
      from google_ads_gaithertv.campaign_performance_reports as apr
      group by 1
      ) as apr
      inner join (
      select date_start
      , sum(COALESCE((cost/1000000),0 )) as spend
      ,name
      from ad
      group by date_start,name
      ) as b
      on apr.date_start=b.date_start
      group by apr.date_start, b.spend,b.name
      ),

      t1 as (
      -- manually adding spend to historical google adwords record
      select date(date_start) as date_start,
      spend,
      impressions,
      clicks,
      channel
      from google_perf
      union all
      select date(date_start)as date_start,
      spend,
      impressions,
      clicks,
      channel
      from fb_perf

      )





      -- NOTE: we discontinued use of the spend forecasting logic.
      , outer_query as (
      select
      date_start,
      ca.free_trial_created,
      channel,
      spend
      from t1
      inner join customers_analytics as ca
      on date(date_start) = date(timestamp)
      group by 1,2,3,4

      )
            select * from outer_query
      ;;
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


  measure: spend {
    type: sum
    sql_distinct_key: ${timestamp_date} ;;
    sql: ${TABLE}.spend ;;
    value_format_name: usd
  }


  measure: free_trial_created {
    type: sum_distinct
    sql_distinct_key: ${timestamp_date} ;;
    sql: ${TABLE}.free_trial_created ;;
  }

}
