view: ads_compare {
  derived_table: {
    sql: with
      fb_perf as (
        select
                i.date_start,
                sum(i.spend) as spend,
                sum(i.impressions) as impresssions,
                sum(i.clicks) as clicks,
                'Facebook Ads'::text as source
          from  facebook_ads.insights i
      group by  1
      ),
      subscribers as (
        select timestamp as timestamp,
               free_trial_converted + paying_created as paid_gains
        from customers.analytics
      ),
      google_perf as (
        select  apr.date_start,
                sum(apr.cost/1000000) as spend,
                sum(apr.impressions) as impresssions,
                sum(apr.clicks) as clicks,
                'Google Ad Words'::text as source
          from  adwords.campaign_performance_reports as apr
          group by  1
      )

      select date_start,source,paid_gains as gains,sum(spend) as spend, sum(impresssions) as impressions,
      sum(clicks) as clicks
        from (select date_start,
                spend,
                impresssions,
                clicks,
                source
                from google_perf
      union all
        select  date_start,
                spend,
                impresssions,
                clicks,
                source
        from fb_perf) as a inner join subscribers as b on date(date_start)=date(timestamp)
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

  dimension_group: date_start {
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
    sql: ${TABLE}.date_start ;;
  }

  measure: spend {
    type: sum
    value_format_name: usd
    sql: ${TABLE}.spend ;;
  }

  measure: impresssions {
    type: sum
    sql: ${TABLE}.impresssions ;;
  }

  measure: clicks {
    type: sum
    sql: ${TABLE}.clicks ;;
  }

  dimension: source {
    type: string
    sql: ${TABLE}.source ;;
  }

  measure: cost_per_click {
    type: number
    sql: ${spend}::float/NULLIF(${clicks},0) ;;
    value_format_name: usd
  }

  measure: cost_per_acquisition {
    type: number
    sql: ${spend}/${paid_gains_total} ;;
    value_format_name: usd
  }

}
