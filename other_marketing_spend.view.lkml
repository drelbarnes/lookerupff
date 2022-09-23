view: other_marketing_spend {
  derived_table: {
    sql:
      with apple_search as (
        select date as date,
        sum(cost) as spend,
        'Apple Search Ads' as channel
        from customers.apple_search
        group by 1,3
      )
      , bing_ads as (
        select date as date,
        sum(cost) as spend,
        'Bing Ads' as channel
        from customers.bing_ads
        group by 1,3
      )
      , google_dcm as (
        select date as date,
        sum(media_cost) as spend,
        'Google Campaign Manager' as channel
        from customers.google_dcm
        group by 1,3
      )
      , mntn as (
        select day as date,
        sum(spend) as spend,
        'MNTN' as channel
        from customers.mntn
        group by 1,3
      )
      , tiktok as (
        select date as date,
        sum(cost) as spend,
        'TikTok' as channel
        from customers.tiktok
        group by 1,3
      )
      , all_spend as (
        select date,
        spend,
        channel
        from apple_search
        union all
        select date
        , spend
        , channel
        from bing_ads
        union all
        select date
        , spend
        , channel
        from google_dcm
        union all
        select date
        , spend
        , channel
        from mntn
        union all
        select date
        , spend
        , channel
        from tiktok
      )
      SELECT * FROM `up-faith-and-family-216419.http_api.other_marketing_spend`
      union all
      select cast(date as date), spend, channel from all_spend

      ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: date {
    type: date
    datatype: date
    sql: ${TABLE}.date ;;
  }

  dimension: user_id {
    type: number
    tags: ["user_id"]
    sql: 1 ;;
  }

  dimension: spend {
    type: number
    sql: CAST(${TABLE}.spend as FLOAT64) ;;
  }

  dimension: channel {
    type: string
    sql: ${TABLE}.channel ;;
  }

  set: detail {
    fields: [date, spend, channel]
  }
}
