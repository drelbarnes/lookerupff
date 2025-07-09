view: other_marketing_spend {
  derived_table: {
    sql:
      with apple_search as (
        select safe_cast(date as timestamp) as date,
        sum(cost) as spend,
        'Apple Search Ads' as channel
        from (select date, cost from customers.apple_search group by 1,2)
        group by 1,3
      )
      , bing_ads as (
        select safe_cast(date as timestamp) as date,
        sum(cost) as spend,
        'Bing Ads' as channel
        from (select date, cost from customers.bing_ads group by 1,2)
        group by 1,3
      )
      , google_dcm as (
        select safe_cast(date as timestamp) as date,
        sum(dbm_cost_usd + media_cost) as spend,
        'Google Campaign Manager' as channel
        from (select date, dbm_cost_usd, media_cost from customers.google_dcm group by 1,2,3)
        group by 1,3
      )
      , mntn as (
        select safe_cast(day as timestamp) as date,
        sum(spend) as spend,
        'MNTN' as channel
        from (select day, spend from customers.mntn group by 1,2)
        group by 1,3
      )
      , tiktok as (
        select safe_cast(date as timestamp) as date,
        sum(cost) as spend,
        'TikTok' as channel
        from (select date, cost from customers.tiktok group by 1,2)
        group by 1,3
      )
      , viant as (
        select safe_cast(date as timestamp) as date,
        sum(dbm_cost_usd + media_cost) as spend,
        'Viant' as channel
        from (select date, dbm_cost_usd, media_cost from customers.viant_spend group by 1,2,3)
        group by 1,3
      )
      , tapjoy as (
        select safe_cast(date as timestamp) as date,
        sum(cost) as spend,
        'Tapjoy' as channel
        from (select date, cost from customers.tapjoy group by 1,2)
        group by 1,3
      )
      , samsung as (
        select safe_cast(date as timestamp) as date,
        sum(spend) as spend,
        'Samsung' as channel
        from (select date, spend from customers.samsung group by 1,2)
        group by 1,3
      )
      /*, pinterest as (
        select safe_cast(date as timestamp) as date,
        sum(cost) as spend,
        'Pinterest' as channel
        from (select date, cost from customers.pinterest group by 1,2)
        group by 1,3
      )*/
      , iheart as (
        select safe_cast(date as timestamp) as date,
        sum(cost) as spend,
        'iHeart' as channel
        from (select date, cost from customers.iheart group by 1,2)
        group by 1,3
      )
      , radio as (
        select safe_cast(date as timestamp) as date,
        sum(cost) as spend,
        'Radio' as channel
        from (select date, cost from customers.radio group by 1,2)
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
        union all
        select date
        , spend
        , channel
        from viant
        union all
        select date
        , spend
        , channel
        from tapjoy
        union all
        select date
        , spend
        , channel
        from samsung
        union all
        /*select date
        , spend
        , channel
        from pinterest
        union all*/
        select date
        , spend
        , channel
        from iheart
        union all
        select date
        , spend
        , channel
        from radio
      )
      , outer_query as (
      SELECT * FROM `up-faith-and-family-216419.http_api.other_marketing_spend`
      union all
      select cast(date as date), spend, channel from all_spend
      )
      select * from outer_query
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
