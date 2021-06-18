view: bigquery_utm_web_visits {
  derived_table: {
    sql: (select anonymous_id,
       referrer,
       timestamp,
       split(split(referrer,"utm_campaign=")[safe_ordinal(2)],"&")[safe_ordinal(1)] as utm_campaign,
       split(split(referrer,"utm_source=")[safe_ordinal(2)],"&")[safe_ordinal(1)] as utm_source,
       split(split(referrer,"utm_medium=")[safe_ordinal(2)],"&")[safe_ordinal(1)] as utm_medium,
       split(split(referrer,"utm_content=")[safe_ordinal(2)],"&")[safe_ordinal(1)] as utm_content
from javascript_upff_home.pages
where date(timestamp)>'2019-09-15')
union all
(select anonymous_id,
       referrer,
       timestamp,
       split(split(referrer,"utm_campaign=")[safe_ordinal(2)],"&")[safe_ordinal(1)] as utm_campaign,
       split(split(referrer,"utm_source=")[safe_ordinal(2)],"&")[safe_ordinal(1)] as utm_source,
       split(split(referrer,"utm_medium=")[safe_ordinal(2)],"&")[safe_ordinal(1)] as utm_medium,
       split(split(referrer,"utm_content=")[safe_ordinal(2)],"&")[safe_ordinal(1)] as utm_content
from javascript.view
where date(timestamp)>'2019-09-15')
union all
(select anonymous_id,
       referrer,
       timestamp,
       split(split(referrer,"utm_campaign=")[safe_ordinal(2)],"&")[safe_ordinal(1)] as utm_campaign,
       split(split(referrer,"utm_source=")[safe_ordinal(2)],"&")[safe_ordinal(1)] as utm_source,
       split(split(referrer,"utm_medium=")[safe_ordinal(2)],"&")[safe_ordinal(1)] as utm_medium,
       split(split(referrer,"utm_content=")[safe_ordinal(2)],"&")[safe_ordinal(1)] as utm_content
from javascript.pages
where date(timestamp)>'2019-09-15')
 ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  measure: web_visits {
    type: count_distinct
    sql: ${TABLE}.anonymous_id ;;
  }

  dimension: utm_content {
    type: string
    sql: ${TABLE}.utm_content ;;
  }

  dimension: anonymous_id {
    type: string
    sql: ${TABLE}.anonymous_id ;;
  }

  dimension: referrer {
    type: string
    sql: ${TABLE}.referrer ;;
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

  dimension: utm_campaign {
    type: string
    sql: ${TABLE}.utm_campaign ;;
  }

  dimension: utm_source {
    type: string
    sql: ${TABLE}.utm_source ;;
  }

  dimension: utm_medium {
    type: string
    sql: ${TABLE}.utm_medium ;;
  }

  set: detail {
    fields: [anonymous_id, referrer, timestamp_time, utm_campaign, utm_medium,utm_source,utm_content]
  }
}
