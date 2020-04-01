view: bigquery_all_conversions {
  derived_table: {
    sql: select anonymous_id,
       timestamp,
       'android' as platform
from android.order_completed
union all
select anonymous_id,
       timestamp,
       'ios' as platform
from ios.order_completed
union all
select anonymous_id,
       timestamp,
       'web' as platform
from javascript.order_completed
union all
select anonymous_id,
       timestamp,
       'android' as platform
from android.conversion
union all
select anonymous_id,
       timestamp,
       'ios' as platform
from ios.conversion
union all
select anonymous_id,
       timestamp,
       'web' as platform
from javascript.conversion
 ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: anonymous_id {
    type: string
    sql: ${TABLE}.anonymous_id ;;
  }

  dimension_group: timestamp {
    type: time
    sql: ${TABLE}.timestamp ;;
  }

  dimension: platform {
    type: string
    sql: ${TABLE}.platform ;;
  }

  measure: conversion_count {
    type: count_distinct
    sql: ${anonymous_id} ;;
  }

  set: detail {
    fields: [anonymous_id, timestamp_time, platform]
  }
}
