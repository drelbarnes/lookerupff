view: redshift_mobile_conversions {
  derived_table: {
    sql: select anonymous_id,
       order_completed.timestamp,
       'android' as source
from android.order_completed
union all
select anonymous_id,
       conversion.timestamp,
       'android' as source
from android.conversion
union all
select anonymous_id,
       order_completed.timestamp,
       'iOS' as source
from ios.order_completed
union all
select anonymous_id,
       conversion.timestamp,
       'iOS' as source
from ios.conversion
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

  dimension: source {
    type: string
    sql: ${TABLE}.source ;;
  }

  measure: conversion_count {
    type: count_distinct
    sql: ${anonymous_id} ;;
  }

  set: detail {
    fields: [anonymous_id, timestamp_time, source]
  }
}
