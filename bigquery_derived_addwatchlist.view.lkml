view: bigquery_derived_addwatchlist {

  derived_table: {
    sql:
        (select min(a.timestamp) as timestamp,
                b.received_at,
                b.created_at,
                b.topic,
                a.user_id,
                b.platform,
                a.event,
                'Android' as source
         from android.addwatchlist as a left join http_api.purchase_event as b
         on a.user_id = b.user_id
        GROUP BY 2,3,4,5,6,7
         union all
         select min(a.timestamp) as timestamp,
                b.received_at,
                 b.created_at,
                b.topic,
                a.user_id,
                b.platform,
                a.event,
                'iOS' as source
         from ios.addwatchlist as a left join http_api.purchase_event as b
         on a.user_id = b.user_id
        GROUP BY 2,3,4,5,6,7
         union all
         select  min(a.timestamp) as timestamp,
                b.received_at,
                 b.created_at,
                b.topic,
                a.user_id,
                b.platform,
                a.event,
                'Web' as source
         from javascript.addwatchlist as a left join http_api.purchase_event as b
         on a.user_id = b.user_id
        GROUP BY 2,3,4,5,6,7
        );;
  }

  dimension: status {
    type: string
    sql: ${TABLE}.status ;;
  }

  dimension: platform {
    type: string
    sql: ${TABLE}.platform ;;
  }

  dimension: source {
    type: string
    sql: ${TABLE}.source ;;
  }

  dimension: event {
    type: string
    sql: ${TABLE}.event ;;
  }

  dimension: topic {
    type: string
    sql: ${TABLE}.topic ;;
  }

  dimension: user_id {
    primary_key: yes
    tags: ["user_id"]
    type: string
    sql: ${TABLE}.user_id ;;
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
    sql: ${TABLE}.timestamp;;
  }

  dimension_group: received_at {
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
    sql: ${TABLE}.received_at ;;
  }

  dimension_group: created_at {
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
    sql: ${TABLE}.created_at ;;
  }

  dimension: days_since_addedwatchlist {
    type: number
    sql:  DATE_DIFF(${timestamp_date},${received_at_date},DAY);;

  }

  measure: average_days_since_addedwatchlist {
    type: average
    sql: ${days_since_addedwatchlist};;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  measure: distinct_count {
    type: count_distinct
    sql: ${user_id} ;;
  }


# ----- Sets of fields for drilling ------
  set: detail {
    fields: [
      platform,
      user_id
    ]
  }
}
