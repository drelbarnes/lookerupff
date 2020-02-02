view: bigquery_derived_addwatchlist {

  derived_table: {
    sql:
        (select a.timestamp,
                b.recieved_at,
                b.topic,
                a.user_id,
                b.platform,
                a.event,
                'Android' as source
         from android.addwatchlist as a left join http_api.purchase_event as b
         on SAFE_CAST(a.user_id AS INT64) = b.user_id
         union all
         select a.timestamp,
                b.recieved_at,
                b.topic,
                a.user_id,
                b.platform,
                a.event,
                'iOS' as source
         from ios.addwatchlist as a left join http_api.purchase_event as b
         on SAFE_CAST(a.user_id AS INT64) = b.user_id
         union all
         select a.timestamp,
                b.recieved_at,
                b.topic,
                a.user_id,
                b.platform,
                a.event,
                'Web' as source
         from javascript.addwatchlist as a left join http_api.purchase_event as b
         on SAFE_CAST(a.user_id AS INT64) = b.user_id
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
    sql: ${TABLE}.timestamp ;;
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

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  measure: distinct_count {
    type: count_distinct
  }


# ----- Sets of fields for drilling ------
  set: detail {
    fields: [
      platform,
      user_id
    ]
  }
}
