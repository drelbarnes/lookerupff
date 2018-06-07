view: http_api_users_v2 {
  sql_table_name: http_api.users ;;

  dimension: id {
    primary_key: yes
    type: string
    sql: ${TABLE}.id ;;
  }

  dimension: charge_status {
    type: string
    sql: ${TABLE}.charge_status ;;
  }

  dimension_group: charge_status {
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
    sql: ${TABLE}.charge_status_date ;;
  }

  dimension: city {
    type: string
    sql: ${TABLE}.city ;;
  }

  dimension: context_library_consumer {
    type: string
    sql: ${TABLE}.context_library_consumer ;;
  }

  dimension: context_library_name {
    type: string
    sql: ${TABLE}.context_library_name ;;
  }

  dimension: context_library_version {
    type: string
    sql: ${TABLE}.context_library_version ;;
  }

  dimension: country {
    type: string
    map_layer_name: countries
    sql: ${TABLE}.country ;;
  }

  dimension_group: created {
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

  dimension: email {
    type: string
    sql: ${TABLE}.email ;;
  }

  dimension: event_status {
    type: string
    sql: ${TABLE}.event_status ;;
  }

  dimension: name {
    type: string
    sql: ${TABLE}.name ;;
  }

  dimension: plan {
    type: string
    sql: ${TABLE}.plan ;;
  }

  dimension: platform {
    type: string
    sql: ${TABLE}.platform ;;
  }

  dimension_group: received {
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

  dimension: region {
    type: string
    sql: ${TABLE}.region ;;
  }

  dimension: status {
    type: string
    sql: ${TABLE}.status ;;
  }

  dimension_group: status {
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
    sql: ${TABLE}.status_date ;;
  }

  dimension: team {
    type: string
    sql: ${TABLE}.team ;;
  }

  dimension: topic {
    type: string
    sql: ${TABLE}.topic ;;
  }

  dimension_group: updated {
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
    sql: ${TABLE}.updated_at ;;
  }

  dimension: upff {
    type: string
    sql: ${TABLE}.upff ;;
  }

  dimension: uuid {
    type: number
    value_format_name: id
    sql: ${TABLE}.uuid ;;
  }

  dimension_group: uuid_ts {
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
    sql: ${TABLE}.uuid_ts ;;
  }

  measure: count {
    type: count
    drill_fields: [id, name, context_library_name]
  }
}
