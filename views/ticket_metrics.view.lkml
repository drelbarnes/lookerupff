view: ticket_metrics {
  sql_table_name: `up-faith-and-family-216419.zendesk.ticket_metrics`
    ;;
  drill_fields: [id]

  dimension: id {
    primary_key: yes
    type: string
    sql: ${TABLE}.id ;;
  }

  dimension_group: _partitiondate {
    type: time
    timeframes: [
      raw,
      date,
      week,
      month,
      quarter,
      year
    ]
    convert_tz: no
    datatype: date
    sql: ${TABLE}._PARTITIONDATE ;;
  }

  dimension_group: _partitiontime {
    type: time
    timeframes: [
      raw,
      date,
      week,
      month,
      quarter,
      year
    ]
    convert_tz: no
    datatype: date
    sql: ${TABLE}._PARTITIONTIME ;;
  }

  dimension: agent_wait_time_in_minutes_business {
    type: number
    sql: ${TABLE}.agent_wait_time_in_minutes_business ;;
  }

  dimension: agent_wait_time_in_minutes_calendar {
    type: number
    sql: ${TABLE}.agent_wait_time_in_minutes_calendar ;;
  }

  dimension_group: assigned {
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
    sql: ${TABLE}.assigned_at ;;
  }

  dimension: assignee_stations {
    type: number
    sql: ${TABLE}.assignee_stations ;;
  }

  dimension_group: assignee_updated {
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
    sql: ${TABLE}.assignee_updated_at ;;
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

  dimension: first_resolution_time_in_minutes_business {
    type: number
    sql: ${TABLE}.first_resolution_time_in_minutes_business ;;
  }

  dimension: first_resolution_time_in_minutes_calendar {
    type: number
    sql: ${TABLE}.first_resolution_time_in_minutes_calendar ;;
  }

  dimension: full_resolution_time_in_minutes_business {
    type: number
    sql: ${TABLE}.full_resolution_time_in_minutes_business ;;
  }

  dimension: full_resolution_time_in_minutes_calendar {
    type: number
    sql: ${TABLE}.full_resolution_time_in_minutes_calendar ;;
  }

  dimension: group_stations {
    type: number
    sql: ${TABLE}.group_stations ;;
  }

  dimension_group: initially_assigned {
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
    sql: ${TABLE}.initially_assigned_at ;;
  }

  dimension_group: latest_comment_added {
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
    sql: ${TABLE}.latest_comment_added_at ;;
  }

  dimension_group: loaded {
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
    sql: ${TABLE}.loaded_at ;;
  }

  dimension: on_hold_time_in_minutes_business {
    type: number
    sql: ${TABLE}.on_hold_time_in_minutes_business ;;
  }

  dimension: on_hold_time_in_minutes_calendar {
    type: number
    sql: ${TABLE}.on_hold_time_in_minutes_calendar ;;
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

  dimension: reopens {
    type: number
    sql: ${TABLE}.reopens ;;
  }

  dimension: replies {
    type: number
    sql: ${TABLE}.replies ;;
  }

  dimension: reply_time_in_minutes_business {
    type: number
    sql: ${TABLE}.reply_time_in_minutes_business ;;
  }

  dimension: reply_time_in_minutes_calendar {
    type: number
    sql: ${TABLE}.reply_time_in_minutes_calendar ;;
  }

  dimension_group: requester_updated {
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
    sql: ${TABLE}.requester_updated_at ;;
  }

  dimension: requester_wait_time_in_minutes_business {
    type: number
    sql: ${TABLE}.requester_wait_time_in_minutes_business ;;
  }

  dimension: requester_wait_time_in_minutes_calendar {
    type: number
    sql: ${TABLE}.requester_wait_time_in_minutes_calendar ;;
  }

  dimension_group: solved {
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
    sql: ${TABLE}.solved_at ;;
  }

  dimension_group: status_updated {
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
    sql: ${TABLE}.status_updated_at ;;
  }

  dimension: ticket_id {
    type: string
    sql: ${TABLE}.ticket_id ;;
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
    drill_fields: [id]
  }
}
