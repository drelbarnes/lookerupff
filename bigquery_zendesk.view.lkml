view: bigquery_zendesk {
  derived_table: {
    sql: select distinct email,
       nps,
       a.created_at,
       reply_time_in_minutes_calendar,
       full_resolution_time_in_minutes_calendar,
       satisfaction_rating_score
from zendesk.tickets as a inner join zendesk.users as b on a.requester_id=cast(b.id as int64)
                          inner join zendesk.ticket_metrics as c on c.ticket_id=a.id ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: email {
    type: string
    sql: ${TABLE}.email ;;
  }

  dimension: nps {
    type: string
    sql: ${TABLE}.nps ;;
  }

  measure: email_count {
    type: count_distinct
    sql: ${email} ;;
  }

  dimension_group: created_at {
    type: time
    sql: ${TABLE}.created_at ;;
  }

  dimension: reply_time_in_minutes_calendar {
    type: number
    sql: ${TABLE}.reply_time_in_minutes_calendar ;;
  }

  dimension: reply_time_in_hours {
    type: number
    sql:round(${reply_time_in_minutes_calendar}/60);;
  }

  measure: reply_time_in_minutes_calendar_ {
    type: sum
    sql: ${TABLE}.reply_time_in_minutes_calendar ;;
  }

  measure: reply_time_in_hours_ {
    type: sum
    sql: ${reply_time_in_hours};;
  }

  dimension: full_resolution_time_in_minutes_calendar {
    type: number
    sql: ${TABLE}.full_resolution_time_in_minutes_calendar ;;
  }

  dimension: full_resolution_time_in_hours_calendar {
    type: number
    sql: round(${TABLE}.full_resolution_time_in_minutes_calendar/60) ;;
  }

  measure: full_resolution_time_in_minutes_calendar_ {
    type: sum
    sql: ${TABLE}.full_resolution_time_in_minutes_calendar ;;
  }

  measure: full_resolution_time_in_hours_calendar_ {
    type: sum
    sql: ${full_resolution_time_in_hours_calendar} ;;
  }

  dimension: satisfaction_rating_score {
    type: string
    sql: ${TABLE}.satisfaction_rating_score ;;
  }



  set: detail {
    fields: [
      email,
      nps,
      created_at_time,
      reply_time_in_minutes_calendar,
      full_resolution_time_in_minutes_calendar,
      satisfaction_rating_score
    ]
  }
}
