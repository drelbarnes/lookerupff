view: bigquery_tickets {
  sql_table_name: zendesk.tickets ;;
  drill_fields: [id]

  dimension: id {
    primary_key: yes
    type: string
    sql: ${TABLE}.id ;;
  }

  dimension: account {
    type: string
    sql: ${TABLE}.account ;;
  }

  dimension: account_settings {
    type: string
    sql: ${TABLE}.account_settings ;;
  }

  dimension: assignee_id {
    type: number
    sql: ${TABLE}.assignee_id ;;
  }

  dimension: billing {
    type: string
    sql: ${TABLE}.billing ;;
  }

  dimension: brand_id {
    type: number
    sql: ${TABLE}.brand_id ;;
  }

  dimension: category {
    type: string
    sql: ${TABLE}.category ;;
  }

  dimension: collaborator_ids {
    type: string
    sql: ${TABLE}.collaborator_ids ;;
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

  dimension: description {
    type: string
    sql: ${TABLE}.description ;;
  }

  dimension: device_type {
    type: string
    sql: ${TABLE}.device_type ;;
  }

  dimension: devices {
    type: string
    sql: ${TABLE}.devices ;;
  }

  dimension: feedback {
    type: string
    sql: ${TABLE}.feedback ;;
  }

  dimension: general {
    type: string
    sql: ${TABLE}.general ;;
  }

  dimension: group_id {
    type: number
    sql: ${TABLE}.group_id ;;
  }

  dimension: issue {
    type: string
    sql: ${TABLE}.issue ;;
  }

  dimension: looker {
    type: yesno
    sql: ${TABLE}.looker ;;
  }

  dimension: needs_attention {
    type: yesno
    sql: ${TABLE}.needs_attention ;;
  }

  dimension: nps {
    type: yesno
    sql: ${TABLE}.nps ;;
  }

  dimension: organization_id {
    type: number
    sql: ${TABLE}.organization_id ;;
  }

  dimension: other {
    type: string
    sql: ${TABLE}.other ;;
  }

  dimension: priority {
    type: string
    sql: ${TABLE}.priority ;;
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

  dimension: recipient {
    type: string
    sql: ${TABLE}.recipient ;;
  }

  dimension: requester_id {
    type: number
    sql: ${TABLE}.requester_id ;;
  }

  dimension: satisfaction_rating_comment {
    type: string
    sql: ${TABLE}.satisfaction_rating_comment ;;
  }

  dimension: satisfaction_rating_id {
    type: number
    sql: ${TABLE}.satisfaction_rating_id ;;
  }

  dimension: satisfaction_rating_reason {
    type: string
    sql: ${TABLE}.satisfaction_rating_reason ;;
  }

  dimension: satisfaction_rating_reason_id {
    type: number
    sql: ${TABLE}.satisfaction_rating_reason_id ;;
  }

  dimension: satisfaction_rating_score {
    type: string
    sql: ${TABLE}.satisfaction_rating_score ;;
  }

  dimension: status {
    type: string
    sql: ${TABLE}.status ;;
  }

  dimension: streaming {
    type: string
    sql: ${TABLE}.streaming ;;
  }

  dimension: streaming_2 {
    type: string
    sql: ${TABLE}.streaming_2 ;;
  }

  dimension: subject {
    type: string
    sql: ${TABLE}.subject ;;
  }

  dimension: submitter_id {
    type: number
    sql: ${TABLE}.submitter_id ;;
  }

  dimension: subscription {
    type: string
    sql: ${TABLE}.subscription ;;
  }

  dimension: tags {
    type: string
    sql: ${TABLE}.tags ;;
  }

  dimension: type {
    type: string
    sql: ${TABLE}.type ;;
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

  dimension: url {
    type: string
    sql: ${TABLE}.url ;;
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

  dimension: watch {
    type: string
    sql: ${TABLE}.watch ;;
  }

  measure: count {
    type: count
    drill_fields: [id]
  }
}
