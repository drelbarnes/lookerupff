view: http_api_purchase_event {
  sql_table_name: http_api.purchase_event ;;

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

  dimension: current_date{
    type: date
    sql: current_date;;
  }

  dimension: days_in_trial{
    description: "Number of days a user is in free trial"
    value_format: "0"
    type: number
    sql:  DATEDIFF('day', ${created_date}::timestamp, ${current_date}::timestamp);;
  }

  dimension: months_since_created {
    type: number
    sql:  DATEDIFF('month', ${created_date}::timestamp, ${sent_date}::timestamp);;
  }

  dimension: email {
    type: string
    tags: ["email"]
    sql: ${TABLE}.email ;;
  }

  dimension: getfname {
    type: string
    hidden: yes
    sql: SPLIT_PART(${email}, '@' , 1);;
  }


  dimension: event {
    type: string
    sql: ${TABLE}.event ;;
  }

  dimension: event_status {
    type: string
    sql: ${TABLE}.event_status ;;
  }

  dimension: event_text {
    type: string
    sql: ${TABLE}.event_text ;;
  }

  dimension: name {
    type: string
    sql: ${TABLE}.name ;;
  }

  dimension: fname {
    type: string
    sql: ${TABLE}.fname ;;
  }

  dimension: findfname {
    case: {
      when: {
        sql: ${getfname} = ${fname};;
        label: "Has Username in First Name"
      }
      else: "Nevers"
    }
  }

  dimension: lname {
    type: string
    sql: ${TABLE}.lname ;;
  }

  dimension_group: original_timestamp {
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
    sql: ${TABLE}.original_timestamp ;;
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

  dimension_group: sent {
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
    sql: ${TABLE}.sent_at ;;
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

  dimension_group: latest_status {
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
    sql: ${TABLE}.status_date;;
  }


  dimension: days_since_created {
    type: number
    sql:  DATEDIFF('day', ${created_date}::timestamp, ${status_date}::timestamp);;
  }


  dimension: days_after_status {
    type:  number
    sql:  DATEDIFF('day', ${latest_status_date}::timestamp, ${current_date}::timestamp);;
  }

  dimension: team {
    type: string
    sql: ${TABLE}.team ;;
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

  dimension: user_id {
    type: string
    # hidden: yes
    tags: ["user_id"]
    sql: ${TABLE}.user_id ;;
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

  dimension: topic {
    type: string
    sql: ${TABLE}.topic ;;
  }

  dimension: moptin {
    type: yesno
    sql: ${TABLE}.moptin ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  measure: distinct_count {
    type: count_distinct
    sql: ${email} ;;
  }

  measure: average_months_since_created {
    type: average
    sql: ${months_since_created};;
  }

  measure: moptin_yes {
    type: count_distinct
    sql: ${TABLE}.user_id ;;
    filters: {
      field: http_api_purchase_event.moptin
      value: "yes"
    }
  }

  measure: moptin_no {
    type: count_distinct
    sql: ${TABLE}.user_id ;;
    filters: {
      field: http_api_purchase_event.moptin
      value: "no"
    }
  }

  measure: moption_conversion_rate {
    type: number
    value_format: ".0#\%"
    sql: 100.0*${moptin_yes}/${distinct_count};;
  }


  measure: last_updated_date {
    type: date
    sql: MAX(${status_date}) ;;
  }


  measure: last_updated_date_v2 {
    type:  number
    sql:  DATEDIFF('day', MAX(${status_date})::timestamp, ${current_date}::timestamp);;
  }

  measure: getFindFname {
    type:  count_distinct
    sql: ${user_id} ;;
    filters: {
      field: findfname
      value: "${fname}"
    }
  }


  # ----- Sets of fields for drilling ------
  set: detail {
    fields: [
      id,
      name,
      context_library_name,
      users.name,
      users.context_library_name,
      users.id
    ]
  }
}
