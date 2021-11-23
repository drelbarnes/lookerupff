view: bigquery_http_api_purchase_event_hubspot {
derived_table: {
sql: with a1 as
  (select email,
         max(status_date) as status_date
  from http_api.purchase_event
  group by 1),

  a2 as
  (select distinct a1.email,
         a1.status_date,
         topic as latest_status
  from a1 inner join http_api.purchase_event as a2 on a1.email=a2.email and a1.status_date=a2.status_date
  where topic not in ('customer.product.charge_failed','customer.created')
  order by 1)

  select a3.*,
         latest_status
  from http_api.purchase_event as a3 left join a2 on a2.email=a3.email;;}


  dimension: id {
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
    tags:["email"]
    sql: ${TABLE}.email ;;
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

  dimension: fname {
    type: string
    sql: ${TABLE}.fname ;;
  }

  dimension: lname {
    type: string
    sql: ${TABLE}.lname ;;
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

  dimension: moptin {
    type: yesno
    sql: ${TABLE}.moptin ;;
  }

  dimension: name {
    type: string
    sql: ${TABLE}.name ;;
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

  dimension: latest_status {
    type: string
    sql: ${TABLE}.latest_status;;
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

  dimension: topic {
    type: string
    sql: ${TABLE}.topic ;;
  }

  dimension: subscription_status {
    type: string
    sql: ${TABLE}.subscription_status ;;
  }

  dimension: recent_topic {
    type:  string
    sql:  MAX(${topic}) ;;
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

  dimension_group: max_status {
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
    sql: Max(${status_date}) ;;
  }

  dimension: upff {
    type: string
    sql: ${TABLE}.upff ;;
  }

  dimension: user_id {
    primary_key: yes
    type: string
    tags: ["user_id"]
    sql: ${TABLE}.user_id ;;
  }

  dimension: current_date{
    type: date
    sql: current_date;;
  }

  dimension: max_status_days {
    type: number
    sql: DATE_DIFF(${current_date}, ${status_date}, DAY);;
  }

  dimension: cancelled_days_14 {
    type: number
    sql: DATE_DIFF(${current_date}, ${status_date}, DAY) = 14;;
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
    drill_fields: [id, lname, fname, context_library_name, name]
  }


  measure: count_by_email {
    type: count_distinct
    sql: ${email} ;;
  }

  measure: last_status_date {
    type: date
    sql: MAX(${status_date}) ;;
  }

  measure: recent_status {
    type: string
    sql: MAX(${topic} = 'customer.product.cancelled' OR ${topic} = 'customer.product.expired' or ${topic}='customer.product.disabled') ;;

  }

  measure: churn_status {
    type: count_distinct
    sql: case when ${topic} = 'customer.product.cancelled' OR ${topic} = 'customer.product.expired' or ${topic}='customer.product.disabled' then ${email} else null end ;;

  }

  measure: distinct_count {
    type: count_distinct
    sql: ${user_id} ;;
  }



}
