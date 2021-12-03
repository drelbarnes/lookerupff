view: most_recent_purchase_events {
  derived_table: {
    sql: SELECT max(topic) topic
      , user_id
      , email
      , first_name
      , last_name
      , platform
      , plan
      , referrer
      , subscription_frequency
      , subscription_status
      , moptin as subscriber_marketing_opt_in
      , uuid_ts as status_ts
      FROM `up-faith-and-family-216419.http_api.purchase_event`
      WHERE ((( timestamp ) >= ((TIMESTAMP_ADD(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), HOUR), INTERVAL -2 HOUR))) AND ( timestamp ) < ((TIMESTAMP_ADD(TIMESTAMP_ADD(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), HOUR), INTERVAL -2 HOUR), INTERVAL 3 HOUR)))))
      GROUP BY 2,3,4,5,6,7,8,9,10,11,12
       ;;
      sql_trigger_value: SELECT EXTRACT(HOUR FROM CURRENT_TIMESTAMP()) ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: topic {
    type: string
    sql: ${TABLE}.topic ;;
  }


  dimension: user_id {
    type: string
    tags: ["user_id"]
    sql: ${TABLE}.user_id ;;
  }

  dimension: email {
    type: string
    tags: ["email"]
    sql: ${TABLE}.email ;;
  }

  dimension: first_name {
    type: string
    sql: ${TABLE}.first_name ;;
  }

  dimension: last_name {
    type: string
    sql: ${TABLE}.last_name ;;
  }

  dimension: platform {
    type: string
    sql: ${TABLE}.platform ;;
  }

  dimension: plan {
    type: string
    sql: ${TABLE}.plan ;;
  }

  dimension: referrer {
    type: string
    sql: ${TABLE}.referrer ;;
  }

  dimension: subscription_frequency {
    type: string
    sql: ${TABLE}.subscription_frequency ;;
  }

  dimension: subscription_status {
    type: string
    sql: ${TABLE}.subscription_status ;;
  }

  dimension: subscriber_marketing_opt_in {
    type: yesno
    sql:  ${TABLE}.subscriber_marketing_opt_in ;;
  }

  set: detail {
    fields: [
      topic,
      user_id,
      email,
      first_name,
      last_name,
      platform,
      plan,
      referrer,
      subscription_frequency,
      subscription_status
    ]
  }
}
