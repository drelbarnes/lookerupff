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
      FROM `up-faith-and-family-216419.http_api.purchase_event`
      WHERE ((( timestamp ) >= ((TIMESTAMP_ADD(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), HOUR), INTERVAL -1 HOUR))) AND ( timestamp ) < ((TIMESTAMP_ADD(TIMESTAMP_ADD(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), HOUR), INTERVAL -1 HOUR), INTERVAL 2 HOUR)))))
      GROUP BY 2,3,4,5,6,7,8,9,10
       ;;
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
    tags: ["emails"]
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
