view: most_recent_purchase_events {
  derived_table: {
    sql: with most_recent_purchase_events as (SELECT max(topic) topic
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
      , timestamp as ts
      FROM `up-faith-and-family-216419.http_api.purchase_event`
      WHERE ((( timestamp ) >= ((TIMESTAMP_ADD(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), HOUR), INTERVAL -2 HOUR))) AND ( timestamp ) < ((TIMESTAMP_ADD(TIMESTAMP_ADD(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), HOUR), INTERVAL -2 HOUR), INTERVAL 3 HOUR)))))
      GROUP BY 2,3,4,5,6,7,8,9,10,11,12
) /* most_recent_purchase_events gets the most recent topic for each distinct email address, along with all other fields related to HubSpot contact */
/* filter table flags any users with multiple topics and orders their records by descending time stamp */
, filter as (SELECT user_id
      , email
      , first_name
      , last_name
      , topic
      , platform
      , plan
      , referrer
      , subscription_frequency
      , subscription_status
      , subscriber_marketing_opt_in
      , ts
    , ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY ts) as col
FROM most_recent_purchase_events
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
ORDER BY 1
)
/* only returns rows that have the most recent topic, as flagged by the filter table. Add brand column with constant as "upff" */
SELECT * FROM filter WHERE col = 1
       ;;
      sql_trigger_value: SELECT EXTRACT(HOUR FROM CURRENT_TIMESTAMP()) ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: user_id {
    type: string
    tags: ["user_id"]
    primary_key: yes
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

  dimension: topic {
    type: string
    sql: ${TABLE}.topic ;;
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
      subscription_frequency,
      subscription_status,
      subscriber_marketing_opt_in
    ]
  }
}
