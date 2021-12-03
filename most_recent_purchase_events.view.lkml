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
, filter as (SELECT
    most_recent_purchase_events.user_id  AS most_recent_purchase_events_user_id,
    most_recent_purchase_events.email  AS most_recent_purchase_events_email,
    most_recent_purchase_events.subscription_status  AS most_recent_purchase_events_subscription_status,
    most_recent_purchase_events.subscription_frequency  AS most_recent_purchase_events_subscription_frequency,
        (CASE WHEN most_recent_purchase_events.subscriber_marketing_opt_in  THEN 'Yes' ELSE 'No' END) AS most_recent_purchase_events_subscriber_marketing_opt_in,
    most_recent_purchase_events.platform  AS most_recent_purchase_events_platform,
    most_recent_purchase_events.topic  AS most_recent_purchase_events_topic,
    most_recent_purchase_events.ts AS most_recent_purchase_events_ts,
    ROW_NUMBER() OVER (PARTITION BY most_recent_purchase_events.user_id ORDER BY most_recent_purchase_events.ts) as col
FROM most_recent_purchase_events
GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8
ORDER BY
    1
)
/* only returns rows that have the most recent topic, as flagged by the filter table */
SELECT most_recent_purchase_events_user_id as user_id
, most_recent_purchase_events_email as email
, most_recent_purchase_events_subscription_status as subscription_status
, most_recent_purchase_events_subscription_frequency as frequency
, most_recent_purchase_events_subscriber_marketing_opt_in as moptin
, most_recent_purchase_events_platform as platform
, most_recent_purchase_events_topic as topic
, "upff" as brand
FROM filter WHERE col = 1
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
