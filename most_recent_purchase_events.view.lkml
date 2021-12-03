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
    most_recent_purchase_events.user_id  AS user_id
    , most_recent_purchase_events.email  AS email
    , most_recent_purchase_events.first_name AS first_name
    , most_recent_purchase_events.last_name AS last_name
    , most_recent_purchase_events.subscription_status  AS subscription_status
    , most_recent_purchase_events.subscription_frequency  AS frequency
    , (CASE WHEN most_recent_purchase_events.subscriber_marketing_opt_in  THEN 'Yes' ELSE 'No' END) AS moptin
    , most_recent_purchase_events.platform AS platform
    , most_recent_purchase_events.topic AS topic
    , most_recent_purchase_events.plan AS plan
    , most_recent_purchase_events.referrer AS referrer
    , most_recent_purchase_events.ts AS ts
    , ROW_NUMBER() OVER (PARTITION BY most_recent_purchase_events.user_id ORDER BY most_recent_purchase_events.ts) as col
FROM most_recent_purchase_events
GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8,
    9,
    10,
    11,
    12
ORDER BY
    1
)
/* only returns rows that have the most recent topic, as flagged by the filter table. Add brand column with constant as "upff" */
SELECT *
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

  dimension: frequency {
    type: string
    sql: ${TABLE}.frequency ;;
  }

  dimension: subscription_status {
    type: string
    sql: ${TABLE}.subscription_status ;;
  }

  dimension: moptin {
    type: yesno
    sql:  ${TABLE}.moptin ;;
  }

  dimension: brand {
    type: string
    sql:  ${TABLE}.brand ;;
  }

  set: detail {
    fields: [
      topic,
      user_id,
      email,
      first_name,
      last_name,
      platform,
      frequency,
      subscription_status
    ]
  }
}
