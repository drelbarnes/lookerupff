view: validate_dunning {
  derived_table: {
    sql: with customers as (
      SELECT
      TIMESTAMP_MILLIS(cast(properties_lastmodifieddate_value as int)) as last_modified
      , email
      , properties_firstname_value
      , properties_lastname_value
      , properties_moptin_value
      , properties_topic_value
      , properties_subscription_status_value
      , properties_frequency_value
      , properties_churn_score_value
      , properties_vod_brand_value
      , ROW_NUMBER() OVER (PARTITION BY email ORDER BY TIMESTAMP_MILLIS(cast(properties_lastmodifieddate_value as int)) DESC) as n
      FROM `up-faith-and-family-216419.hubspot.contacts`
      where email is not null and properties_topic_value = "customer.product.charge_failed" and properties_subscription_status_value = "enabled"
      )
      , c as (SELECT email
      , properties_firstname_value
      , properties_lastname_value
      , properties_moptin_value
      , properties_topic_value
      , properties_subscription_status_value
      , properties_frequency_value
      , properties_churn_score_value
      , properties_vod_brand_value
      , last_modified
      FROM customers
      WHERE n = 1
      )
      , purchase_events as (SELECT
            user_id
            , email
            , first_name
            , last_name
            , platform
            , plan
            , subscription_frequency
            , topic
            , subscription_status
            , moptin as subscriber_marketing_opt_in
            , timestamp
            , ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY timestamp DESC) as n2
            FROM `up-faith-and-family-216419.http_api.purchase_event`
            WHERE email in (select email from c)
      ) /* most_recent_purchase_events gets the most recent topic for each distinct email address, along with all other fields related to HubSpot contact */
      /* filter table flags any users with multiple topics and orders their records by descending time stamp */
      , f as (SELECT user_id
            , email
            , first_name
            , last_name
            , topic
            , platform
            , plan
            , subscription_frequency
            , subscription_status
            , subscriber_marketing_opt_in
            , timestamp
      FROM purchase_events
      WHERE n2 = 1
      )
      SELECT user_id
      , f.email
      , first_name
      , last_name
      , topic
      , platform
      , plan
      , subscription_frequency
      , subscription_status
      , subscriber_marketing_opt_in
      FROM f LEFT JOIN c ON f.email = c.email
      WHERE f.timestamp <= c.last_modified
      AND
      f.topic not in (c.properties_topic_value) and f.subscription_status not in (c.properties_subscription_status_value)
       ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
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

  dimension: subscription_frequency {
    type: string
    sql: ${TABLE}.subscription_frequency ;;
  }

  dimension: subscription_status {
    type: string
    sql: ${TABLE}.subscription_status ;;
  }

  dimension: subscriber_marketing_opt_in {
    type: string
    sql: ${TABLE}.subscriber_marketing_opt_in ;;
  }

  set: detail {
    fields: [
      user_id,
      email,
      first_name,
      last_name,
      topic,
      platform,
      plan,
      subscription_frequency,
      subscription_status,
      subscriber_marketing_opt_in
    ]
  }
}
