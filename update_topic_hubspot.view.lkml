view: update_topic_hubspot {
  derived_table: {
    sql: with customers as (
      -- this table contains historical records of all key hubspot contact properties, ranked by descending last_modified date
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
      )
      , c as (
      -- this table contains the most recent record for each distinct user in the customers table
      SELECT email
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
      , purchase_events as (
      -- this table contains all pertinant purchase event data from the webhook, ranked by descending timestamp
      SELECT user_id
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
      )
      , p as (
      -- this table contains the most recent purchase event for each distinct user in the purchase_events table
      SELECT user_id
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
      -- we join c and p, select the relevant columns and filter out
      -- 1) purchase events that come after the latest record in c (this is to account for the delay in syncing HubSpot data with our warehouses
      -- 2) records in c that meet the dunning workflow enrollment criteria
      -- 3) records in p that do not match records in c, which is to say that the record's HubSpot contact data does not align with the latest webhook event
      -- we are left with a table of users that need to have their HubSpot properties updated.
      SELECT user_id
      , p.email
      , p.timestamp
      , c.last_modified
      , first_name
      , last_name
      , topic
      , c.properties_topic_value as hubspot_topic
      , platform
      , plan
      , subscription_frequency
      , subscription_status
      , c.properties_subscription_status_value as hubspot_status
      , subscriber_marketing_opt_in
      FROM p LEFT JOIN c ON p.email = c.email
      WHERE p.timestamp <= c.last_modified
      -- AND
      -- c.properties_topic_value = "customer.product.charge_failed" and c.properties_subscription_status_value = "enabled"
      -- AND
      -- p.topic not in (c.properties_topic_value) and p.subscription_status not in (c.properties_subscription_status_value)
       ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: user_id {
    type: string
    sql: ${TABLE}.user_id ;;
  }

  dimension: email {
    type: string
    sql: ${TABLE}.email ;;
  }

  dimension_group: timestamp {
    type: time
    sql: ${TABLE}.timestamp ;;
  }

  dimension_group: last_modified {
    type: time
    sql: ${TABLE}.last_modified ;;
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

  dimension: hubspot_topic {
    type: string
    sql: ${TABLE}.hubspot_topic ;;
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

  dimension: hubspot_status {
    type: string
    sql: ${TABLE}.hubspot_status ;;
  }

  dimension: subscriber_marketing_opt_in {
    type: string
    sql: ${TABLE}.subscriber_marketing_opt_in ;;
  }

  set: detail {
    fields: [
      user_id,
      email,
      timestamp_time,
      last_modified_time,
      first_name,
      last_name,
      topic,
      hubspot_topic,
      platform,
      plan,
      subscription_frequency,
      subscription_status,
      hubspot_status,
      subscriber_marketing_opt_in
    ]
  }
}
