view: update_topic_hubspot {
  derived_table: {
    sql: -- This table contains all records of events that were sent by the Vimeo OTT webhook but not by our webhook
      with missing_records as (
      -- the tables below (hn, wn and jn) serve to join the Vimeo OTT webhook tables with their respective records from the http_api table broken out by event
      with h1 as (
      SELECT user_id as h_user_id, email as h_email, subscription_status as h_status, subscription_frequency as h_frequency, topic as h_topic, moptin as h_moptin, status_date as h_datetime
      FROM `up-faith-and-family-216419.http_api.purchase_event`
      WHERE topic = "customer.product.cancelled"
      ORDER BY status_date
      )
      , w1 as (
      SELECT user_id as w_user_id, email as w_email, subscription_status as w_status, subscription_frequency as w_frequency, event_text as w_topic, marketing_opt_in as w_moptin, timestamp as w_datetime
      FROM `up-faith-and-family-216419.vimeo_ott_webhook.customer_product_cancelled`
      ORDER BY timestamp
      )
      , j1 as (
      SELECT w_user_id, w_email, w_status, w_frequency, w_topic, w_moptin, w_datetime, h_user_id, h_email, h_status, h_frequency, h_topic, h_moptin, h_datetime
      FROM h1
      FULL JOIN w1 on h_user_id = w_user_id
      ORDER BY w_datetime desc
      )
      , h2 as(
      SELECT user_id as h_user_id, email as h_email, subscription_status as h_status, subscription_frequency as h_frequency, topic as h_topic, moptin as h_moptin, status_date as h_datetime
      FROM `up-faith-and-family-216419.http_api.purchase_event`
      WHERE topic = "customer.product.charge_failed"
      ORDER BY status_date
      )
      , w2 as (
      SELECT user_id as w_user_id, email as w_email, subscription_status as w_status, subscription_frequency as w_frequency, event_text as w_topic, marketing_opt_in as w_moptin, timestamp as w_datetime
      FROM `up-faith-and-family-216419.vimeo_ott_webhook.customer_product_charge_failed`
      ORDER BY timestamp
      )
      , j2 as (
      SELECT w_user_id, w_email, w_status, w_frequency, w_topic, w_moptin, w_datetime, h_user_id, h_email, h_status, h_frequency, h_topic, h_moptin, h_datetime
      FROM h2
      FULL JOIN w2 on h_user_id = w_user_id
      ORDER BY w_datetime desc
      )
      , h3 as(
      SELECT user_id as h_user_id, email as h_email, subscription_status as h_status, subscription_frequency as h_frequency, topic as h_topic, moptin as h_moptin, status_date as h_datetime
      FROM `up-faith-and-family-216419.http_api.purchase_event`
      WHERE topic = "customer.product.created"
      ORDER BY status_date
      )
      , w3 as (
      SELECT user_id as w_user_id, email as w_email, subscription_status as w_status, subscription_frequency as w_frequency, event_text as w_topic, marketing_opt_in as w_moptin, timestamp as w_datetime
      FROM `up-faith-and-family-216419.vimeo_ott_webhook.customer_product_created`
      ORDER BY timestamp
      )
      , j3 as (
      SELECT w_user_id, w_email, w_status, w_frequency, w_topic, w_moptin, w_datetime, h_user_id, h_email, h_status, h_frequency, h_topic, h_moptin, h_datetime
      FROM h3
      FULL JOIN w3 on h_user_id = w_user_id
      ORDER BY w_datetime desc
      )
      , h4 as(
      SELECT user_id as h_user_id, email as h_email, subscription_status as h_status, subscription_frequency as h_frequency, topic as h_topic, moptin as h_moptin, status_date as h_datetime
      FROM `up-faith-and-family-216419.http_api.purchase_event`
      WHERE topic = "customer.product.disabled"
      ORDER BY status_date
      )
      , w4 as (
      SELECT user_id as w_user_id, email as w_email, subscription_status as w_status, subscription_frequency as w_frequency, event_text as w_topic, marketing_opt_in as w_moptin, timestamp as w_datetime
      FROM `up-faith-and-family-216419.vimeo_ott_webhook.customer_product_disabled`
      ORDER BY timestamp
      )
      , j4 as (
      SELECT w_user_id, w_email, w_status, w_frequency, w_topic, w_moptin, w_datetime, h_user_id, h_email, h_status, h_frequency, h_topic, h_moptin, h_datetime
      FROM h4
      FULL JOIN w4 on h_user_id = w_user_id
      ORDER BY w_datetime desc
      )
      , h5 as(
      SELECT user_id as h_user_id, email as h_email, subscription_status as h_status, subscription_frequency as h_frequency, topic as h_topic, moptin as h_moptin, status_date as h_datetime
      FROM `up-faith-and-family-216419.http_api.purchase_event`
      WHERE topic = "customer.product.expired"
      ORDER BY status_date
      )
      , w5 as (
      SELECT user_id as w_user_id, email as w_email, subscription_status as w_status, subscription_frequency as w_frequency, event_text as w_topic, marketing_opt_in as w_moptin, timestamp as w_datetime
      FROM `up-faith-and-family-216419.vimeo_ott_webhook.customer_product_expired`
      ORDER BY timestamp
      )
      , j5 as (
      SELECT w_user_id, w_email, w_status, w_frequency, w_topic, w_moptin, w_datetime, h_user_id, h_email, h_status, h_frequency, h_topic, h_moptin, h_datetime
      FROM h5
      FULL JOIN w5 on h_user_id = w_user_id
      ORDER BY w_datetime desc
      )
      , h6 as(
      SELECT user_id as h_user_id, email as h_email, subscription_status as h_status, subscription_frequency as h_frequency, topic as h_topic, moptin as h_moptin, status_date as h_datetime
      FROM `up-faith-and-family-216419.http_api.purchase_event`
      WHERE topic = "customer.product.free_trial_converted"
      ORDER BY status_date
      )
      , w6 as (
      SELECT user_id as w_user_id, email as w_email, subscription_status as w_status, subscription_frequency as w_frequency, event_text as w_topic, marketing_opt_in as w_moptin, timestamp as w_datetime
      FROM `up-faith-and-family-216419.vimeo_ott_webhook.customer_product_free_trial_converted`
      ORDER BY timestamp
      )
      , j6 as (
      SELECT w_user_id, w_email, w_status, w_frequency, w_topic, w_moptin, w_datetime, h_user_id, h_email, h_status, h_frequency, h_topic, h_moptin, h_datetime
      FROM h6
      FULL JOIN w6 on h_user_id = w_user_id
      ORDER BY w_datetime desc
      )
      , h7 as(
      SELECT user_id as h_user_id, email as h_email, subscription_status as h_status, subscription_frequency as h_frequency, topic as h_topic, moptin as h_moptin, status_date as h_datetime
      FROM `up-faith-and-family-216419.http_api.purchase_event`
      WHERE topic = "customer.product.free_trial_created"
      ORDER BY status_date
      )
      , w7 as (
      SELECT user_id as w_user_id, email as w_email, subscription_status as w_status, subscription_frequency as w_frequency, event_text as w_topic, marketing_opt_in as w_moptin, timestamp as w_datetime
      FROM `up-faith-and-family-216419.vimeo_ott_webhook.customer_product_free_trial_created`
      ORDER BY timestamp
      )
      , j7 as (
      SELECT w_user_id, w_email, w_status, w_frequency, w_topic, w_moptin, w_datetime, h_user_id, h_email, h_status, h_frequency, h_topic, h_moptin, h_datetime
      FROM h7
      FULL JOIN w7 on h_user_id = w_user_id
      ORDER BY w_datetime desc
      )
      , h8 as(
      SELECT user_id as h_user_id, email as h_email, subscription_status as h_status, subscription_frequency as h_frequency, topic as h_topic, moptin as h_moptin, status_date as h_datetime
      FROM `up-faith-and-family-216419.http_api.purchase_event`
      WHERE topic = "customer.product.free_trial_expired"
      ORDER BY status_date
      )
      , w8 as (
      SELECT user_id as w_user_id, email as w_email, subscription_status as w_status, subscription_frequency as w_frequency, event_text as w_topic, marketing_opt_in as w_moptin, timestamp as w_datetime
      FROM `up-faith-and-family-216419.vimeo_ott_webhook.customer_product_free_trial_expired`
      ORDER BY timestamp
      )
      , j8 as (
      SELECT w_user_id, w_email, w_status, w_frequency, w_topic, w_moptin, w_datetime, h_user_id, h_email, h_status, h_frequency, h_topic, h_moptin, h_datetime
      FROM h8
      FULL JOIN w8 on h_user_id = w_user_id
      ORDER BY w_datetime desc
      )
      , h9 as(
      SELECT user_id as h_user_id, email as h_email, subscription_status as h_status, subscription_frequency as h_frequency, topic as h_topic, moptin as h_moptin, status_date as h_datetime
      FROM `up-faith-and-family-216419.http_api.purchase_event`
      WHERE topic = "customer.product.paused"
      ORDER BY status_date
      )
      , w9 as (
      SELECT user_id as w_user_id, email as w_email, subscription_status as w_status, subscription_frequency as w_frequency, event_text as w_topic, marketing_opt_in as w_moptin, timestamp as w_datetime
      FROM `up-faith-and-family-216419.vimeo_ott_webhook.customer_product_paused`
      ORDER BY timestamp
      )
      , j9 as (
      SELECT w_user_id, w_email, w_status, w_frequency, w_topic, w_moptin, w_datetime, h_user_id, h_email, h_status, h_frequency, h_topic, h_moptin, h_datetime
      FROM h9
      FULL JOIN w9 on h_user_id = w_user_id
      ORDER BY w_datetime desc
      )
      , h10 as(
      SELECT user_id as h_user_id, email as h_email, subscription_status as h_status, subscription_frequency as h_frequency, topic as h_topic, moptin as h_moptin, status_date as h_datetime
      FROM `up-faith-and-family-216419.http_api.purchase_event`
      WHERE topic = "customer.product.renewed"
      ORDER BY status_date
      )
      , w10 as (
      SELECT user_id as w_user_id, email as w_email, subscription_status as w_status, subscription_frequency as w_frequency, event_text as w_topic, marketing_opt_in as w_moptin, timestamp as w_datetime
      FROM `up-faith-and-family-216419.vimeo_ott_webhook.customer_product_renewed`
      ORDER BY timestamp
      )
      , j10 as (
      SELECT w_user_id, w_email, w_status, w_frequency, w_topic, w_moptin, w_datetime, h_user_id, h_email, h_status, h_frequency, h_topic, h_moptin, h_datetime
      FROM h10
      FULL JOIN w10 on h_user_id = w_user_id
      ORDER BY w_datetime desc
      )
      , h11 as(
      SELECT user_id as h_user_id, email as h_email, subscription_status as h_status, subscription_frequency as h_frequency, topic as h_topic, moptin as h_moptin, status_date as h_datetime
      FROM `up-faith-and-family-216419.http_api.purchase_event`
      WHERE topic = "customer.product.resumed"
      ORDER BY status_date
      )
      , w11 as (
      SELECT user_id as w_user_id, email as w_email, subscription_status as w_status, subscription_frequency as w_frequency, event_text as w_topic, marketing_opt_in as w_moptin, timestamp as w_datetime
      FROM `up-faith-and-family-216419.vimeo_ott_webhook.customer_product_resumed`
      ORDER BY timestamp
      )
      , j11 as (
      SELECT w_user_id, w_email, w_status, w_frequency, w_topic, w_moptin, w_datetime, h_user_id, h_email, h_status, h_frequency, h_topic, h_moptin, h_datetime
      FROM h11
      FULL JOIN w11 on h_user_id = w_user_id
      ORDER BY w_datetime desc
      )
      , h12 as(
      SELECT user_id as h_user_id, email as h_email, subscription_status as h_status, subscription_frequency as h_frequency, topic as h_topic, moptin as h_moptin, status_date as h_datetime
      FROM `up-faith-and-family-216419.http_api.purchase_event`
      WHERE topic = "customer.product.set_cancellation"
      ORDER BY status_date
      )
      , w12 as (
      SELECT user_id as w_user_id, email as w_email, subscription_status as w_status, subscription_frequency as w_frequency, event_text as w_topic, marketing_opt_in as w_moptin, timestamp as w_datetime
      FROM `up-faith-and-family-216419.vimeo_ott_webhook.customer_product_set_cancellation`
      ORDER BY timestamp
      )
      , j12 as (
      SELECT w_user_id, w_email, w_status, w_frequency, w_topic, w_moptin, w_datetime, h_user_id, h_email, h_status, h_frequency, h_topic, h_moptin, h_datetime
      FROM h12
      FULL JOIN w12 on h_user_id = w_user_id
      ORDER BY w_datetime desc
      )
      , h13 as(
      SELECT user_id as h_user_id, email as h_email, subscription_status as h_status, subscription_frequency as h_frequency, topic as h_topic, moptin as h_moptin, status_date as h_datetime
      FROM `up-faith-and-family-216419.http_api.purchase_event`
      WHERE topic = "customer.product.set_paused"
      ORDER BY status_date
      )
      , w13 as (
      SELECT user_id as w_user_id, email as w_email, subscription_status as w_status, subscription_frequency as w_frequency, event_text as w_topic, marketing_opt_in as w_moptin, timestamp as w_datetime
      FROM `up-faith-and-family-216419.vimeo_ott_webhook.customer_product_set_paused`
      ORDER BY timestamp
      )
      , j13 as (
      SELECT w_user_id, w_email, w_status, w_frequency, w_topic, w_moptin, w_datetime, h_user_id, h_email, h_status, h_frequency, h_topic, h_moptin, h_datetime
      FROM h13
      FULL JOIN w13 on h_user_id = w_user_id
      ORDER BY w_datetime desc
      )
      , h14 as(
      SELECT user_id as h_user_id, email as h_email, subscription_status as h_status, subscription_frequency as h_frequency, topic as h_topic, moptin as h_moptin, status_date as h_datetime
      FROM `up-faith-and-family-216419.http_api.purchase_event`
      WHERE topic = "customer.product.undo_set_cancellation"
      ORDER BY status_date
      )
      , w14 as (
      SELECT user_id as w_user_id, email as w_email, subscription_status as w_status, subscription_frequency as w_frequency, event_text as w_topic, marketing_opt_in as w_moptin, timestamp as w_datetime
      FROM `up-faith-and-family-216419.vimeo_ott_webhook.customer_product_undo_set_cancellation`
      ORDER BY timestamp
      )
      , j14 as (
      SELECT w_user_id, w_email, w_status, w_frequency, w_topic, w_moptin, w_datetime, h_user_id, h_email, h_status, h_frequency, h_topic, h_moptin, h_datetime
      FROM h14
      FULL JOIN w14 on h_user_id = w_user_id
      ORDER BY w_datetime desc
      )
      , h15 as(
      SELECT user_id as h_user_id, email as h_email, subscription_status as h_status, subscription_frequency as h_frequency, topic as h_topic, moptin as h_moptin, status_date as h_datetime
      FROM `up-faith-and-family-216419.http_api.purchase_event`
      WHERE topic = "customer.product.undo_set_paused"
      ORDER BY status_date
      )
      , w15 as (
      SELECT user_id as w_user_id, email as w_email, subscription_status as w_status, subscription_frequency as w_frequency, event_text as w_topic, marketing_opt_in as w_moptin, timestamp as w_datetime
      FROM `up-faith-and-family-216419.vimeo_ott_webhook.customer_product_undo_set_paused`
      ORDER BY timestamp
      )
      , j15 as (
      SELECT w_user_id, w_email, w_status, w_frequency, w_topic, w_moptin, w_datetime, h_user_id, h_email, h_status, h_frequency, h_topic, h_moptin, h_datetime
      FROM h15
      FULL JOIN w15 on h_user_id = w_user_id
      ORDER BY w_datetime desc
      )
      , h16 as(
      SELECT user_id as h_user_id, email as h_email, subscription_status as h_status, subscription_frequency as h_frequency, topic as h_topic, moptin as h_moptin, status_date as h_datetime
      FROM `up-faith-and-family-216419.http_api.purchase_event`
      WHERE topic = "customer.product.updated"
      ORDER BY status_date
      )
      , w16 as (
      SELECT user_id as w_user_id, email as w_email, subscription_status as w_status, subscription_frequency as w_frequency, event_text as w_topic, marketing_opt_in as w_moptin, timestamp as w_datetime
      FROM `up-faith-and-family-216419.vimeo_ott_webhook.customer_product_updated`
      ORDER BY timestamp
      )
      , j16 as (
      SELECT w_user_id, w_email, w_status, w_frequency, w_topic, w_moptin, w_datetime, h_user_id, h_email, h_status, h_frequency, h_topic, h_moptin, h_datetime
      FROM h16
      FULL JOIN w16 on h_user_id = w_user_id
      ORDER BY w_datetime desc
      )
      -- the following table is the union of all the records from the joined tables above where there is no corresponding event in hn to any given record in wn
      -- which is to say, we're looking for events sent by the Vimeo OTT webhook that were not sent by our webhook
      , u as (
      select * from j1 where h_user_id is NULL
      UNION ALL
      select * from j2 where h_user_id is NULL
      UNION ALL
      select * from j3 where h_user_id is NULL
      UNION ALL
      select * from j4 where h_user_id is NULL
      UNION ALL
      select * from j5 where h_user_id is NULL
      UNION ALL
      select * from j6 where h_user_id is NULL
      UNION ALL
      select * from j7 where h_user_id is NULL
      UNION ALL
      select * from j8 where h_user_id is NULL
      UNION ALL
      select * from j9 where h_user_id is NULL
      UNION ALL
      select * from j10 where h_user_id is NULL
      UNION ALL
      select * from j11 where h_user_id is NULL
      UNION ALL
      select * from j12 where h_user_id is NULL
      UNION ALL
      select * from j13 where h_user_id is NULL
      UNION ALL
      select * from j14 where h_user_id is NULL
      UNION ALL
      select * from j15 where h_user_id is NULL
      UNION ALL
      select * from j16 where h_user_id is NULL
      )
      -- finally, we reformat the union table above so we can insert the missing records into a derived http_api table
      SELECT w_user_id as user_id, w_email as email, w_status as subscription_status, w_frequency as subscription_frequency, w_topic as topic, w_moptin as moptin, w_datetime as status_date
      FROM u
      ORDER BY w_datetime DESC
      )
      -- this is the union of the missing event records and all the http api webhook purchase events
      , p as (
      with purchase_events as
      (
      SELECT user_id, email, subscription_status, subscription_frequency, topic, moptin, status_date
      FROM `up-faith-and-family-216419.http_api.purchase_event`
      -- WHERE user_id in (SELECT distinct user_id FROM missing_records)
      UNION ALL
      SELECT * FROM missing_records
      )
      -- we rank the records from the table above by descending datetime then select the most recent purchase events
      , ranked as (
      SELECT *
      , RANK() OVER (PARTITION BY user_id ORDER BY status_date DESC) AS rank
      FROM purchase_events
      )
      , most_recent as (
      SELECT user_id, email, subscription_status, subscription_frequency, topic, moptin, status_date, rank
      FROM ranked
      WHERE rank = 1 AND
      ((( status_date ) >= ((TIMESTAMP_ADD(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/New_York'), INTERVAL -(30 - 1) DAY))) AND
      ( status_date ) < ((TIMESTAMP_ADD(TIMESTAMP_ADD(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY, 'America/New_York'), INTERVAL -(30 - 1) DAY), INTERVAL 30 DAY)))))
      )
      -- this part is a bit hacky. Sometimes, charge failed and product.expired events fire at the same time. The table below serves to filter out the charge_failed records and leave the product.expired records
      , x as (
      SELECT * FROM most_recent
      EXCEPT DISTINCT
      SELECT user_id, email, subscription_status, subscription_frequency, topic, moptin, status_date, rank
      FROM most_recent
      WHERE user_id in (
      SELECT user_id
      FROM most_recent
      WHERE topic in ('customer.product.charge_failed') AND user_id in (SELECT user_id FROM most_recent WHERE topic in ('customer.product.free_trial_expired', 'customer.product.expired'))
      )
      AND topic in ('customer.product.charge_failed')
      )
      SELECT user_id, email, subscription_status, subscription_frequency, topic, moptin, status_date FROM x
      )
      -- this table contains historical records of all key hubspot contact properties, ranked by descending last_modified date
      , customers as (
      SELECT email
      , properties_subscription_status_value as subscription_status
      , properties_frequency_value as subscription_frequency
      , properties_topic_value as topic
      , properties_moptin_value as moptin
      , TIMESTAMP_MILLIS(cast(properties_lastmodifieddate_value as int)) as last_modified
      , RANK() OVER (PARTITION BY email ORDER BY TIMESTAMP_MILLIS(cast(properties_lastmodifieddate_value as int)) DESC) as rank
      FROM `up-faith-and-family-216419.hubspot.contacts`
      )
      -- this table contains the most recent record for each distinct user in the customers table
      -- note: we group all fields because there are duplicates for some reason
      , c as (
      SELECT email
      , subscription_status
      , subscription_frequency
      , topic
      , moptin
      , last_modified
      , rank
      FROM customers
      WHERE rank = 1
      GROUP BY 1,2,3,4,5,6,7
      )
      , compare as (
      SELECT p.user_id
      , p.email
      , p.status_date
      , c.last_modified
      , p.topic
      , c.topic as hubspot_topic
      , p.subscription_frequency
      , c.subscription_frequency as hubspot_frequency
      , p.subscription_status
      , c.subscription_status as hubspot_status
      , p.moptin
      FROM p LEFT JOIN c ON p.email = c.email
      WHERE p.status_date <= c.last_modified
      )
      SELECT user_id
      , email
      -- Sometime the purchase events are mssing a status or frequency
      -- and we don't want to overwrite the values in HubSpot
      -- HubSpot seems to use an empty list as its null value and we don't what to use that either
      , CASE
          WHEN subscription_status IS NULL THEN
          CASE
            WHEN hubspot_status = "[]" THEN subscription_status
            ELSE hubspot_status
          END
          ELSE subscription_status
        END AS subscription_status
      , CASE
          WHEN subscription_frequency IS NULL THEN
          CASE
            WHEN hubspot_frequency = "[]" THEN subscription_frequency
            ELSE hubspot_frequency
          END
          ELSE subscription_frequency
        END AS subscription_frequency
      , topic
      , moptin
      , status_date
      , last_modified
      FROM compare
      WHERE topic != hubspot_topic OR subscription_status != hubspot_status
      GROUP BY 1,2,3,4,5,6,7,8
      ORDER BY status_date ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: user_id {
    type: string
    primary_key: yes
    tags: ["user_id"]
    sql: ${TABLE}.user_id ;;
  }

  dimension: email {
    type: string
    tags: ["email"]
    sql: ${TABLE}.email ;;
  }

  dimension_group: status_date {
    type: time
    sql: ${TABLE}.status_date ;;
  }

  dimension_group: last_modified {
    type: time
    sql: ${TABLE}.last_modified ;;
  }

  dimension: topic {
    type: string
    sql: ${TABLE}.topic ;;
  }

  dimension: subscription_frequency {
    type: string
    sql: ${TABLE}.subscription_frequency ;;
  }

  dimension: subscription_status {
    type: string
    sql: ${TABLE}.subscription_status ;;
  }

  dimension: moptin {
    type: yesno
    sql: ${TABLE}.moptin ;;
  }

  set: detail {
    fields: [
      user_id,
      email,
      topic,
      subscription_frequency,
      subscription_status,
      moptin
    ]
  }
}
