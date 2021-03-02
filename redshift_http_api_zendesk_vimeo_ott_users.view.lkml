view: redshift_http_api_zendesk_vimeo_ott_users {
  derived_table: {
    sql: SELECT distinct MAX(date(e.status_date)) AS received_at, u.id,e.user_id, u.email, Max(e.topic) as topic, e.platform, e.moptin,e.subscription_frequency, e.subscription_price, e.subscription_status  FROM zendesk.users AS u LEFT JOIN http_api.purchase_event AS e ON u.email = e.email WHERE (user_id IS NOT NULL AND subscription_frequency IS NOT NULL AND subscription_price IS NOT NULL) GROUP BY 2,3,4,6,7,8,9, 10
      ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension_group: received_at {
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

  dimension: id {
    type: string
    sql: ${TABLE}.id ;;
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

  dimension: topic {
    type: string
    sql: CASE
    WHEN ${TABLE}.topic = 'customer.product.created' THEN 'product created'
    WHEN ${TABLE}.topic = 'customer.product.charge_failed' THEN 'charge failed'
    WHEN ${TABLE}.topic = 'customer.product.paused_created' THEN 'paused created'
    WHEN ${TABLE}.topic = 'customer.product.free_trial_converted' THEN 'free trial converted'
    WHEN ${TABLE}.topic = 'customer.product.free_trial_created' THEN 'rree trial created'
    WHEN ${TABLE}.topic = 'customer.product.renewed' THEN 'renewed'
    WHEN ${TABLE}.topic = 'customer.product.set_cancellation' THEN 'set cancellation'
    WHEN ${TABLE}.topic = 'customer.product.set_paused' THEN 'set paused'
    WHEN ${TABLE}.topic = 'customer.product.undo_set_cancellation' THEN 'undo set cancellation'
    WHEN ${TABLE}.topic = 'customer.product.undo_set_paused' THEN 'undo set paused'
    WHEN ${TABLE}.topic = 'customer.product.updated' THEN 'product updated'
    WHEN ${TABLE}.topic = 'customer.updated' THEN 'account updated'
    WHEN ${TABLE}.topic = 'customer.created' THEN 'account created'
    ELSE ${TABLE}.topic
    END ;;
  }

  dimension: platform {
    type: string
    sql: ${TABLE}.platform ;;
  }

  dimension: moptin {
    type: string
    sql: ${TABLE}.moptin ;;
  }

  dimension: subscription_frequency {
    type: string
    sql: ${TABLE}.subscription_frequency ;;
  }

  dimension: subscription_price {
    type: number
    sql: ${TABLE}.subscription_price ;;
  }

  dimension: subscription_status {
    type: string
    sql: ${TABLE}.subscription_status ;;
  }

  set: detail {
    fields: [
      id,
      user_id,
      email,
      topic,
      platform,
      moptin,
      subscription_frequency,
      subscription_price,
      subscription_status
    ]
  }
}
