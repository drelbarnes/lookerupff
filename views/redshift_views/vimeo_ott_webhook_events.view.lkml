view: vimeo_ott_webhook_events {
  derived_table: {
    sql: with purchase_events as (
      select "timestamp", user_id, event, campaign, city, country, created_at, email, first_name, last_name, last_payment_date, marketing_opt_in, name, next_payment_date, plan, platform, promotion_code, referrer, region, registered_to_site, source, subscribed_to_site, subscription_frequency, subscription_price, subscription_status, updated_at
      from vimeo_ott_webhook.customer_product_free_trial_created
      union all
      select "timestamp", user_id, event, campaign, city, country, created_at, email, first_name, last_name, last_payment_date, marketing_opt_in, name, next_payment_date, plan, platform, promotion_code, referrer, region, registered_to_site, source, subscribed_to_site, subscription_frequency, subscription_price, subscription_status, updated_at
      from vimeo_ott_webhook.customer_product_created
      union all
      select "timestamp", user_id, event, campaign, city, country, created_at, email, first_name, last_name, last_payment_date, marketing_opt_in, name, next_payment_date, plan, platform, promotion_code, referrer, region, registered_to_site, source, subscribed_to_site, subscription_frequency, subscription_price, subscription_status, updated_at
      from vimeo_ott_webhook.customer_created
      union all
      -- note about product expired events: they trigger at the exact same time as the preceding charge failed event. So we increment the "timestamp" by 1 minute to set them apart.
      select "timestamp" + INTERVAL '1 minute' AS "timestamp", user_id, event, campaign, city, country, created_at, email, first_name, last_name, last_payment_date, marketing_opt_in, name, next_payment_date, plan, platform, promotion_code, referrer, region, registered_to_site, source, subscribed_to_site, subscription_frequency, subscription_price, subscription_status, updated_at
      from vimeo_ott_webhook.customer_product_expired
      union all
      select "timestamp", user_id, event, campaign, city, country, created_at, email, first_name, last_name, last_payment_date, marketing_opt_in, name, next_payment_date, plan, platform, promotion_code, referrer, region, registered_to_site, source, subscribed_to_site, subscription_frequency, subscription_price, subscription_status, updated_at
      from vimeo_ott_webhook.customer_product_free_trial_converted
      union all
      select "timestamp", user_id, event, campaign, city, country, created_at, email, first_name, last_name, last_payment_date, marketing_opt_in, name, next_payment_date, plan, platform, promotion_code, referrer, region, registered_to_site, source, subscribed_to_site, subscription_frequency, subscription_price, subscription_status, updated_at
      from vimeo_ott_webhook.customer_product_free_trial_expired
      union all
      select "timestamp", user_id, event, campaign, city, country, created_at, email, first_name, last_name, last_payment_date, marketing_opt_in, name, next_payment_date, plan, platform, promotion_code, referrer, region, registered_to_site, source, subscribed_to_site, subscription_frequency, subscription_price, subscription_status, updated_at
      from vimeo_ott_webhook.customer_product_charge_failed
      union all
      select "timestamp", user_id, event, campaign, city, country, created_at, email, first_name, last_name, last_payment_date, marketing_opt_in, name, next_payment_date, plan, platform, promotion_code, referrer, region, registered_to_site, source, subscribed_to_site, subscription_frequency, subscription_price, subscription_status, updated_at
      from vimeo_ott_webhook.customer_product_renewed
      union all
      select "timestamp", user_id, event, campaign, city, country, created_at, email, first_name, last_name, last_payment_date, marketing_opt_in, name, next_payment_date, plan, platform, promotion_code, referrer, region, registered_to_site, source, subscribed_to_site, subscription_frequency, subscription_price, subscription_status, updated_at
      from vimeo_ott_webhook.customer_product_set_cancellation
      union all
      select "timestamp", user_id, event, campaign, city, country, created_at, email, first_name, last_name, last_payment_date, marketing_opt_in, name, next_payment_date, plan, platform, promotion_code, referrer, region, registered_to_site, source, subscribed_to_site, subscription_frequency, subscription_price, subscription_status, updated_at
      from vimeo_ott_webhook.customer_product_cancelled
      union all
      select "timestamp", user_id, event, campaign, city, country, created_at, email, first_name, last_name, last_payment_date, marketing_opt_in, name, next_payment_date, plan, platform, promotion_code, referrer, region, registered_to_site, source, subscribed_to_site, subscription_frequency, subscription_price, subscription_status, updated_at
      from vimeo_ott_webhook.customer_product_resumed
      union all
      select "timestamp", user_id, event, campaign, city, country, created_at, email, first_name, last_name, last_payment_date, marketing_opt_in, name, next_payment_date, plan, platform, promotion_code, referrer, region, registered_to_site, source, subscribed_to_site, subscription_frequency, subscription_price, subscription_status, updated_at
      from vimeo_ott_webhook.customer_product_updated
      union all
      select "timestamp", user_id, event, campaign, city, country, created_at, email, first_name, last_name, last_payment_date, marketing_opt_in, name, next_payment_date, plan, platform, promotion_code, referrer, region, registered_to_site, source, subscribed_to_site, subscription_frequency, subscription_price, subscription_status, updated_at
      from vimeo_ott_webhook.customer_product_paused
      union all
      select "timestamp", user_id, event, campaign, city, country, created_at, email, first_name, last_name, last_payment_date, marketing_opt_in, name, next_payment_date, plan, platform, promotion_code, referrer, region, registered_to_site, source, subscribed_to_site, subscription_frequency, subscription_price, subscription_status, updated_at
      from vimeo_ott_webhook.customer_product_disabled
      union all
      select "timestamp", user_id, event, campaign, city, country, created_at, email, first_name, last_name, last_payment_date, marketing_opt_in, name, next_payment_date, plan, platform, promotion_code, referrer, region, registered_to_site, source, subscribed_to_site, subscription_frequency, subscription_price, subscription_status, updated_at
      from vimeo_ott_webhook.customer_product_undo_set_paused
      union all
      select "timestamp", user_id, event, campaign, city, country, created_at, email, first_name, last_name, last_payment_date, marketing_opt_in, name, next_payment_date, plan, platform, promotion_code, referrer, region, registered_to_site, source, subscribed_to_site, subscription_frequency, subscription_price, subscription_status, updated_at
      from vimeo_ott_webhook.customer_product_set_paused
      union all
      select "timestamp", user_id, event, campaign, city, country, created_at, email, first_name, last_name, last_payment_date, marketing_opt_in, name, next_payment_date, plan, platform, promotion_code, referrer, region, registered_to_site, source, subscribed_to_site, subscription_frequency, subscription_price, subscription_status, updated_at
      from vimeo_ott_webhook.customer_product_undo_set_cancellation
      union all
      select "timestamp", user_id, event, campaign, city, country, created_at, null::VARCHAR as email, null::VARCHAR as first_name, null::VARCHAR as last_name, last_payment_date, marketing_opt_in, null::VARCHAR as name, next_payment_date, plan, platform, promotion_code, referrer, region, registered_to_site, source, subscribed_to_site, subscription_frequency, subscription_price, subscription_status, updated_at
      from vimeo_ott_webhook.customer_deleted
    )
    select *, row_number() over (order by "timestamp", user_id) as row from purchase_events where platform in ('tvos', 'android_tv', 'ios', 'amazon_fire_tv', 'web', 'android', 'roku')
    ;;
    datagroup_trigger: upff_event_processing
    distribution_style: all
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: row {
    primary_key: yes
    type: number
    sql: ${TABLE}.row ;;
  }

  dimension_group: timestamp {
    type: time
    sql: ${TABLE}.timestamp ;;
  }

  dimension: user_id {
    type: string
    sql: ${TABLE}.user_id ;;
  }

  dimension: event {
    type: string
    sql: ${TABLE}.event ;;
  }

  dimension: campaign {
    type: string
    sql: ${TABLE}.campaign ;;
  }

  dimension: city {
    type: string
    sql: ${TABLE}.city ;;
  }

  dimension: country {
    type: string
    sql: ${TABLE}.country ;;
  }

  dimension_group: created_at {
    type: time
    sql: ${TABLE}.created_at ;;
  }

  dimension: email {
    type: string
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

  dimension_group: last_payment_date {
    type: time
    sql: ${TABLE}.last_payment_date ;;
  }

  dimension: marketing_opt_in {
    type: yesno
    sql: ${TABLE}.marketing_opt_in ;;
  }

  dimension: name {
    type: string
    sql: ${TABLE}.name ;;
  }

  dimension_group: next_payment_date {
    type: time
    sql: ${TABLE}.next_payment_date ;;
  }

  dimension: plan {
    type: string
    sql: ${TABLE}.plan ;;
  }

  dimension: platform {
    type: string
    sql: ${TABLE}.platform ;;
  }

  dimension: promotion_code {
    type: string
    sql: ${TABLE}.promotion_code ;;
  }

  dimension: referrer {
    type: string
    sql: ${TABLE}.referrer ;;
  }

  dimension: region {
    type: string
    sql: ${TABLE}.region ;;
  }

  dimension: registered_to_site {
    type: yesno
    sql: ${TABLE}.registered_to_site ;;
  }

  dimension: source {
    type: string
    sql: ${TABLE}.source ;;
  }

  dimension: subscribed_to_site {
    type: yesno
    sql: ${TABLE}.subscribed_to_site ;;
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

  dimension_group: updated_at {
    type: time
    sql: ${TABLE}.updated_at ;;
  }

  set: detail {
    fields: [
      timestamp_time,
      user_id,
      event,
      campaign,
      city,
      country,
      created_at_time,
      email,
      first_name,
      last_name,
      last_payment_date_time,
      marketing_opt_in,
      name,
      next_payment_date_time,
      plan,
      platform,
      promotion_code,
      referrer,
      region,
      registered_to_site,
      source,
      subscribed_to_site,
      subscription_frequency,
      subscription_price,
      subscription_status,
      updated_at_time,
      row
    ]
  }
}
