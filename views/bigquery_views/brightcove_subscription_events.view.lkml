view: brightcove_subscription_events {
  derived_table: {
    sql:
      with p0 as (
      SELECT
        content_customer_id as customer_id,
        content_subscription_id as subscription_id,
        DATE(TIMESTAMP_SECONDS(content_subscription_trial_start)) as trial_start_date,
        DATE(TIMESTAMP_SECONDS(content_subscription_next_billing_at)) as active_start_date,
        safe_cast(null as date) as active_end_date,
        SAFE.PARSE_JSON(content_subscription_subscription_items) as content_subscription_subscription_items,
        CASE WHEN content_subscription_channel = "web" THEN "Direct" else content_subscription_channel END as channel,
        content_subscription_billing_period_unit as subscription_unit,
        CASE WHEN content_customer_promotional_credits > 0 then true else FALSE end as special_offer,
        content_subscription_currency_code as unit_price_currency,
        false as is_involuntary,
        false as is_reconnect
      FROM
        `up-faith-and-family-216419.chargebee_webhook_events.subscription_created`
      )
      , p1 as (
      SELECT
        content_customer_id as customer_id,
        content_subscription_id as subscription_id,
        DATE(TIMESTAMP_SECONDS(content_subscription_trial_start)) as trial_start_date,
        coalesce(DATE(TIMESTAMP_SECONDS(content_subscription_activated_at)), DATE(TIMESTAMP_SECONDS(content_subscription_trial_end))) as active_start_date,
        DATE(TIMESTAMP_SECONDS(content_subscription_cancelled_at)) as active_end_date,
        SAFE.PARSE_JSON(content_subscription_subscription_items) as content_subscription_subscription_items,
        CASE WHEN content_subscription_channel = "web" THEN "Direct" else content_subscription_channel END as channel,
        content_subscription_billing_period_unit as subscription_unit,
        CASE WHEN content_customer_promotional_credits > 0 then true else FALSE end as special_offer,
        content_subscription_currency_code as unit_price_currency,
        case when content_subscription_cancel_reason in ("") then true else false end as is_involuntary,
        false as is_reconnect
      FROM
        `up-faith-and-family-216419.chargebee_webhook_events.subscription_cancelled`
      )
      , p2 as (
      SELECT
        content_customer_id as customer_id,
        content_subscription_id as subscription_id,
        DATE(TIMESTAMP_SECONDS(content_subscription_trial_start)) as trial_start_date,
        DATE(TIMESTAMP_SECONDS(content_subscription_next_billing_at)) as active_start_date,
        safe_cast(null as date) as active_end_date,
        SAFE.PARSE_JSON(content_subscription_subscription_items) as content_subscription_subscription_items,
        CASE WHEN content_subscription_channel = "web" THEN "Direct" else content_subscription_channel END as channel,
        content_subscription_billing_period_unit as subscription_unit,
        CASE WHEN content_customer_promotional_credits > 0 then true else FALSE end as special_offer,
        content_subscription_currency_code as unit_price_currency,
        false as is_involuntary,
        false as is_reconnect
      FROM
        `up-faith-and-family-216419.chargebee_webhook_events.subscription_activated`
      )
      -- , p3 as (
      -- SELECT
      --   content_customer_id as customer_id,
      --   content_subscription_id as subscription_id,
      --   DATE(TIMESTAMP_SECONDS(content_subscription_trial_start)) as trial_start_date,
      --   DATE(TIMESTAMP_SECONDS(content_subscription_next_billing_at)) as active_start_date,
      --   safe_cast(null as date) as active_end_date,
      --   SAFE.PARSE_JSON(content_subscription_subscription_items) as content_subscription_subscription_items,
      --   CASE WHEN content_subscription_channel = "web" THEN "Direct" else content_subscription_channel END as channel,
      --   content_subscription_billing_period_unit as subscription_unit,
      --   CASE WHEN content_customer_promotional_credits > 0 then true else FALSE end as special_offer,
      --   content_subscription_currency_code as unit_price_currency,
      --   false as is_involuntary,
      --   false as is_reconnect
      -- FROM
      --   `up-faith-and-family-216419.chargebee_webhook_events.subscription_reactivated`
      -- )
      select
      customer_id,
      subscription_id,
      trial_start_date,
      active_start_date,
      active_end_date,
      json_value(content_subscription_subscription_item.item_price_id) as product_name,
      json_value(content_subscription_subscription_item.amount) as unit_price,
      subscription_unit,
      channel,
      special_offer,
      unit_price_currency,
      is_involuntary,
      is_reconnect
      from p0, unnest(json_query_array(content_subscription_subscription_items)) as content_subscription_subscription_item
      UNION ALL
      select
      customer_id,
      subscription_id,
      trial_start_date,
      active_start_date,
      active_end_date,
      json_value(content_subscription_subscription_item.item_price_id) as product_name,
      json_value(content_subscription_subscription_item.amount) as unit_price,
      subscription_unit,
      channel,
      special_offer,
      unit_price_currency,
      is_involuntary,
      is_reconnect
      from p1, unnest(json_query_array(content_subscription_subscription_items)) as content_subscription_subscription_item
      union all
      select
      customer_id,
      subscription_id,
      trial_start_date,
      active_start_date,
      active_end_date,
      json_value(content_subscription_subscription_item.item_price_id) as product_name,
      json_value(content_subscription_subscription_item.amount) as unit_price,
      subscription_unit,
      channel,
      special_offer,
      unit_price_currency,
      is_involuntary,
      is_reconnect
      from p2, unnest(json_query_array(content_subscription_subscription_items)) as content_subscription_subscription_item
      -- union all
      -- select
      -- customer_id,
      -- subscription_id,
      -- trial_start_date,
      -- active_start_date,
      -- active_end_date,
      -- json_value(content_subscription_subscription_item.item_price_id) as product_name,
      -- json_value(content_subscription_subscription_item.amount) as unit_price,
      -- channel,
      -- special_offer,
      -- unit_price_currency,
      -- is_involuntary
      -- from p4, unnest(json_query_array(content_subscription_subscription_items)) as content_subscription_subscription_item
      ;;
  }

  dimension: customer_id {
    type: string
    sql: ${TABLE}.customer_id ;;
  }

  dimension: subscription_id {
    type: string
    sql: ${TABLE}.subscription_id ;;
  }

  dimension: trial_start_date {
    type: date
    sql: ${TABLE}.trial_start_date ;;
  }

  dimension: active_start_date {
    type: date
    sql: ${TABLE}.active_start_date ;;
  }

  dimension: active_end_date {
    type: date
    sql: ${TABLE}.active_end_date ;;
  }

  dimension: product_name {
    type: string
    sql: ${TABLE}.product_name ;;
  }

  dimension: is_involuntary {
    type: yesno
    sql: ${TABLE}.is_involuntary ;;
  }

  dimension: channel {
    type: string
    sql: ${TABLE}.channel ;;
  }

  dimension: is_reconnect {
    type: yesno
    sql: ${TABLE}.is_reconnect ;;
  }

  dimension: subscription_unit {
    type: string
    sql: ${TABLE}.subscription_unit ;;
  }

  dimension: unit_price {
    type: number
    sql: ${TABLE}.unit_price ;;
  }

  dimension: special_offer {
    type: yesno
    sql: ${TABLE}.special_offer ;;
  }

  dimension: unit_price_currency {
    type: string
    sql: ${TABLE}.unit_price_currency ;;
  }
}

# }
