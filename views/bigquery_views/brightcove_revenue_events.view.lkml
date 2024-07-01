view: brightcove_revenue_events {
#   # Or, you could make this view a derived table, like this:
  derived_table: {
    sql:
      with p0 as (
        SELECT
        content_subscription_id AS subscription_id,
        DATE(TIMESTAMP_SECONDS(content_invoice_date)) AS date,
        DATE(TIMESTAMP_SECONDS(content_subscription_current_term_start)) AS term_start_date,
        DATE(TIMESTAMP_SECONDS(content_subscription_current_term_end)) AS term_end_date,
        SAFE.PARSE_JSON(content_subscription_subscription_items) as content_subscription_subscription_items,
        content_subscription_currency_code as currency
        FROM `up-faith-and-family-216419.chargebee_webhook_events.subscription_renewed`
      )
      , p1 as (
        SELECT
        content_subscription_id AS subscription_id,
        DATE(TIMESTAMP_SECONDS(content_invoice_date)) AS date,
        DATE(TIMESTAMP_SECONDS(content_subscription_current_term_start)) AS term_start_date,
        DATE(TIMESTAMP_SECONDS(content_subscription_current_term_end)) AS term_end_date,
        SAFE.PARSE_JSON(content_subscription_subscription_items) as content_subscription_subscription_items,
        content_subscription_currency_code as currency
        FROM `up-faith-and-family-216419.chargebee_webhook_events.subscription_activated`
      )
      , unionize_events as (
        select
        subscription_id,
        date,
        term_start_date,
        term_end_date,
        json_value(content_subscription_subscription_item.item_price_id) as product_id,
        json_value(content_subscription_subscription_item.amount) as revenue_amount,
        currency
        from p0, unnest(json_query_array(content_subscription_subscription_items)) as content_subscription_subscription_item
        union all
        select
        subscription_id,
        date,
        term_start_date,
        term_end_date,
        json_value(content_subscription_subscription_item.item_price_id) as product_id,
        json_value(content_subscription_subscription_item.amount) as revenue,
        currency
        from p1, unnest(json_query_array(content_subscription_subscription_items)) as content_subscription_subscription_item
      )
      select * from unionize_events where date is not null
      ;;
  }

  dimension: subscription_id {
    type: string
    sql: ${TABLE}.subscription_id ;;
    description: "Unique identifier of the customer-held subscription."
  }

  dimension: date {
    type: date
    sql: ${TABLE}.date ;;
    description: "Date of the transaction."
  }

  dimension: term_start_date {
    type: date
    sql: ${TABLE}.term_start_date ;;
    description: "Start date of the revenue period, inclusive, starting at midnight of this date."
  }

  dimension: term_end_date {
    type: date
    sql: ${TABLE}.term_end_date ;;
    description: "End date of the revenue period, exclusive, up to midnight of this date."
  }

  dimension: product_id {
    type: string
    sql: ${TABLE}.product_id ;;
    description: "The ID of the product that was sold."
  }

  dimension: revenue_amount {
    type: number
    sql: ${TABLE}.revenue_amount ;;
    description: "The amount of currency in the transaction."
  }

  dimension: currency {
    type: string
    sql: ${TABLE}.currency ;;
    description: "String holding an ISO 4217:2015 currency code."
  }

}
