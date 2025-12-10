view: profile_update {

  derived_table: {
    sql:

    with a as (
      SELECT
        content_customer_email,
        content_card_customer_id,
        date(timestamp) as subscription_created_at
      FROM chargebee_webhook_events.subscription_created
      WHERE timestamp BETWEEN (CURRENT_DATE - INTERVAL '8 day')
                   AND CURRENT_DATE
      ),

    b as (
      SELECT
        content_customer_email,
        content_card_customer_id,
        date(timestamp) as profile_changed_at
      FROM chargebee_webhook_events.customer_changed
      WHERE timestamp BETWEEN (CURRENT_DATE - INTERVAL '8 day')
                   AND CURRENT_DATE
      ),

    c as (
      select
        b.content_card_customer_id as customer_id,
        b.content_customer_email as changed_email,
        a.content_customer_email as og_email
      from a INNER JOIN b on a.content_card_customer_id = b.content_card_customer_id)

    select distinct * from c WHERE og_email != changed_email ;;
  }

  dimension: customer_id {
    type: string
    sql: ${TABLE}.customer_id ;;
  }

  dimension: og_email {
    type: string
    sql: ${TABLE}.og_email ;;
  }

  dimension: changed_email {
    type: string
    sql: ${TABLE}.changed_email ;;
  }
}
