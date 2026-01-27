view: paused {
  derived_table: {
    sql:
    with one_paused as (
      SELECT
        distinct content_customer_id
      FROM chargebee_webhook_events.subscription_paused
      WHERE date(timestamp)>='2025-07-01'),

    bundles_consolidated as (
    select
      distinct content_customer_id
    FROM chargebee_webhook_events.payment_succeeded
    WHERE content_customer_id in (select * from one_paused)
    and date(timestamp) > '2025-10-01'
    and content_invoice_line_items_0_entity_id is not NULL
    and content_invoice_line_items_1_entity_id is not NULL
    ),

    bundles as (
      select
        count(*) as count,
        content_customer_id,
        date(timestamp) as report_date
      FROM chargebee_webhook_events.payment_succeeded
      WHERE content_customer_id in (select * from one_paused)
      GROUP by 2,3
),
    bundles2 as (
      select
        distinct content_customer_id
      FROM bundles
      WHERE count >1
    )

    no_bundles as (
      select
        content_customer_id
      from one_paused
      where content_customer_id not in (select * from bundles2)
      and content)customer_iud not in (select * from bundles_consolidated
        )

select count(*) from no_bundles ;;
  }
  }
