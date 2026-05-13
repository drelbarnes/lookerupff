view: paypal_old {
  derived_table: {
    sql: -- Declare variables for start and end dates
      DECLARE start_date DEFAULT DATE '2025-02-24';
      DECLARE end_date DEFAULT DATE '2025-11-30';

      with paypal as (SELECT distinct
      '' as customer_id
      , To_Email_Address as email
      , date(_Date_) as charge_created
      , 'charge' as reporting_category
      , Reference_Txn_ID as source_id
      , Transaction_ID as transaction_id
      , Gross
      , 'paypal' as payment_gateway
      FROM `up-faith-and-family-216419.customers.paypal_payout_recon_11_2025_v3`   WHERE date(_Date_) start_date AND end_date),

      charges as (SELECT distinct
      content_transaction_customer_id as customer_id,
      content_customer_email as email,
      content_transaction_id_at_gateway as transaction_id,
      received_at,
      content_invoice_line_items_0_entity_id as product_1,
      content_invoice_line_items_1_entity_id as product_2,
      content_invoice_line_items_2_entity_id as product_3,
      content_invoice_line_items_0_tax_amount as tax_1,
      content_invoice_line_items_1_tax_amount as tax_2,
      content_invoice_line_items_2_tax_amount as tax_3,
      content_invoice_line_items_0_unit_amount as original_amount1,
      content_invoice_line_items_1_unit_amount as original_amount2,
      content_invoice_line_items_2_unit_amount as original_amount3,
      content_invoice_line_items_0_discount_amount  AS discount_amount1,
      content_invoice_line_items_1_discount_amount  AS discount_amount2,
      content_invoice_line_items_2_discount_amount  AS discount_amount3,
      content_invoice_amount_paid as total_amount,
      'charge' AS reporting_category
      `up-faith-and-family-216419.chargebee_webhook_events.payment_succeeded` WHERE date(received_at) between start_date AND end_date),

      refunds as (SELECT distinct
      content_transaction_customer_id as customer_id,
      content_customer_email as email,
      content_transaction_id_at_gateway as transaction_id,
      received_at,
      content_invoice_line_items_0_entity_id as product_1,
      content_invoice_line_items_1_entity_id as product_2,
      content_invoice_line_items_2_entity_id as product_3,
      content_invoice_line_items_0_tax_amount as tax_1,
      content_invoice_line_items_1_tax_amount as tax_2,
      content_invoice_line_items_2_tax_amount as tax_3,
      content_invoice_line_items_0_unit_amount as original_amount1,
      content_invoice_line_items_1_unit_amount as original_amount2,
      content_invoice_line_items_2_unit_amount as original_amount3,
      content_invoice_line_items_0_discount_amount  AS discount_amount1,
      content_invoice_line_items_1_discount_amount  AS discount_amount2,
      content_invoice_line_items_2_discount_amount  AS discount_amount3,
      content_invoice_amount_paid as total_amount,
      'refund' AS reporting_category
      FROM
      `up-faith-and-family-216419.chargebee_webhook_events.payment_refunded` WHERE date(received_at) between start_date AND end_date
      /*
      UNION ALL

      SELECT
      upper(SUBSTR(content_credit_note_billing_address_first_name, 1, 1)) as first_initial, -- First Name
      upper(SUBSTR(content_credit_note_billing_address_last_name, 1, 1)) as last_initial, -- Last Name
      content_credit_note_customer_id as content_transaction_customer_id,
      NULL as content_customer_email,
      content_transaction_id_at_gateway,
      received_at,
      content_credit_note_status,
      content_credit_note_line_items_0_entity_id AS entity_id,


      FROM
      `up-faith-and-family-216419.chargebee_webhook_events.credit_note_created` WHERE date(received_at) between start_date AND end_date
      */
      ),


      chargebee_transactions as (
      SELECT * FROM charges
      UNION ALL
      SELECT * FROM refunds

      ),

      result as (
      select distinct
      coalesce(c.customer_id, p.customer_id) as id
      , c.email
      , p.charge_created as timestamp_charge_created
      , date(p.created) as date
      , p.payment_gateway
      , c.reporting_category as chargebee_type
      , c.product_1
      ,c.product_2
      ,c.product_3
      ,c.original_amount1
      ,c.original_amount2
      ,c.original_amount3
      ,c.discount_amount1
      ,c.discount_amount2
      ,c.discount_amount3
      ,c.tax_1
      ,c.tax_2
      ,c.tax_3
      ,c.total_amount
      , p.source_id as stripe_ref_id
      , p.gross as stripe_remitted
      from paypal p
      right join chargebee_transactions c on p.transaction_id = c.transaction_id )



      select distinct * from result ;;
  }
}
