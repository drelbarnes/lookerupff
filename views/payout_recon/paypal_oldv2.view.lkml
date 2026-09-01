view: paypal_oldv2 {derived_table: {
    sql: -- Declare variables for start and end dates


      with cfg AS (  -- renamed from "cfg"
      SELECT report_date
      FROM ${config.SQL_TABLE_NAME}),

      union_old as (
      SELECT *
      FROM `up-faith-and-family-216419.customers.paypal_payout_recon_june_2026`
      UNION ALL
      SELECT *
      FROM `up-faith-and-family-216419.customers.paypal_payout_recon_5_2026`
      UNION ALL
      SELECT *
      FROM `up-faith-and-family-216419.customers.paypal_payout_recon_4_2026`
      UNION ALL
      SELECT *
      FROM `up-faith-and-family-216419.customers.paypal_payout_recon_3_2026`
      UNION ALL
      SELECT *
      FROM `up-faith-and-family-216419.customers.paypal_recon_payout_feb_2026`
      UNION ALL
      SELECT *
      FROM `up-faith-and-family-216419.customers.paypal_payout_recon_2_2026`
      ),

      paypal as (
      SELECT distinct
      To_Email_Address as email
      , date(_Date_) as charge_created
      , 'charge' as reporting_category
      , Reference_Txn_ID as source_id
      , Transaction_ID as transaction_id
      , Gross
      , fee
      , 'paypal' as payment_gateway
      , type as payment_description
      FROM union_old
      ),

      paypal_chargebee as (
      SELECT * FROM paypal
      WHERE payment_description in ('Payment Refund','PreApproved Payment Bill User Payment'))

      select * from paypal_chargebee



      ;;
  }}
