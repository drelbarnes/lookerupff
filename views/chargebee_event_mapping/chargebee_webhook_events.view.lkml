view: chargebee_webhook_events {
  derived_table: {
    sql: select
      timestamp
      , user_id
      , "customer_created" as event
      , safe_cast(null as string) as campaign
      , safe_cast(null as string) as city
      , safe_cast(null as string) as country
      , content_customer_created_at as created_at
      , content_customer_email as email
      , content_customer_first_name as first_name
      , content_customer_last_name as last_name
      , safe_cast(null as date) as last_payment_date
      , content_customer_cs_marketing_opt_in as marketing_opt_in
      , CONCAT(content_customer_first_name, ' ', content_customer_last_name) as name
      , safe_cast(null as date) as next_payment_date
      , safe_cast(null as string) as plan
      , "chargebee" as platform
      , safe_cast(null as string) as promotion_code
      , safe_cast(null as string) as referrer
      , safe_cast(null as string) as region
      , safe_cast(null as boolean) as registered_to_site
      , source
      , safe_cast(null as boolean) as subscribed_to_site
      , safe_cast(null as string) as subscription_frequency
      , safe_cast(null as string) as subscription_price
      , safe_cast(null as string) as subscription_status
      , content_customer_updated_at as updated_at
      from `up-faith-and-family-216419.chargebee_dev_2_s_dpdcmh_vqn_60_tr_ql6_fz_gu_ci_7vi.customer_created`
      ;;
  }
}
