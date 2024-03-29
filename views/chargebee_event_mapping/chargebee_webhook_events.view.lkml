view: chargebee_webhook_events {
  derived_table: {
    sql: select
      timestamp
      , id
      , user_id
      , "customer.created" as event
      -- do we want to blend attribution data?
      , safe_cast(null as string) as campaign
      -- do we want to blend geolocation?
      , safe_cast(null as string) as city
      , safe_cast(null as string) as country
      , content_customer_created_at as created_at
      , content_customer_email as email
      , content_card_first_name as first_name
      , content_card_last_name as last_name
      -- Will the customer resource be created by the subscription resource?
      , safe_cast(null as date) as last_payment_date
      , content_customer_cs_marketing_opt_in as marketing_opt_in
      , CONCAT(content_card_first_name, ' ', content_card_last_name) as name
      , safe_cast(null as date) as next_payment_date
      , safe_cast(null as string) as plan
      , "web" as platform
      , safe_cast(null as string) as promotion_code
      , safe_cast(null as string) as referrer
      , safe_cast(null as string) as region
      , safe_cast(null as boolean) as registered_to_site
      , "chargebee" as source
      , safe_cast(null as boolean) as subscribed_to_site
      , safe_cast(null as string) as subscription_frequency
      , safe_cast(null as string) as subscription_price
      , safe_cast(null as string) as subscription_status
      , content_customer_updated_at as updated_at
      from `up-faith-and-family-216419.chargebee_webhook_events.customer_created`
      union all
      select
      timestamp
      , id
      , user_id
      , "customer.product.free_trial_created" as event
      , safe_cast(null as string) as campaign
      , safe_cast(null as string) as city
      , safe_cast(null as string) as country
      , content_customer_created_at as created_at
      , content_customer_email as email
      , content_card_first_name as first_name
      , content_card_last_name as last_name
      , safe_cast(null as date) as last_payment_date
      , content_customer_cs_marketing_opt_in as marketing_opt_in
      , CONCAT(content_card_first_name, ' ', content_card_last_name) as name
      , safe_cast(null as date) as next_payment_date
      , safe_cast(null as string) as plan
      , "web" as platform
      , safe_cast(null as string) as promotion_code
      , safe_cast(null as string) as referrer
      , safe_cast(null as string) as region
      , safe_cast(null as boolean) as registered_to_site
      , "chargebee" as source
      , safe_cast(null as boolean) as subscribed_to_site
      , case when JSON_EXTRACT_SCALAR(content_subscription_subscription_items, '$[0].item_price_id') = 'GaitherTV-USD-Monthly' then 'monthly'
        else 'yearly'
        end as subscription_frequency
      , safe_cast(null as string) as subscription_price
      , "free_trial" as subscription_status
      , content_customer_updated_at as updated_at
      from `up-faith-and-family-216419.chargebee_webhook_events.subscription_created`
      where content_subscription_status = "in_trial"
      union all
      select
      timestamp
      , id
      , user_id
      , "customer.product.created" as event
      , safe_cast(null as string) as campaign
      , safe_cast(null as string) as city
      , safe_cast(null as string) as country
      , content_customer_created_at as created_at
      , content_customer_email as email
      , content_card_first_name as first_name
      , content_card_last_name as last_name
      , safe_cast(null as date) as last_payment_date
      , content_customer_cs_marketing_opt_in as marketing_opt_in
      , CONCAT(content_card_first_name, ' ', content_card_last_name) as name
      , safe_cast(null as date) as next_payment_date
      , safe_cast(null as string) as plan
      , "web" as platform
      , safe_cast(null as string) as promotion_code
      , safe_cast(null as string) as referrer
      , safe_cast(null as string) as region
      , safe_cast(null as boolean) as registered_to_site
      , "chargebee" as source
      , safe_cast(null as boolean) as subscribed_to_site
      , case when JSON_EXTRACT_SCALAR(content_subscription_subscription_items, '$[0].item_price_id') = 'GaitherTV-USD-Monthly' then 'monthly'
        else 'yearly'
        end as subscription_frequency
      , safe_cast(null as string) as subscription_price
      , "enabled" as subscription_status
      , content_customer_updated_at as updated_at
      from `up-faith-and-family-216419.chargebee_webhook_events.subscription_created`
      where content_subscription_status = "active"
      union all
      select
      timestamp
      , id
      , user_id
      , "customer.product.free_trial_converted" as event
      , safe_cast(null as string) as campaign
      , safe_cast(null as string) as city
      , safe_cast(null as string) as country
      , content_customer_created_at as created_at
      , content_customer_email as email
      -- Event doesn't contain customer names for some reason
      , safe_cast(null as string) as first_name
      , safe_cast(null as string) as last_name
      , safe_cast(null as date) as last_payment_date
      , content_customer_cs_marketing_opt_in as marketing_opt_in
      , safe_cast(null as string) as name
      , safe_cast(null as date) as next_payment_date
      , safe_cast(null as string) as plan
      , "web" as platform
      , safe_cast(null as string) as promotion_code
      , safe_cast(null as string) as referrer
      , safe_cast(null as string) as region
      , safe_cast(null as boolean) as registered_to_site
      , "chargebee" as source
      , safe_cast(null as boolean) as subscribed_to_site
      , case when JSON_EXTRACT_SCALAR(content_subscription_subscription_items, '$[0].item_price_id') = 'GaitherTV-USD-Monthly' then 'monthly'
        else 'yearly'
        end as subscription_frequency
      , safe_cast(null as string) as subscription_price
      , "enabled" as subscription_status
      , content_customer_updated_at as updated_at
      from `up-faith-and-family-216419.chargebee_webhook_events.subscription_activated`
      union all
      select
      timestamp
      , id
      , user_id
      , "customer.product.renewed" as event
      , safe_cast(null as string) as campaign
      , safe_cast(null as string) as city
      , safe_cast(null as string) as country
      , content_customer_created_at as created_at
      , content_customer_email as email
      -- event doesn't contain user names
      , safe_cast(null as string) as first_name
      , safe_cast(null as string) as last_name
      , safe_cast(null as date) as last_payment_date
      , content_customer_cs_marketing_opt_in as marketing_opt_in
      , safe_cast(null as string) as name
      , safe_cast(null as date) as next_payment_date
      , safe_cast(null as string) as plan
      , "web" as platform
      , safe_cast(null as string) as promotion_code
      , safe_cast(null as string) as referrer
      , safe_cast(null as string) as region
      , safe_cast(null as boolean) as registered_to_site
      , "chargebee" as source
      , safe_cast(null as boolean) as subscribed_to_site
      , case when JSON_EXTRACT_SCALAR(content_subscription_subscription_items, '$[0].item_price_id') = 'GaitherTV-USD-Monthly' then 'monthly'
        else 'yearly'
        end as subscription_frequency
      , safe_cast(null as string) as subscription_price
      , "enabled" as subscription_status
      , content_customer_updated_at as updated_at
      from `up-faith-and-family-216419.chargebee_webhook_events.subscription_renewed`
      union all
      select
      timestamp
      , id
      , user_id
      , "customer.product.charge_failed" as event
      , safe_cast(null as string) as campaign
      , safe_cast(null as string) as city
      , safe_cast(null as string) as country
      , content_customer_created_at as created_at
      , content_customer_email as email
      , content_card_first_name as first_name
      , content_card_last_name as last_name
      , safe_cast(null as date) as last_payment_date
      -- event doesn't contain marketing opt in
      , safe_cast(null as boolean) as marketing_opt_in
      , CONCAT(content_card_first_name, ' ', content_card_last_name) as name
      , safe_cast(null as date) as next_payment_date
      , safe_cast(null as string) as plan
      , "web" as platform
      , safe_cast(null as string) as promotion_code
      , safe_cast(null as string) as referrer
      , safe_cast(null as string) as region
      , safe_cast(null as boolean) as registered_to_site
      , "chargebee" as source
      , safe_cast(null as boolean) as subscribed_to_site
      , case when JSON_EXTRACT_SCALAR(content_subscription_subscription_items, '$[0].item_price_id') = 'GaitherTV-USD-Monthly' then 'monthly'
        else 'yearly'
        end as subscription_frequency
      , safe_cast(null as string) as subscription_price
      , "enabled" as subscription_status
      , content_customer_updated_at as updated_at
      from `up-faith-and-family-216419.chargebee_webhook_events.payment_failed`
      union all
      select
      timestamp
      , id
      , user_id
      , "customer.product.set_cancellation" as event
      , safe_cast(null as string) as campaign
      , safe_cast(null as string) as city
      , safe_cast(null as string) as country
      , content_customer_created_at as created_at
      , content_customer_email as email
      -- event doesn't contain names
      , safe_cast(null as string) as first_name
      , safe_cast(null as string) as last_name
      , safe_cast(null as date) as last_payment_date
      , content_customer_cs_marketing_opt_in as marketing_opt_in
      , safe_cast(null as string) as name
      , safe_cast(null as date) as next_payment_date
      , safe_cast(null as string) as plan
      , "web" as platform
      , safe_cast(null as string) as promotion_code
      , safe_cast(null as string) as referrer
      , safe_cast(null as string) as region
      , safe_cast(null as boolean) as registered_to_site
      , "chargebee" as source
      , safe_cast(null as boolean) as subscribed_to_site
      , case when JSON_EXTRACT_SCALAR(content_subscription_subscription_items, '$[0].item_price_id') = 'GaitherTV-USD-Monthly' then 'monthly'
        else 'yearly'
        end as subscription_frequency
      , safe_cast(null as string) as subscription_price
      , "enabled" as subscription_status
      , content_customer_updated_at as updated_at
      from `up-faith-and-family-216419.chargebee_webhook_events.subscription_cancellation_scheduled`
      union all
      -- TODO: Create "customer_product_expired" event for failed charge related cancellations using the "cancel_reason" field.
      -- Enumeration of reasons ("Product Unsatisfactory", "Service Unsatisfactory", "Order Change", "Other", "Not Paid", "No Card", "Fraud Review Failed", "Non Compliant EU Customer", "Tax Calculation Failed", "Currency Incompatible With Gateway", "Non Compliant Customer")
      -- Note: "customer_product_disabled" triggers for Vimeo OTT API users. Therefore, it will trigger for chargebee users. No need to map over, but we should merge events instead.
      select
      timestamp
      , id
      , user_id
      , "customer.product.cancelled" as event
      , safe_cast(null as string) as campaign
      , safe_cast(null as string) as city
      , safe_cast(null as string) as country
      , content_customer_created_at as created_at
      , content_customer_email as email
      , content_card_first_name as first_name
      , content_card_last_name as last_name
      , safe_cast(null as date) as last_payment_date
      -- event doesn't contain marketing opt in
      , safe_cast(null as boolean) as marketing_opt_in
      , CONCAT(content_card_first_name, ' ', content_card_last_name) as name
      , safe_cast(null as date) as next_payment_date
      , safe_cast(null as string) as plan
      , "web" as platform
      , safe_cast(null as string) as promotion_code
      , safe_cast(null as string) as referrer
      , safe_cast(null as string) as region
      , safe_cast(null as boolean) as registered_to_site
      , "chargebee" as source
      , safe_cast(null as boolean) as subscribed_to_site
      , case when JSON_EXTRACT_SCALAR(content_subscription_subscription_items, '$[0].item_price_id') = 'GaitherTV-USD-Monthly' then 'monthly'
        else 'yearly'
        end as subscription_frequency
      , safe_cast(null as string) as subscription_price
      , "cancelled" as subscription_status
      , content_customer_updated_at as updated_at
      from `up-faith-and-family-216419.chargebee_webhook_events.subscription_cancelled`
      ;;
    datagroup_trigger: upff_daily_refresh_datagroup
  }
}
