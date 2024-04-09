view: gaithertvplus_analytics {
  derived_table: {
    sql: with get_analytics_p0 as (
        with p0 as (
          select
          *
          , 2 as report_version
          from php_gaithertv.get_analytics
          where date(sent_at)=current_date
        )
        select
        analytics_timestamp as timestamp
        , NULL::INTEGER as existing_free_trials
        , NULL::INTEGER as existing_paying
        , free_trial_churn
        , free_trial_converted
        , free_trial_created
        -- , NULL::INTEGER as paused_created
        , paying_churn
        , paying_created
        , total_free_trials
        , total_paying
        , report_version
        , row_number() over (partition by analytics_timestamp, report_version order by sent_at desc) as n
        from p0
      )
      , events as (
        /*                            */
        /*      CUSTOMER CREATED      */
        /*                            */
        select
        "timestamp"::TIMESTAMP
        , id::VARCHAR
        , content_customer_id::VARCHAR as customer_id
        , null::VARCHAR as subscription_id
        , 'customer_created' as event
        -- do we want to blend attribution data?
        , null::VARCHAR as campaign
        -- do we want to blend geolocation?
        , null::VARCHAR as city
        , content_customer_billing_address_country::VARCHAR as country
        , null::VARCHAR as coupon_code
        , (TIMESTAMP 'epoch' + content_customer_created_at * INTERVAL '1 second') AS created_at
        , content_customer_email::VARCHAR as email
        , content_card_first_name::VARCHAR as first_name
        , content_card_last_name::VARCHAR as last_name
        -- Will the customer resource be created by the subscription resource?
        , null::TIMESTAMP as last_payment_date
        , content_customer_cs_marketing_opt_in::BOOLEAN as marketing_opt_in
        , content_card_first_name::VARCHAR || ' ' || content_card_last_name::VARCHAR AS name
        , null::TIMESTAMP as next_payment_date
        , null::VARCHAR as plan
        , 'web' as platform
        , null::VARCHAR as promotion_code
        , null::VARCHAR as referrer
        , content_customer_billing_address_state_code::VARCHAR as region
        , null::BOOLEAN as registered_to_site
        , 'chargebee' as source
        , null::BOOLEAN as subscribed_to_site
        , null::VARCHAR as subscription_frequency
        , null::INT as subscription_price
        , null::VARCHAR as subscription_status
        , (TIMESTAMP 'epoch' + content_customer_updated_at * INTERVAL '1 second') AS updated_at
        from chargebee_webhook_events.customer_created
        union all
        /*                            */
        /*      CUSTOMER DELETED      */
        /*                            */
        select
        "timestamp"::TIMESTAMP
        , id::VARCHAR
        , content_customer_id::VARCHAR as customer_id
        , null::VARCHAR as subscription_id
        , 'customer_deleted' as event
        -- do we want to blend attribution data?
        , null::VARCHAR as campaign
        -- do we want to blend geolocation?
        , null::VARCHAR as city
        , content_customer_billing_address_country::VARCHAR as country
        , null::VARCHAR as coupon_code
        , (TIMESTAMP 'epoch' + content_customer_created_at * INTERVAL '1 second') AS created_at
        , content_customer_email::VARCHAR as email
        , content_card_first_name::VARCHAR as first_name
        , content_card_last_name::VARCHAR as last_name
        -- Will the customer resource be created by the subscription resource?
        , null::TIMESTAMP as last_payment_date
        , content_customer_cs_marketing_opt_in::BOOLEAN as marketing_opt_in
        , content_card_first_name::VARCHAR || ' ' || content_card_last_name::VARCHAR AS name
        , null::TIMESTAMP as next_payment_date
        , null::VARCHAR as plan
        , 'web' as platform
        , null::VARCHAR as promotion_code
        , null::VARCHAR as referrer
        -- , content_customer_billing_address_state_code::VARCHAR as region
        , null::VARCHAR as region
        , null::BOOLEAN as registered_to_site
        , 'chargebee' as source
        , null::BOOLEAN as subscribed_to_site
        , null::VARCHAR as subscription_frequency
        , null::INT as subscription_price
        , null::VARCHAR as subscription_status
        , (TIMESTAMP 'epoch' + content_customer_updated_at * INTERVAL '1 second') AS updated_at
        from chargebee_webhook_events.customer_deleted
        union all
        /*                            */
        /*      CUSTOMER UPDATED      */
        /*                            */
        select
        "timestamp"::TIMESTAMP
        , id::VARCHAR
        , content_customer_id::VARCHAR as customer_id
        , null::VARCHAR as subscription_id
        , 'customer_updated' as event
        -- do we want to blend attribution data?
        , null::VARCHAR as campaign
        -- do we want to blend geolocation?
        , null::VARCHAR as city
        , content_customer_billing_address_country::VARCHAR as country
        , null::VARCHAR as coupon_code
        , (TIMESTAMP 'epoch' + content_customer_created_at * INTERVAL '1 second') AS created_at
        , content_customer_email::VARCHAR as email
        , content_card_first_name::VARCHAR as first_name
        , content_card_last_name::VARCHAR as last_name
        -- Will the customer resource be created by the subscription resource?
        , null::TIMESTAMP as last_payment_date
        , content_customer_cs_marketing_opt_in::BOOLEAN as marketing_opt_in
        , content_card_first_name::VARCHAR || ' ' || content_card_last_name::VARCHAR AS name
        , null::TIMESTAMP as next_payment_date
        , null::VARCHAR as plan
        , 'web' as platform
        , null::VARCHAR as promotion_code
        , null::VARCHAR as referrer
        , content_customer_billing_address_state_code::VARCHAR as region
        , null::BOOLEAN as registered_to_site
        , 'chargebee' as source
        , null::BOOLEAN as subscribed_to_site
        , null::VARCHAR as subscription_frequency
        , null::INT as subscription_price
        , null::VARCHAR as subscription_status
        , (TIMESTAMP 'epoch' + content_customer_updated_at * INTERVAL '1 second') AS updated_at
        from chargebee_webhook_events.customer_changed
        union all
        /*                                        */
        /*        CUSTOMER PRODUCT CREATED        */
        /*  CUSTOMER PRODUCT FREE TRIAL CREATED   */
        /*                                        */
        select
        "timestamp"::TIMESTAMP
        , id::VARCHAR
        , content_customer_id::VARCHAR as customer_id
        , content_subscription_id::VARCHAR as subscription_id
        , CASE
            WHEN event  = 'subscription_created' AND content_subscription_status = 'in_trial' THEN 'customer_product_free_trial_created'
            ELSE 'customer_product_created'
          END AS event
        , null::VARCHAR as campaign
        , null::VARCHAR as city
        , content_customer_billing_address_country::VARCHAR as country
        -- , content_coupons_0_coupon_code as coupon_code
        , null::VARCHAR as coupon_code
        , (TIMESTAMP 'epoch' + content_customer_created_at * INTERVAL '1 second') AS created_at
        , content_customer_email::VARCHAR as email
        , content_card_first_name::VARCHAR as first_name
        , content_card_last_name::VARCHAR as last_name
        , null::TIMESTAMP as last_payment_date
        , content_customer_cs_marketing_opt_in::BOOLEAN as marketing_opt_in
        , content_card_first_name::VARCHAR || ' ' || content_card_last_name::VARCHAR AS name
        , (TIMESTAMP 'epoch' + content_subscription_next_billing_at * INTERVAL '1 second') AS next_payment_date
        , coalesce(
          content_subscription_subscription_items_0_item_price_id::VARCHAR, json_extract_path_text(
            json_extract_array_element_text(
              content_subscription_subscription_items, 0
            ),'item_price_id'
          )::VARCHAR
        ) as plan
        , content_subscription_channel::VARCHAR as platform
        -- , content_coupons_0_coupon_id as promotion_code
        , null::VARCHAR as promotion_code
        , null::VARCHAR as referrer
        , content_customer_billing_address_state_code::VARCHAR as region
        , null::BOOLEAN as registered_to_site
        , 'chargebee' as source
        , null::BOOLEAN as subscribed_to_site
        , CASE content_subscription_billing_period_unit
              WHEN 'month' THEN 'monthly'
              WHEN 'year' THEN 'yearly'
              ELSE content_subscription_billing_period_unit::VARCHAR
          END AS subscription_frequency
        , coalesce(
          content_subscription_subscription_items_0_unit_price::INT, json_extract_path_text(
            json_extract_array_element_text(
              content_subscription_subscription_items, 0
            ),'unit_price'
          )::INT
        ) as subscription_price
        , CASE content_subscription_status
              WHEN 'active' THEN 'enabled'
              WHEN 'in_trial' THEN 'free_trial'
              WHEN 'cancelled' THEN 'cancelled'
              WHEN 'non_renewing' THEN 'non_renewing'
              WHEN 'disabled' THEN 'disabled'
              WHEN 'paused' THEN 'paused'
              ELSE 'unknown' -- Default case if no match is found
            END AS subscription_status
        , (TIMESTAMP 'epoch' + content_customer_updated_at * INTERVAL '1 second') AS updated_at
        from chargebee_webhook_events.subscription_created
        union all
        /*                                        */
        /*  CUSTOMER PRODUCT FREE TRIAL CONVERTED */
        /*                                        */
          select
        "timestamp"::TIMESTAMP
        , id::VARCHAR
        , content_customer_id::VARCHAR as customer_id
        , content_subscription_id::VARCHAR as subscription_id
        , 'customer_product_free_trial_converted' as event
        , null::VARCHAR as campaign
        , null::VARCHAR as city
        , content_customer_billing_address_country::VARCHAR as country
        -- , content_coupons_0_coupon_code as coupon_code
        , null::VARCHAR as coupon_code
        , (TIMESTAMP 'epoch' + content_customer_created_at * INTERVAL '1 second') AS created_at
        , content_customer_email::VARCHAR as email
        , content_card_first_name::VARCHAR as first_name
        , content_card_last_name::VARCHAR as last_name
        , (TIMESTAMP 'epoch' + content_invoice_paid_at * INTERVAL '1 second') AS last_payment_date
        -- , null::TIMESTAMP as last_payment_date
        , content_customer_cs_marketing_opt_in::BOOLEAN as marketing_opt_in
        , content_card_first_name::VARCHAR || ' ' || content_card_last_name::VARCHAR AS name
        , (TIMESTAMP 'epoch' + content_subscription_next_billing_at * INTERVAL '1 second') AS next_payment_date
        , coalesce(
          content_subscription_subscription_items_0_item_price_id::VARCHAR, json_extract_path_text(
            json_extract_array_element_text(
              content_subscription_subscription_items, 0
            ),'item_price_id'
          )::VARCHAR
        ) as plan
        , content_subscription_channel::VARCHAR as platform
        -- , content_coupons_0_coupon_id as promotion_code
        , null::VARCHAR as promotion_code
        , null::VARCHAR as referrer
        , content_customer_billing_address_state_code::VARCHAR as region
        , null::BOOLEAN as registered_to_site
        , 'chargebee' as source
        , null::BOOLEAN as subscribed_to_site
        , CASE content_subscription_billing_period_unit
              WHEN 'month' THEN 'monthly'
              WHEN 'year' THEN 'yearly'
              ELSE content_subscription_billing_period_unit::VARCHAR
          END AS subscription_frequency
        , coalesce(
          content_subscription_subscription_items_0_unit_price::INT, json_extract_path_text(
            json_extract_array_element_text(
              content_subscription_subscription_items, 0
            ),'unit_price'
          )::INT
        ) as subscription_price
        , CASE content_subscription_status
              WHEN 'active' THEN 'enabled'
              WHEN 'in_trial' THEN 'free_trial'
              WHEN 'cancelled' THEN 'cancelled'
              WHEN 'non_renewing' THEN 'non_renewing'
              WHEN 'disabled' THEN 'disabled'
              WHEN 'paused' THEN 'paused'
              ELSE 'unknown' -- Default case if no match is found
            END AS subscription_status
        , (TIMESTAMP 'epoch' + content_customer_updated_at * INTERVAL '1 second') AS updated_at
        from chargebee_webhook_events.subscription_activated
        where content_subscription_due_invoices_count = 0
        union all
        /*                                        */
        /*  CUSTOMER PRODUCT FREE TRIAL CONVERTED */
        /*                DUNNING                 */
        /*                                        */
          select
        a."timestamp"::TIMESTAMP
        , a.id::VARCHAR
        , a.content_customer_id::VARCHAR as customer_id
        , a.content_subscription_id::VARCHAR as subscription_id
        , 'customer_product_free_trial_converted' as event
        , null::VARCHAR as campaign
        , null::VARCHAR as city
        , a.content_customer_billing_address_country::VARCHAR as country
        -- , content_coupons_0_coupon_code as coupon_code
        , null::VARCHAR as coupon_code
        , (TIMESTAMP 'epoch' + a.content_customer_created_at * INTERVAL '1 second') AS created_at
        , a.content_customer_email::VARCHAR as email
        , a.content_card_first_name::VARCHAR as first_name
        , a.content_card_last_name::VARCHAR as last_name
        , (TIMESTAMP 'epoch' + a.content_invoice_paid_at * INTERVAL '1 second') AS last_payment_date
        -- , null::TIMESTAMP as last_payment_date
        , a.content_customer_cs_marketing_opt_in::BOOLEAN as marketing_opt_in
        , a.content_card_first_name::VARCHAR || ' ' || a.content_card_last_name::VARCHAR AS name
        , (TIMESTAMP 'epoch' + a.content_subscription_next_billing_at * INTERVAL '1 second') AS next_payment_date
        , coalesce(
          a.content_subscription_subscription_items_0_item_price_id::VARCHAR, json_extract_path_text(
            json_extract_array_element_text(
              a.content_subscription_subscription_items, 0
            ),'item_price_id'
          )::VARCHAR
        ) as plan
        , a.content_subscription_channel::VARCHAR as platform
        -- , content_coupons_0_coupon_id as promotion_code
        , null::VARCHAR as promotion_code
        , null::VARCHAR as referrer
        , a.content_customer_billing_address_state_code::VARCHAR as region
        , null::BOOLEAN as registered_to_site
        , 'chargebee' as source
        , null::BOOLEAN as subscribed_to_site
        , CASE a.content_subscription_billing_period_unit
              WHEN 'month' THEN 'monthly'
              WHEN 'year' THEN 'yearly'
              ELSE a.content_subscription_billing_period_unit::VARCHAR
          END AS subscription_frequency
        , coalesce(
          a.content_subscription_subscription_items_0_unit_price::INT, json_extract_path_text(
            json_extract_array_element_text(
              a.content_subscription_subscription_items, 0
            ),'unit_price'
          )::INT
        ) as subscription_price
        , CASE a.content_subscription_status
              WHEN 'active' THEN 'enabled'
              WHEN 'in_trial' THEN 'free_trial'
              WHEN 'cancelled' THEN 'cancelled'
              WHEN 'non_renewing' THEN 'non_renewing'
              WHEN 'disabled' THEN 'disabled'
              WHEN 'paused' THEN 'paused'
              ELSE 'unknown' -- Default case if no match is found
          END AS subscription_status
        , (TIMESTAMP 'epoch' + a.content_customer_updated_at * INTERVAL '1 second') AS updated_at
        from chargebee_webhook_events.subscription_activated a
        inner join chargebee_webhook_events.payment_succeeded b
        on a.content_invoice_id = b. content_invoice_id
        where a.content_subscription_due_invoices_count >= 1
        union all
        /*                                        */
        /*        CUSTOMER PRODUCT CREATED        */
        /*              REACQUISITION             */
        /*                                        */
        select
        "timestamp"::TIMESTAMP
        , id::VARCHAR
        , content_customer_id::VARCHAR as customer_id
        , content_subscription_id::VARCHAR as subscription_id
        , 'customer_product_created' as event
        , null::VARCHAR as campaign
        , null::VARCHAR as city
        , content_customer_billing_address_country::VARCHAR as country
        -- , content_coupons_0_coupon_code as coupon_code
        , null::VARCHAR as coupon_code
        , (TIMESTAMP 'epoch' + content_customer_created_at * INTERVAL '1 second') AS created_at
        , content_customer_email::VARCHAR as email
        , content_card_first_name::VARCHAR as first_name
        , content_card_last_name::VARCHAR as last_name
        , (TIMESTAMP 'epoch' + content_invoice_paid_at * INTERVAL '1 second') AS last_payment_date
        , content_customer_cs_marketing_opt_in::BOOLEAN as marketing_opt_in
        , content_card_first_name::VARCHAR || ' ' || content_card_last_name::VARCHAR AS name
        , (TIMESTAMP 'epoch' + content_subscription_next_billing_at * INTERVAL '1 second') AS next_payment_date
        , coalesce(
          content_subscription_subscription_items_0_item_price_id::VARCHAR, json_extract_path_text(
            json_extract_array_element_text(
              content_subscription_subscription_items, 0
            ),'item_price_id'
          )::VARCHAR
        ) as plan
        , content_subscription_channel::VARCHAR as platform
        -- , content_coupons_0_coupon_id as promotion_code
        , null::VARCHAR as promotion_code
        , null::VARCHAR as referrer
        , content_customer_billing_address_state_code::VARCHAR as region
        , null::BOOLEAN as registered_to_site
        , 'chargebee' as source
        , null::BOOLEAN as subscribed_to_site
        , CASE content_subscription_billing_period_unit
              WHEN 'month' THEN 'monthly'
              WHEN 'year' THEN 'yearly'
              ELSE content_subscription_billing_period_unit::VARCHAR
          END AS subscription_frequency
        , coalesce(
          content_subscription_subscription_items_0_unit_price::INT, json_extract_path_text(
            json_extract_array_element_text(
              content_subscription_subscription_items, 0
            ),'unit_price'
          )::INT
        ) as subscription_price
        , CASE content_subscription_status
              WHEN 'active' THEN 'enabled'
              WHEN 'in_trial' THEN 'free_trial'
              WHEN 'cancelled' THEN 'cancelled'
              WHEN 'non_renewing' THEN 'non_renewing'
              WHEN 'disabled' THEN 'disabled'
              WHEN 'paused' THEN 'paused'
              ELSE 'unknown' -- Default case if no match is found
            END AS subscription_status
        , (TIMESTAMP 'epoch' + content_customer_updated_at * INTERVAL '1 second') AS updated_at
        from chargebee_webhook_events.subscription_reactivated
        /*                                        */
        /*        CUSTOMER PRODUCT RENEWED        */
        /*                                        */
        union all
        select
        "timestamp"::TIMESTAMP
        , id::VARCHAR
        , content_customer_id::VARCHAR as customer_id
        , content_subscription_id::VARCHAR as subscription_id
        , 'customer_product_renewed' as event
        , null::VARCHAR as campaign
        , null::VARCHAR as city
        , content_customer_billing_address_country::VARCHAR as country
        -- , content_coupons_0_coupon_code as coupon_code
        , null::VARCHAR as coupon_code
        , (TIMESTAMP 'epoch' + content_customer_created_at * INTERVAL '1 second') AS created_at
        , content_customer_email::VARCHAR as email
        , content_card_first_name::VARCHAR as first_name
        , content_card_last_name::VARCHAR as last_name
        , (TIMESTAMP 'epoch' + content_invoice_paid_at * INTERVAL '1 second') AS last_payment_date
        , content_customer_cs_marketing_opt_in::BOOLEAN as marketing_opt_in
        , content_card_first_name::VARCHAR || ' ' || content_card_last_name::VARCHAR AS name
        , (TIMESTAMP 'epoch' + content_subscription_next_billing_at * INTERVAL '1 second') AS next_payment_date
        , coalesce(
          content_subscription_subscription_items_0_item_price_id::VARCHAR, json_extract_path_text(
            json_extract_array_element_text(
              content_subscription_subscription_items, 0
            ),'item_price_id'
          )::VARCHAR
        ) as plan
        , content_subscription_channel::VARCHAR as platform
        -- , content_coupons_0_coupon_id::VARCHAR as promotion_code
        , null::VARCHAR as promotion_code
        , null::VARCHAR as referrer
        , content_customer_billing_address_state_code::VARCHAR as region
        , null::BOOLEAN as registered_to_site
        , 'chargebee' as source
        , null::BOOLEAN as subscribed_to_site
        , CASE content_subscription_billing_period_unit
              WHEN 'month' THEN 'monthly'
              WHEN 'year' THEN 'yearly'
              ELSE content_subscription_billing_period_unit::VARCHAR
          END AS subscription_frequency
        , coalesce(
          content_subscription_subscription_items_0_unit_price::INT, json_extract_path_text(
            json_extract_array_element_text(
              content_subscription_subscription_items, 0
            ),'unit_price'
          )::INT
        ) as subscription_price
        , CASE content_subscription_status
              WHEN 'active' THEN 'enabled'
              WHEN 'in_trial' THEN 'free_trial'
              WHEN 'cancelled' THEN 'cancelled'
              WHEN 'non_renewing' THEN 'non_renewing'
              WHEN 'disabled' THEN 'disabled'
              WHEN 'paused' THEN 'paused'
              ELSE 'unknown' -- Default case if no match is found
            END AS subscription_status
        , (TIMESTAMP 'epoch' + content_customer_updated_at * INTERVAL '1 second') AS updated_at
        from chargebee_webhook_events.subscription_renewed
        union all
        /*                                        */
        /*      CUSTOMER PRODUCT CANCELLED        */
        /*  CUSTOMER PRODUCT FREE TRIAL EXPIRED   */
        /*                                        */
        select
        "timestamp"::TIMESTAMP
        , id::VARCHAR
        , content_customer_id::VARCHAR as customer_id
        , content_subscription_id::VARCHAR as subscription_id
        , CASE
            WHEN (content_subscription_cancelled_at - content_subscription_trial_end) < 10000 THEN 'customer_product_free_trial_expired'
            ELSE 'customer_product_cancelled'
          END AS event
        , null::VARCHAR as campaign
        , null::VARCHAR as city
        , content_customer_billing_address_country::VARCHAR as country
        -- , content_coupons_0_coupon_code as coupon_code
        , null::VARCHAR as coupon_code
        , (TIMESTAMP 'epoch' + content_customer_created_at * INTERVAL '1 second') AS created_at
        , content_customer_email::VARCHAR as email
        , content_card_first_name::VARCHAR as first_name
        , content_card_last_name::VARCHAR as last_name
        , (TIMESTAMP 'epoch' + content_subscription_current_term_start * INTERVAL '1 second') AS last_payment_date
        -- , content_subscription_current_term_start:: as last_payment_date
        , content_customer_cs_marketing_opt_in::BOOLEAN as marketing_opt_in
        , content_card_first_name::VARCHAR || ' ' || content_card_last_name::VARCHAR AS name
        , null::TIMESTAMP as next_payment_date
        -- , (TIMESTAMP 'epoch' + content_subscription_next_billing_at * INTERVAL '1 second') AS next_payment_date
        , coalesce(
          content_subscription_subscription_items_0_item_price_id::VARCHAR, json_extract_path_text(
            json_extract_array_element_text(
              content_subscription_subscription_items, 0
            ),'item_price_id'
          )::VARCHAR
        ) as plan
        , content_subscription_channel::VARCHAR as platform
        -- , content_coupons_0_coupon_id as promotion_code
        , null::VARCHAR as promotion_code
        , null::VARCHAR as referrer
        , content_customer_billing_address_state_code::VARCHAR as region
        , null::BOOLEAN as registered_to_site
        , 'chargebee' as source
        , null::BOOLEAN as subscribed_to_site
        , CASE content_subscription_billing_period_unit
              WHEN 'month' THEN 'monthly'
              WHEN 'year' THEN 'yearly'
              ELSE content_subscription_billing_period_unit::VARCHAR
          END AS subscription_frequency
        , coalesce(
          content_subscription_subscription_items_0_unit_price::INT, json_extract_path_text(
            json_extract_array_element_text(
              content_subscription_subscription_items, 0
            ),'unit_price'
          )::INT
        ) as subscription_price
        , CASE content_subscription_status
              WHEN 'active' THEN 'enabled'
              WHEN 'in_trial' THEN 'free_trial'
              WHEN 'cancelled' THEN 'cancelled'
              WHEN 'non_renewing' THEN 'non_renewing'
              WHEN 'disabled' THEN 'disabled'
              WHEN 'paused' THEN 'paused'
              ELSE 'unknown' -- Default case if no match is found
            END AS subscription_status
        , (TIMESTAMP 'epoch' + content_customer_updated_at * INTERVAL '1 second') AS updated_at
        from chargebee_webhook_events.subscription_cancelled
        where content_subscription_cancel_reason_code not in ('Not Paid', 'No Card', 'Fraud Review Failed', 'Non Compliant EU Customer', 'Tax Calculation Failed', 'Currency incompatible with Gateway', 'Non Compliant Customer')
        union all
        /*                                        */
        /*      CUSTOMER PRODUCT CANCELLED        */
        /*  CUSTOMER PRODUCT FREE TRIAL EXPIRED   */
        /*                DUNNING                 */
        /*                                        */
        select
        "timestamp"::TIMESTAMP
        , id::VARCHAR
        , content_customer_id::VARCHAR as customer_id
        , content_subscription_id::VARCHAR as subscription_id
        , CASE -- if cancelled less that 28 days since activating, free_trial expired.
            WHEN (content_subscription_cancelled_at - content_subscription_activated_at) < 2419200 THEN 'customer_product_free_trial_expired'
            ELSE 'customer_product_cancelled'
          END AS event
        , null::VARCHAR as campaign
        , null::VARCHAR as city
        , content_customer_billing_address_country::VARCHAR as country
        -- , content_coupons_0_coupon_code as coupon_code
        , null::VARCHAR as coupon_code
        , (TIMESTAMP 'epoch' + content_customer_created_at * INTERVAL '1 second') AS created_at
        , content_customer_email::VARCHAR as email
        , content_card_first_name::VARCHAR as first_name
        , content_card_last_name::VARCHAR as last_name
        , (TIMESTAMP 'epoch' + content_subscription_current_term_start * INTERVAL '1 second') AS last_payment_date
        -- , content_subscription_current_term_start:: as last_payment_date
        , content_customer_cs_marketing_opt_in::BOOLEAN as marketing_opt_in
        , content_card_first_name::VARCHAR || ' ' || content_card_last_name::VARCHAR AS name
        , null::TIMESTAMP as next_payment_date
        -- , (TIMESTAMP 'epoch' + content_subscription_next_billing_at * INTERVAL '1 second') AS next_payment_date
        , coalesce(
          content_subscription_subscription_items_0_item_price_id::VARCHAR, json_extract_path_text(
            json_extract_array_element_text(
              content_subscription_subscription_items, 0
            ),'item_price_id'
          )::VARCHAR
        ) as plan
        , content_subscription_channel::VARCHAR as platform
        -- , content_coupons_0_coupon_id as promotion_code
        , null::VARCHAR as promotion_code
        , null::VARCHAR as referrer
        , content_customer_billing_address_state_code::VARCHAR as region
        , null::BOOLEAN as registered_to_site
        , 'chargebee' as source
        , null::BOOLEAN as subscribed_to_site
        , CASE content_subscription_billing_period_unit
              WHEN 'month' THEN 'monthly'
              WHEN 'year' THEN 'yearly'
              ELSE content_subscription_billing_period_unit::VARCHAR
          END AS subscription_frequency
        , coalesce(
          content_subscription_subscription_items_0_unit_price::INT, json_extract_path_text(
            json_extract_array_element_text(
              content_subscription_subscription_items, 0
            ),'unit_price'
          )::INT
        ) as subscription_price
        , 'expired'as subscription_status
        , (TIMESTAMP 'epoch' + content_customer_updated_at * INTERVAL '1 second') AS updated_at
        from chargebee_webhook_events.subscription_cancelled
        where content_subscription_cancel_reason_code in ('Not Paid', 'No Card', 'Fraud Review Failed', 'Non Compliant EU Customer', 'Tax Calculation Failed', 'Currency incompatible with Gateway', 'Non Compliant Customer')
        union all
        /*                                        */
        /*        CUSTOMER PRODUCT PAUSED         */
        /*                                        */
        select
        "timestamp"::TIMESTAMP
        , id::VARCHAR
        , content_customer_id::VARCHAR as customer_id
        , content_subscription_id::VARCHAR as subscription_id
        , 'customer_product_paused' event
        , null::VARCHAR as campaign
        , null::VARCHAR as city
        , content_customer_billing_address_country::VARCHAR as country
        -- , content_coupons_0_coupon_code as coupon_code
        , null::VARCHAR as coupon_code
        , (TIMESTAMP 'epoch' + content_customer_created_at * INTERVAL '1 second') AS created_at
        , content_customer_email::VARCHAR as email
        , content_card_first_name::VARCHAR as first_name
        , content_card_last_name::VARCHAR as last_name
        , (TIMESTAMP 'epoch' + content_subscription_current_term_start * INTERVAL '1 second') AS last_payment_date
        -- , content_subscription_current_term_start:: as last_payment_date
        , content_customer_cs_marketing_opt_in::BOOLEAN as marketing_opt_in
        , content_card_first_name::VARCHAR || ' ' || content_card_last_name::VARCHAR AS name
        , null::TIMESTAMP as next_payment_date
        -- , (TIMESTAMP 'epoch' + content_subscription_next_billing_at * INTERVAL '1 second') AS next_payment_date
        , coalesce(
          content_subscription_subscription_items_0_item_price_id::VARCHAR, json_extract_path_text(
            json_extract_array_element_text(
              content_subscription_subscription_items, 0
            ),'item_price_id'
          )::VARCHAR
        ) as plan
        , content_subscription_channel::VARCHAR as platform
        -- , content_coupons_0_coupon_id as promotion_code
        , null::VARCHAR as promotion_code
        , null::VARCHAR as referrer
        , content_customer_billing_address_state_code::VARCHAR as region
        , null::BOOLEAN as registered_to_site
        , 'chargebee' as source
        , null::BOOLEAN as subscribed_to_site
        , CASE content_subscription_billing_period_unit
              WHEN 'month' THEN 'monthly'
              WHEN 'year' THEN 'yearly'
              ELSE content_subscription_billing_period_unit::VARCHAR
          END AS subscription_frequency
        , coalesce(
          content_subscription_subscription_items_0_unit_price::INT, json_extract_path_text(
            json_extract_array_element_text(
              content_subscription_subscription_items, 0
            ),'unit_price'
          )::INT
        ) as subscription_price
        , CASE content_subscription_status
              WHEN 'active' THEN 'enabled'
              WHEN 'in_trial' THEN 'free_trial'
              WHEN 'cancelled' THEN 'cancelled'
              WHEN 'non_renewing' THEN 'non_renewing'
              WHEN 'disabled' THEN 'disabled'
              WHEN 'paused' THEN 'paused'
              ELSE 'unknown' -- Default case if no match is found
            END AS subscription_status
        , (TIMESTAMP 'epoch' + content_customer_updated_at * INTERVAL '1 second') AS updated_at
        from chargebee_webhook_events.subscription_paused
        union all
        /*                                        */
        /*        CUSTOMER PRODUCT RESUMED        */
        /*                                        */
        select
        "timestamp"::TIMESTAMP
        , id::VARCHAR
        , content_customer_id::VARCHAR as customer_id
        , content_subscription_id::VARCHAR as subscription_id
        , 'customer_product_resumed' event
        , null::VARCHAR as campaign
        , null::VARCHAR as city
        , content_customer_billing_address_country::VARCHAR as country
        -- , content_coupons_0_coupon_code as coupon_code
        , null::VARCHAR as coupon_code
        , (TIMESTAMP 'epoch' + content_customer_created_at * INTERVAL '1 second') AS created_at
        , content_customer_email::VARCHAR as email
        , content_card_first_name::VARCHAR as first_name
        , content_card_last_name::VARCHAR as last_name
        , (TIMESTAMP 'epoch' + content_subscription_current_term_start * INTERVAL '1 second') AS last_payment_date
        -- , content_subscription_current_term_start:: as last_payment_date
        , content_customer_cs_marketing_opt_in::BOOLEAN as marketing_opt_in
        , content_card_first_name::VARCHAR || ' ' || content_card_last_name::VARCHAR AS name
        -- , null::TIMESTAMP as next_payment_date
        , (TIMESTAMP 'epoch' + content_subscription_next_billing_at * INTERVAL '1 second') AS next_payment_date
        , coalesce(
          content_subscription_subscription_items_0_item_price_id::VARCHAR, json_extract_path_text(
            json_extract_array_element_text(
              content_subscription_subscription_items, 0
            ),'item_price_id'
          )::VARCHAR
        ) as plan
        , content_subscription_channel::VARCHAR as platform
        -- , content_coupons_0_coupon_id as promotion_code
        , null::VARCHAR as promotion_code
        , null::VARCHAR as referrer
        , content_customer_billing_address_state_code::VARCHAR as region
        , null::BOOLEAN as registered_to_site
        , 'chargebee' as source
        , null::BOOLEAN as subscribed_to_site
        , CASE content_subscription_billing_period_unit
              WHEN 'month' THEN 'monthly'
              WHEN 'year' THEN 'yearly'
              ELSE content_subscription_billing_period_unit::VARCHAR
          END AS subscription_frequency
        , coalesce(
          content_subscription_subscription_items_0_unit_price::INT, json_extract_path_text(
            json_extract_array_element_text(
              content_subscription_subscription_items, 0
            ),'unit_price'
          )::INT
        ) as subscription_price
        , CASE content_subscription_status
              WHEN 'active' THEN 'enabled'
              WHEN 'in_trial' THEN 'free_trial'
              WHEN 'cancelled' THEN 'cancelled'
              WHEN 'non_renewing' THEN 'non_renewing'
              WHEN 'disabled' THEN 'disabled'
              WHEN 'paused' THEN 'paused'
              ELSE 'unknown' -- Default case if no match is found
            END AS subscription_status
        , (TIMESTAMP 'epoch' + content_customer_updated_at * INTERVAL '1 second') AS updated_at
        from chargebee_webhook_events.subscription_resumed
        union all
        /*                                        */
        /*      CUSTOMER PRODUCT CHARGE FAILED    */
        /*                                        */
        select
        "timestamp"::TIMESTAMP
        , id::VARCHAR
        , content_customer_id::VARCHAR as customer_id
        , content_subscription_id::VARCHAR as subscription_id
        , 'customer_product_charge_failed' as event
        , null::VARCHAR as campaign
        , null::VARCHAR as city
        , content_customer_billing_address_country::VARCHAR as country
        -- , content_coupons_0_coupon_code as coupon_code
        , null::VARCHAR as coupon_code
        , (TIMESTAMP 'epoch' + content_customer_created_at * INTERVAL '1 second') AS created_at
        , content_customer_email::VARCHAR as email
        , content_card_first_name::VARCHAR as first_name
        , content_card_last_name::VARCHAR as last_name
        , (TIMESTAMP 'epoch' + content_transaction_updated_at * INTERVAL '1 second') AS last_payment_date
        , content_customer_cs_marketing_opt_in::BOOLEAN as marketing_opt_in
        , content_card_first_name::VARCHAR || ' ' || content_card_last_name::VARCHAR AS name
        , (TIMESTAMP 'epoch' + content_subscription_next_billing_at * INTERVAL '1 second') AS next_payment_date
        , coalesce(
          content_subscription_subscription_items_0_item_price_id::VARCHAR, json_extract_path_text(
            json_extract_array_element_text(
              content_subscription_subscription_items, 0
            ),'item_price_id'
          )::VARCHAR
        ) as plan
        , content_subscription_channel::VARCHAR as platform
        -- , content_coupons_0_coupon_id::VARCHAR as promotion_code
        , null::VARCHAR as promotion_code
        , null::VARCHAR as referrer
        , content_customer_billing_address_state_code::VARCHAR as region
        , null::BOOLEAN as registered_to_site
        , 'chargebee' as source
        , null::BOOLEAN as subscribed_to_site
        , CASE content_subscription_billing_period_unit
              WHEN 'month' THEN 'monthly'
              WHEN 'year' THEN 'yearly'
              ELSE content_subscription_billing_period_unit::VARCHAR
          END AS subscription_frequency
        , coalesce(
          content_subscription_subscription_items_0_unit_price::INT, json_extract_path_text(
            json_extract_array_element_text(
              content_subscription_subscription_items, 0
            ),'unit_price'
          )::INT
        ) as subscription_price
        , CASE content_subscription_status
              WHEN 'active' THEN 'enabled'
              WHEN 'in_trial' THEN 'free_trial'
              WHEN 'cancelled' THEN 'cancelled'
              WHEN 'non_renewing' THEN 'non_renewing'
              WHEN 'disabled' THEN 'disabled'
              WHEN 'paused' THEN 'paused'
              ELSE 'unknown' -- Default case if no match is found
            END AS subscription_status
        , (TIMESTAMP 'epoch' + content_customer_updated_at * INTERVAL '1 second') AS updated_at
        from chargebee_webhook_events.payment_failed
        union all
        /*                                        */
        /*    CUSTOMER PRODUCT SET CANCELLATION   */
        /*                                        */
        select
        "timestamp"::TIMESTAMP
        , id::VARCHAR
        , content_customer_id::VARCHAR as customer_id
        , content_subscription_id::VARCHAR as subscription_id
        , 'customer_product_set_cancellation' event
        , null::VARCHAR as campaign
        , null::VARCHAR as city
        , content_customer_billing_address_country::VARCHAR as country
        -- , content_coupons_0_coupon_code as coupon_code
        , null::VARCHAR as coupon_code
        , (TIMESTAMP 'epoch' + content_customer_created_at * INTERVAL '1 second') AS created_at
        , content_customer_email::VARCHAR as email
        , content_card_first_name::VARCHAR as first_name
        , content_card_last_name::VARCHAR as last_name
        , (TIMESTAMP 'epoch' + content_subscription_current_term_start * INTERVAL '1 second') AS last_payment_date
        -- , content_subscription_current_term_start:: as last_payment_date
        , content_customer_cs_marketing_opt_in::BOOLEAN as marketing_opt_in
        , content_card_first_name::VARCHAR || ' ' || content_card_last_name::VARCHAR AS name
        , null::TIMESTAMP as next_payment_date
        -- , (TIMESTAMP 'epoch' + content_subscription_next_billing_at * INTERVAL '1 second') AS next_payment_date
        , coalesce(
          content_subscription_subscription_items_0_item_price_id::VARCHAR, json_extract_path_text(
            json_extract_array_element_text(
              content_subscription_subscription_items, 0
            ),'item_price_id'
          )::VARCHAR
        ) as plan
        , content_subscription_channel::VARCHAR as platform
        -- , content_coupons_0_coupon_id as promotion_code
        , null::VARCHAR as promotion_code
        , null::VARCHAR as referrer
        , content_customer_billing_address_state_code::VARCHAR as region
        , null::BOOLEAN as registered_to_site
        , 'chargebee' as source
        , null::BOOLEAN as subscribed_to_site
        , CASE content_subscription_billing_period_unit
              WHEN 'month' THEN 'monthly'
              WHEN 'year' THEN 'yearly'
              ELSE content_subscription_billing_period_unit::VARCHAR
          END AS subscription_frequency
        , coalesce(
          content_subscription_subscription_items_0_unit_price::INT, json_extract_path_text(
            json_extract_array_element_text(
              content_subscription_subscription_items, 0
            ),'unit_price'
          )::INT
        ) as subscription_price
        , CASE content_subscription_status
              WHEN 'active' THEN 'enabled'
              WHEN 'in_trial' THEN 'free_trial'
              WHEN 'cancelled' THEN 'cancelled'
              WHEN 'non_renewing' THEN 'non_renewing'
              WHEN 'disabled' THEN 'disabled'
              WHEN 'paused' THEN 'paused'
              ELSE 'unknown' -- Default case if no match is found
            END AS subscription_status
        , (TIMESTAMP 'epoch' + content_customer_updated_at * INTERVAL '1 second') AS updated_at
        from chargebee_webhook_events.subscription_cancellation_scheduled
        union all
        /*                                          */
        /*  CUSTOMER PRODUCT UNDO SET CANCELLATION  */
        /*                                          */
        select
        "timestamp"::TIMESTAMP
        , id::VARCHAR
        , content_customer_id::VARCHAR as customer_id
        , content_subscription_id::VARCHAR as subscription_id
        , 'customer_product_undo_set_cancellation' event
        , null::VARCHAR as campaign
        , null::VARCHAR as city
        , content_customer_billing_address_country::VARCHAR as country
        -- , content_coupons_0_coupon_code as coupon_code
        , null::VARCHAR as coupon_code
        , (TIMESTAMP 'epoch' + content_customer_created_at * INTERVAL '1 second') AS created_at
        , content_customer_email::VARCHAR as email
        , content_card_first_name::VARCHAR as first_name
        , content_card_last_name::VARCHAR as last_name
        , (TIMESTAMP 'epoch' + content_subscription_current_term_start * INTERVAL '1 second') AS last_payment_date
        -- , content_subscription_current_term_start:: as last_payment_date
        , content_customer_cs_marketing_opt_in::BOOLEAN as marketing_opt_in
        , content_card_first_name::VARCHAR || ' ' || content_card_last_name::VARCHAR AS name
        -- , null::TIMESTAMP as next_payment_date
        , (TIMESTAMP 'epoch' + content_subscription_next_billing_at * INTERVAL '1 second') AS next_payment_date
        , coalesce(
          content_subscription_subscription_items_0_item_price_id::VARCHAR, json_extract_path_text(
            json_extract_array_element_text(
              content_subscription_subscription_items, 0
            ),'item_price_id'
          )::VARCHAR
        ) as plan
        , content_subscription_channel::VARCHAR as platform
        -- , content_coupons_0_coupon_id as promotion_code
        , null::VARCHAR as promotion_code
        , null::VARCHAR as referrer
        , content_customer_billing_address_state_code::VARCHAR as region
        , null::BOOLEAN as registered_to_site
        , 'chargebee' as source
        , null::BOOLEAN as subscribed_to_site
        , CASE content_subscription_billing_period_unit
              WHEN 'month' THEN 'monthly'
              WHEN 'year' THEN 'yearly'
              ELSE content_subscription_billing_period_unit::VARCHAR
          END AS subscription_frequency
        , coalesce(
          content_subscription_subscription_items_0_unit_price::INT, json_extract_path_text(
            json_extract_array_element_text(
              content_subscription_subscription_items, 0
            ),'unit_price'
          )::INT
        ) as subscription_price
        , CASE content_subscription_status
              WHEN 'active' THEN 'enabled'
              WHEN 'in_trial' THEN 'free_trial'
              WHEN 'cancelled' THEN 'cancelled'
              WHEN 'non_renewing' THEN 'non_renewing'
              WHEN 'disabled' THEN 'disabled'
              WHEN 'paused' THEN 'paused'
              ELSE 'unknown' -- Default case if no match is found
            END AS subscription_status
        , (TIMESTAMP 'epoch' + content_customer_updated_at * INTERVAL '1 second') AS updated_at
        from chargebee_webhook_events.subscription_scheduled_cancellation_removed
        union all
        /*                                        */
        /*      CUSTOMER PRODUCT SET PAUSED       */
        /*                                        */
        select
        "timestamp"::TIMESTAMP
        , id::VARCHAR
        , content_customer_id::VARCHAR as customer_id
        , content_subscription_id::VARCHAR as subscription_id
        , 'customer_product_set_paused' event
        , null::VARCHAR as campaign
        , null::VARCHAR as city
        , content_customer_billing_address_country::VARCHAR as country
        -- , content_coupons_0_coupon_code as coupon_code
        , null::VARCHAR as coupon_code
        , (TIMESTAMP 'epoch' + content_customer_created_at * INTERVAL '1 second') AS created_at
        , content_customer_email::VARCHAR as email
        , content_card_first_name::VARCHAR as first_name
        , content_card_last_name::VARCHAR as last_name
        , (TIMESTAMP 'epoch' + content_subscription_current_term_start * INTERVAL '1 second') AS last_payment_date
        -- , content_subscription_current_term_start:: as last_payment_date
        , content_customer_cs_marketing_opt_in::BOOLEAN as marketing_opt_in
        , content_card_first_name::VARCHAR || ' ' || content_card_last_name::VARCHAR AS name
        -- , null::TIMESTAMP as next_payment_date
        , (TIMESTAMP 'epoch' + content_subscription_next_billing_at * INTERVAL '1 second') AS next_payment_date
        , coalesce(
          content_subscription_subscription_items_0_item_price_id::VARCHAR, json_extract_path_text(
            json_extract_array_element_text(
              content_subscription_subscription_items, 0
            ),'item_price_id'
          )::VARCHAR
        ) as plan
        , content_subscription_channel::VARCHAR as platform
        -- , content_coupons_0_coupon_id as promotion_code
        , null::VARCHAR as promotion_code
        , null::VARCHAR as referrer
        , content_customer_billing_address_state_code::VARCHAR as region
        , null::BOOLEAN as registered_to_site
        , 'chargebee' as source
        , null::BOOLEAN as subscribed_to_site
        , CASE content_subscription_billing_period_unit
              WHEN 'month' THEN 'monthly'
              WHEN 'year' THEN 'yearly'
              ELSE content_subscription_billing_period_unit::VARCHAR
          END AS subscription_frequency
        , coalesce(
          content_subscription_subscription_items_0_unit_price::INT, json_extract_path_text(
            json_extract_array_element_text(
              content_subscription_subscription_items, 0
            ),'unit_price'
          )::INT
        ) as subscription_price
        , CASE content_subscription_status
              WHEN 'active' THEN 'enabled'
              WHEN 'in_trial' THEN 'free_trial'
              WHEN 'cancelled' THEN 'cancelled'
              WHEN 'non_renewing' THEN 'non_renewing'
              WHEN 'disabled' THEN 'disabled'
              WHEN 'paused' THEN 'paused'
              ELSE 'unknown' -- Default case if no match is found
            END AS subscription_status
        , (TIMESTAMP 'epoch' + content_customer_updated_at * INTERVAL '1 second') AS updated_at
        from chargebee_webhook_events.subscription_pause_scheduled
        union all
        /*                                        */
        /*      CUSTOMER PRODUCT SET PAUSED       */
        /*                                        */
        select
        "timestamp"::TIMESTAMP
        , id::VARCHAR
        , content_customer_id::VARCHAR as customer_id
        , content_subscription_id::VARCHAR as subscription_id
        , 'customer_product_undo_set_paused' event
        , null::VARCHAR as campaign
        , null::VARCHAR as city
        , content_customer_billing_address_country::VARCHAR as country
        -- , content_coupons_0_coupon_code as coupon_code
        , null::VARCHAR as coupon_code
        , (TIMESTAMP 'epoch' + content_customer_created_at * INTERVAL '1 second') AS created_at
        , content_customer_email::VARCHAR as email
        , content_card_first_name::VARCHAR as first_name
        , content_card_last_name::VARCHAR as last_name
        , (TIMESTAMP 'epoch' + content_subscription_current_term_start * INTERVAL '1 second') AS last_payment_date
        -- , content_subscription_current_term_start:: as last_payment_date
        , content_customer_cs_marketing_opt_in::BOOLEAN as marketing_opt_in
        , content_card_first_name::VARCHAR || ' ' || content_card_last_name::VARCHAR AS name
        -- , null::TIMESTAMP as next_payment_date
        , (TIMESTAMP 'epoch' + content_subscription_next_billing_at * INTERVAL '1 second') AS next_payment_date
        , coalesce(
          content_subscription_subscription_items_0_item_price_id::VARCHAR, json_extract_path_text(
            json_extract_array_element_text(
              content_subscription_subscription_items, 0
            ),'item_price_id'
          )::VARCHAR
        ) as plan
        , content_subscription_channel::VARCHAR as platform
        -- , content_coupons_0_coupon_id as promotion_code
        , null::VARCHAR as promotion_code
        , null::VARCHAR as referrer
        , content_customer_billing_address_state_code::VARCHAR as region
        , null::BOOLEAN as registered_to_site
        , 'chargebee' as source
        , null::BOOLEAN as subscribed_to_site
        , CASE content_subscription_billing_period_unit
              WHEN 'month' THEN 'monthly'
              WHEN 'year' THEN 'yearly'
              ELSE content_subscription_billing_period_unit::VARCHAR
          END AS subscription_frequency
        , coalesce(
          content_subscription_subscription_items_0_unit_price::INT, json_extract_path_text(
            json_extract_array_element_text(
              content_subscription_subscription_items, 0
            ),'unit_price'
          )::INT
        ) as subscription_price
        , CASE content_subscription_status
              WHEN 'active' THEN 'enabled'
              WHEN 'in_trial' THEN 'free_trial'
              WHEN 'cancelled' THEN 'cancelled'
              WHEN 'non_renewing' THEN 'non_renewing'
              WHEN 'disabled' THEN 'disabled'
              WHEN 'paused' THEN 'paused'
              ELSE 'unknown' -- Default case if no match is found
            END AS subscription_status
        , (TIMESTAMP 'epoch' + content_customer_updated_at * INTERVAL '1 second') AS updated_at
        from chargebee_webhook_events.subscription_scheduled_pause_removed
      )
      , gaithertv_events as (
        select * from events
        WHERE plan LIKE '%GaitherTV%'
      )
      , chargebee_webhook_analytics as (
        select
        date(timestamp) as date
        , 'web' as platform
        ,count(case when (event = 'customer_product_free_trial_created') then 1 else null end) as free_trial_created
        , count(case when (event = 'customer_product_free_trial_converted') then 1 else null end) as free_trial_converted
        , count(case when (event = 'customer_product_free_trial_expired') then 1 else null end) as free_trial_churn
        , count(case when (event = 'customer_product_created') then 1 else null end) as paying_created
        , count(case when (event = 'customer_product_cancelled') then 1 else null end) as paying_churn
        , count(case when (event = 'customer_product_paused') then 1 else null end) as paused_created
        from gaithertv_events
        group by 1,2 order by 1
      )
      , subs as (
        with p0 as (
          SELECT
          uploaded_at
          , subscription_id
          , customer_id
          , subscription_status as status
          , subscription_subscription_items_0_object
          , subscription_subscription_items_0_item_type
          , subscription_subscription_items_0_unit_price
          , subscription_subscription_items_0_item_price_id
          FROM http_api.chargebee_subscriptions
          WHERE subscription_subscription_items_0_item_price_id LIKE '%GaitherTV%'
          )
          select
          a.*
          , b.*
          , row_number() over (partition by a.subscription_id,uploaded_at order by uploaded_at desc, b."timestamp" desc) as rn
          from p0 a
          left join gaithertv_events b
          on a.customer_id = b.customer_id and a.uploaded_at = date(b."timestamp")
        )
          , chargebee_totals as (
            select
            uploaded_at
            , count(case when (status = 'active' or status = 'non_renewing') then 1 else null end) as total_paying
            , count(case when (status = 'in_trial') then 1 else null end) as total_free_trials
            from subs
            group by 1 order by 1
          )
          , get_analytics_p1 as (
            select
            a.timestamp as timestamp
            , existing_free_trials
            , existing_paying
            , (a.free_trial_created + b.free_trial_created) as free_trial_created
            , (a.free_trial_converted + b.free_trial_converted) as free_trial_converted
            , a.free_trial_churn + b.free_trial_churn as free_trial_churn
            -- , NULL::INTEGER as paused_created
            , greatest(0, a.paying_created - b.free_trial_created - a.free_trial_converted) as paying_created
            , greatest(0, a.paying_churn - b.free_trial_churn) as paying_churn
            -- Need to figure chargebee trials out
            , a.total_free_trials + c.total_free_trials as total_free_trials
            , greatest(0, a.total_paying - c.total_free_trials) as total_paying
            from (select * from get_analytics_p0 where report_version = 2 and n=1) as a
            left join chargebee_webhook_analytics as b
            on date(a.timestamp) = b."date"
            left join chargebee_totals as c
            on date(a.timestamp) = date(c.uploaded_at)
          )
          -- BACK UP SOURCE INCASE OF API OUTAGE
          , distinct_events as (
            select distinct user_id
            , action
            , status
            , platform
            , frequency
            , to_timestamp(customer_created_at, 'YYYY-MM-DD HH24:MI:SS') as customer_created_at
            , to_timestamp(event_created_at, 'YYYY-MM-DD HH24:MI:SS') as event_created_at
            , to_date(event_created_at, 'YYYY-MM-DD') as event_date
            , to_date(report_date, 'YYYY-MM-DD') as report_date
            from customers.gaithertvplus_all_customers
            where action = 'subscription'
          )
          , total_counts as (
            select report_date
            , count(distinct case when status = 'enabled' and platform = 'api' then user_id end) as total_paying_api
            , lag(total_paying_api, 1) over (order by report_date) as existing_paying
            , count(distinct case when status = 'free_trial' and platform = 'api' then user_id end) as total_free_trials_api
            , lag(total_free_trials_api, 1) over (order by report_date) as existing_free_trials_api
            , count(distinct case when status = 'enabled' and platform = 'roku' then user_id end) as total_paying_roku
            , lag(total_paying_roku, 1) over (order by report_date) as existing_paying_roku
            , count(distinct case when status = 'free_trial' and platform = 'roku' then user_id end) as total_free_trials_roku
            , lag(total_free_trials_roku, 1) over (order by report_date) as existing_free_trials_roku
            , count(distinct case when status = 'enabled' and platform = 'ios' then user_id end) as total_paying_ios
            , lag(total_paying_ios, 1) over (order by report_date) as existing_paying_ios
            , count(distinct case when status = 'free_trial' and platform = 'ios' then user_id end) as total_free_trials_ios
            , lag(total_free_trials_ios, 1) over (order by report_date) as existing_free_trials_ios
            , count(distinct case when status = 'enabled' and platform = 'tvos' then user_id end) as total_paying_tvos
            , lag(total_paying_tvos, 1) over (order by report_date) as existing_paying_tvos
            , count(distinct case when status = 'free_trial' and platform = 'tvos' then user_id end) as total_free_trials_tvos
            , lag(total_free_trials_tvos, 1) over (order by report_date) as existing_free_trials_tvos
            , count(distinct case when status = 'enabled' and platform = 'android' then user_id end) as total_paying_android
            , lag(total_paying_android, 1) over (order by report_date) as existing_paying_android
            , count(distinct case when status = 'free_trial' and platform = 'android' then user_id end) as total_free_trials_android
            , lag(total_free_trials_android, 1) over (order by report_date) as existing_free_trials_android
            , count(distinct case when status = 'enabled' and platform = 'android_tv' then user_id end) as total_paying_android_tv
            , lag(total_paying_android_tv, 1) over (order by report_date) as existing_paying_android_tv
            , count(distinct case when status = 'free_trial' and platform = 'android_tv' then user_id end) as total_free_trials_android_tv
            , lag(total_free_trials_android_tv, 1) over (order by report_date) as existing_free_trials_android_tv
            , count(distinct case when status = 'enabled' and platform = 'amazon_fire_tv' then user_id end) as total_paying_fire_tv
            , lag(total_paying_fire_tv, 1) over (order by report_date) as existing_paying_fire_tv
            , count(distinct case when status = 'free_trial' and platform = 'amazon_fire_tv' then user_id end) as total_free_trials_fire_tv
            , lag(total_free_trials_fire_tv, 1) over (order by report_date) as existing_free_trials_fire_tv
            from distinct_events
            group by 1
          )
          , latest_date AS (
          SELECT MAX(report_date) AS latest_report_date FROM total_counts
          )
          -- AS OF 02/02/2024. Update Monthly
          , app_platform_offsets as (
            select
            (205 - total_paying_ios - total_paying_tvos) as apple_offset
            , (124 - total_paying_android - total_paying_android_tv) as android_offset
            , (1214 - total_paying_roku) as roku_offset
            , (460 - total_paying_fire_tv) as fire_tv_offset
            from total_counts, latest_date
            where total_counts.report_date = latest_date.latest_report_date
          )
          , customers_analytics as (
            with p0 as (
              select get_analytics_p1.timestamp,
              -- coalesce(get_analytics_p1.existing_free_trials, total_counts.existing_free_trials) as existing_free_trials,
              -- coalesce(get_analytics_p1.existing_paying, total_counts.existing_paying) as existing_paying,
              get_analytics_p1.free_trial_churn,
              get_analytics_p1.free_trial_converted,
              get_analytics_p1.free_trial_created,
              -- get_analytics_p1.paused_created,
              get_analytics_p1.paying_churn,
              get_analytics_p1.paying_created,
              get_analytics_p1.total_free_trials as total_free_trials,
              (get_analytics_p1.total_paying+apple_offset+android_offset+roku_offset+fire_tv_offset) as total_paying
              from get_analytics_p1, app_platform_offsets
              -- full join total_counts
              -- on total_counts.report_date = trunc(get_analytics_p1.timestamp)
            )
            select * from p0 where date(timestamp) < current_date
          ),

      a as (select a.timestamp, ROW_NUMBER() OVER(ORDER BY a.timestamp desc) AS Row
      from customers_analytics as a),

      b as (select a.timestamp,total_paying,ROW_NUMBER() OVER(ORDER BY a.timestamp desc) AS Row
      from customers_analytics as a where a.timestamp < (DATEADD(day,-30, DATE_TRUNC('day',GETDATE()) ))),

      c as (select a.timestamp,total_paying as paying_30_days_prior from a inner join b on a.row=b.row),

      d as ((select a1.timestamp, a1.paying_churn+sum(coalesce(a2.paying_churn,0)) as churn_30_days, a1.paying_churn+sum(coalesce(a2.paying_created,0)) as winback_30_days
      from customers_analytics as a1
      left join customers_analytics as a2 on datediff(day,a2.timestamp,a1.timestamp)<=29 and datediff(day,a2.timestamp,a1.timestamp)>0
      group by a1.timestamp,a1.paying_churn)),

      e as (select c.timestamp, cast(paying_30_days_prior as decimal) as paying_30_days_prior,
      cast(churn_30_days as decimal) as churn_30_days,
      cast(paying_30_days_prior as decimal)/cast(churn_30_days as decimal) as churn_30_day_percent,
      cast(winback_30_days as decimal) as winback_30_days
      from c inner join d on c.timestamp=d.timestamp),

      f as (select *, sum((49000-(total_paying))/nullif((365-day_of_year),0)) OVER (PARTITION by cast(datepart(month,date(timestamp)) as varchar) order by timestamp asc rows between unbounded preceding and current row) as Running_Free_Trial_Target
      from (select *, SUM(free_trial_created) OVER (PARTITION by cast(datepart(month,date(timestamp)) as varchar) order by timestamp asc rows between unbounded preceding and current row) AS Running_Free_Trials
      from (select distinct * from (select a.*,
      case when extract(YEAR from a.timestamp)='2018' then 795+((49000-795)*(cast(datepart(dayofyear,date(a.timestamp)) as integer)-1)/365)
      when extract(YEAR from a.timestamp)='2019' then 16680+((55000-16680)*(cast(datepart(dayofyear,date(a.timestamp)) as integer)-1)/365)
      when extract(YEAR from a.timestamp)='2020' then 64907+((125000-64907)*(cast(datepart(dayofyear,date(a.timestamp)) as integer)-1)/365)
      when extract(YEAR from a.timestamp)='2021' then 148678+((190000-148678)*(cast(datepart(dayofyear,date(a.timestamp)) as integer)-1)/365)
      when extract(YEAR from a.timestamp)='2022' then 229371+((339000-229371)*(cast(datepart(dayofyear,date(a.timestamp)) as integer)-1)/365) end as target,
      case when extract(YEAR from a.timestamp)='2018' then 3246+((49000-3246)*(cast(datepart(dayofyear,date(a.timestamp)) as integer)-1)/365)
      when extract(YEAR from a.timestamp)='2019' then 24268+((55000-24268)*(cast(datepart(dayofyear,date(a.timestamp)) as integer)-1)/365)
      when extract(YEAR from a.timestamp)='2020' then 70039+((125000-70039)*(cast(datepart(dayofyear,date(a.timestamp)) as integer)-1)/365)
      when extract(YEAR from a.timestamp)='2021' then 157586+((190000-157586)*(cast(datepart(dayofyear,date(a.timestamp)) as integer)-1)/365)
      when extract(YEAR from a.timestamp)='2022' then 243181+((339000-243181)*(cast(datepart(dayofyear,date(a.timestamp)) as integer)-1)/365) end as total_target, -- this baseline is paid subs + free trial
      229371+((339000-229371)*(cast(datepart(dayofyear,date(a.timestamp)) as integer)+14)/365) as target_14_days_future, -- this baseline matches total_target baseline
      cast(datepart(dayofyear,date(a.timestamp)) as integer)-1 as day_of_year,
      cast(datepart(dayofyear,date(a.timestamp)) as integer)+14 as day_of_year_14_days,
      case when extract(YEAR from a.timestamp)='2018' then 49000
      when extract(YEAR from a.timestamp)='2019' then 55000
      when extract(YEAR from a.timestamp)='2020' then 125000
      when extract(YEAR from a.timestamp)='2021' then 190000
      when extract(YEAR from a.timestamp)='2022' then 339000 end as annual_target,
      -- case when rownum=max(rownum) over(partition by Week) then existing_paying end as PriorWeekExistingSubs,
      -- case when rownum=max(rownum) over(partition by Month) then existing_paying end as PriorMonthExistingSubs,
      case when rownum=min(rownum) over(partition by Week||year) then total_paying end as CurrentWeekExistingSubs,
      case when rownum=min(rownum) over(partition by Month||year) then total_paying end as CurrentMonthExistingSubs,
      wait_content,
      save_money,
      vacation,
      high_price,
      other
      from
      ((select a.*,cast(datepart(week,date(timestamp)) as varchar) as Week,
      cast(datepart(month,date(timestamp)) as varchar) as Month,
      cast(datepart(Quarter,date(timestamp)) as varchar) as Quarter,
      cast(datepart(Year,date(timestamp)) as varchar) as Year,
      new_trials_14_days_prior from
      (select *, row_number() over(order by timestamp desc) as rownum from customers_analytics) as a
      left join
      (select free_trial_created as new_trials_14_days_prior, row_number() over(order by timestamp desc) as rownum from customers_analytics
      where timestamp in
      (select dateadd(day,-14,timestamp) as timestamp from customers_analytics )) as b on a.rownum=b.rownum)) as a
      left join customers.churn_reasons_aggregated as b on a.timestamp=b."timestamp")) as a))

      , outer_query as (
      select f.*,paying_30_days_prior,churn_30_days,churn_30_day_percent,winback_30_days from e inner join f on e.timestamp=f.timestamp
      )
      select * from outer_query order by timestamp
      ;;
  }

  dimension_group: timestamp {
    type: time
    timeframes: [
      raw,
      time,
      date,
      day_of_week,
      week,
      month,
      quarter,
      year
    ]
    sql: ${TABLE}.timestamp ;;
  }

  dimension: ios_tvos_subscriber_offset {
    sql:  ;;
  }

  dimension: total_paying {
    sql: ${TABLE}.total_paying ;;
  }

  dimension: total_free_trials {
    sql: ${TABLE}.total_free_trials ;;
  }

  dimension: free_trial_created {
    sql: ${TABLE}.free_trial_created ;;
  }

  dimension: new_trials_14_days_prior{
    type: number
    sql: ${TABLE}.new_trials_14_days_prior;;
  }


  dimension: free_trial_converted {
    sql: ${TABLE}.free_trial_converted ;;
  }

  dimension: paying_created {
    sql: ${TABLE}.paying_created ;;
  }

  dimension: paying_churn {
    sql: ${TABLE}.paying_churn ;;
  }

  measure: total_paid {
    type: sum
    sql: ${total_paying} ;;
  }

  measure: total_trials {
    type: sum
    sql: ${total_free_trials} ;;
  }

  measure: total_count {
    type: number
    sql: ${total_paid}+${total_trials} ;;
  }

  measure: new_trials {
    type: sum
    description: "Total number of new trials during a time period."
    sql:  ${free_trial_created} ;;
  }

  measure: total_new_trials_14_days_prior {
    type: sum
    sql: ${TABLE}.new_trials_14_days_prior;;
  }

  measure: trial_to_paid {
    type: sum
    description: "Total number of trials to paid during a time period."
    sql:  ${free_trial_converted} ;;
  }

  measure: conversion_rate_v2 {
    type: number
    value_format: ".0#\%"
    sql: 100.0*${trial_to_paid}/NULLIF(${total_new_trials_14_days_prior},0) ;;
  }

  measure: new_paid {
    type: sum
    description: "Total number of new paids during a time period."
    sql:  ${paying_created} ;;
  }

  measure:  new_paid_total{
    type: sum
    description: "Total number of new paid subs (reacquisitions) and free trial to paid."
    sql: ${free_trial_converted}+${paying_created};;
  }

  measure: new_cancelled_paid {
    type: sum
    description: "Total number of cancelled paid subs during a time period."
    sql:  ${paying_churn} ;;
  }

  measure: cancelled_subs {
    type: sum
    sql: ${paying_churn}*-1 ;;
  }

  measure: net_subscriber_growth{
    type: number
    sql: ${new_paid_total}+${cancelled_subs} ;;
  }

  measure: end_of_month_subs {
    type: sum
    sql: ${TABLE}.CurrentMonthExistingSubs ;;
  }

}
