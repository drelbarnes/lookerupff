view: chargebee_webhook_events {
  derived_table: {
    sql: with event_mapping as (
        /*                            */
        /*      CUSTOMER CREATED      */
        /*                            */
        select
        "timestamp"::TIMESTAMP
        , _id::VARCHAR as id
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
        , _id::VARCHAR as id
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
        , _id::VARCHAR as id
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
        , _id::VARCHAR as id
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
        , _id::VARCHAR as id
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
        , _id::VARCHAR as id
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
        , _id::VARCHAR as id
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
        , _id::VARCHAR as id
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
        where (
          content_subscription_cancel_reason_code not in ('Not Paid', 'No Card', 'Fraud Review Failed', 'Non Compliant EU Customer', 'Tax Calculation Failed', 'Currency incompatible with Gateway', 'Non Compliant Customer')
          or content_subscription_cancel_reason_code is null
        )
        union all
        /*                                        */
        /*      CUSTOMER PRODUCT CANCELLED        */
        /*  CUSTOMER PRODUCT FREE TRIAL EXPIRED   */
        /*                DUNNING                 */
        /*                                        */
        select
        "timestamp"::TIMESTAMP
        , _id::VARCHAR as id
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
        , _id::VARCHAR as id
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
        , _id::VARCHAR as id
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
        , _id::VARCHAR as id
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
        , _id::VARCHAR as id
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
        , _id::VARCHAR as id
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
        , _id::VARCHAR as id
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
        , _id::VARCHAR as id
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
      , distinct_events as (
        select * from (select *, row_number() over (partition by id order by "timestamp") as rn from event_mapping) where rn = 1
      )
      select *, row_number() over (order by "timestamp", customer_id) as row from distinct_events
      ;;
    datagroup_trigger: upff_acquisition_reporting
    distribution_style: all
  }

  dimension_group: timestamp {
    type: time
    sql: ${TABLE}."timestamp" ;;
  }

  dimension: id {
    type: string
    sql: ${TABLE}.id ;;
  }

  dimension: customer_id {
    type: string
    sql: ${TABLE}.customer_id ;;
  }

  dimension: subscription_id {
    type: string
    sql: ${TABLE}.subscription_id ;;
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

  dimension: coupon_code {
    type: string
    sql: ${TABLE}.coupon_code ;;
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

  dimension: row {
    type: number
    sql: ${TABLE}.row ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  measure: total_subscriptions {
    type: count_distinct
    sql: ${subscription_id} ;;
    label: "Total Unique Subscriptions"
  }

  measure: total_customers {
    type: count_distinct
    sql: ${customer_id} ;;
    label: "Total Unique Customers"
  }

  set: detail {
    fields: [
      timestamp_date,
      id,
      customer_id,
      subscription_id,
      event,
      campaign,
      city,
      country,
      coupon_code,
      created_at_date,
      email,
      first_name,
      last_name,
      last_payment_date_date,
      marketing_opt_in,
      name,
      next_payment_date_date,
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
      updated_at_date
    ]
  }

}
