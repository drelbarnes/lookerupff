view: upff_chargebee_webhook_events {
  derived_table: {
    sql: with event_mapping as (
        /*                            */
        /*      CUSTOMER CREATED      */
        /*                            */
        select
            safe_cast(`timestamp` as TIMESTAMP) as `timestamp`,
            safe_cast(_id as STRING) as id,
            safe_cast(content_customer_id as STRING) as customer_id,
            safe_cast(null as string) as subscription_id,
            'customer_created' as event,
            safe_cast(null as string) as campaign,
            safe_cast(null as string) as city,
            safe_cast(content_customer_billing_address_country as STRING) as country,
            safe_cast(null as string) as coupon_code,
            TIMESTAMP_SECONDS(content_customer_created_at) as created_at,
            safe_cast(content_customer_email as STRING) as email,
            safe_cast(content_card_first_name as STRING) as first_name,
            safe_cast(content_card_last_name as STRING) as last_name,
            safe_cast(null as TIMESTAMP) as last_payment_date,
            safe_cast(content_customer_cs_marketing_opt_in as BOOLEAN) as marketing_opt_in,
            concat(safe_cast(content_card_first_name as STRING), ' ', safe_cast(content_card_last_name as STRING)) as name,
            safe_cast(null as TIMESTAMP) as next_payment_date,
            safe_cast(null as string) as plan,
            'web' as platform,
            safe_cast(null as string) as promotion_code,
            safe_cast(null as string) as referrer,
            safe_cast(content_customer_billing_address_state_code as STRING) as region,
            safe_cast(null as BOOLEAN) as registered_to_site,
            'chargebee' as source,
            safe_cast(null as BOOLEAN) as subscribed_to_site,
            safe_cast(null as string) as subscription_frequency,
            safe_cast(null as INT64) as subscription_price,
            safe_cast(null as string) as subscription_status,
            TIMESTAMP_SECONDS(content_customer_updated_at) as updated_at
        from chargebee_webhook_events.customer_created
        union all
        /*                            */
        /*      CUSTOMER DELETED      */
        /*                            */
        select
            safe_cast(`timestamp` as TIMESTAMP) as `timestamp`,
            safe_cast(_id as STRING) as id,
            safe_cast(content_customer_id as STRING) as customer_id,
            safe_cast(null as string) as subscription_id,
            'customer_deleted' as event,
            safe_cast(null as string) as campaign,
            safe_cast(null as string) as city,
            safe_cast(content_customer_billing_address_country as STRING) as country,
            safe_cast(null as string) as coupon_code,
            TIMESTAMP_SECONDS(content_customer_created_at) as created_at,
            safe_cast(content_customer_email as STRING) as email,
            safe_cast(content_card_first_name as STRING) as first_name,
            safe_cast(content_card_last_name as STRING) as last_name,
            safe_cast(null as timestamp) as last_payment_date,
            safe_cast(content_customer_cs_marketing_opt_in as BOOLEAN) as marketing_opt_in,
            concat(safe_cast(content_card_first_name as STRING), ' ', safe_cast(content_card_last_name as STRING)) as name,
            safe_cast(null as timestamp) as next_payment_date,
            safe_cast(null as string) as plan,
            'web' as platform,
            safe_cast(null as string) as promotion_code,
            safe_cast(null as string) as referrer,
            safe_cast(null as string) as region,
            safe_cast(null as BOOLEAN) as registered_to_site,
            'chargebee' as source,
            safe_cast(null as BOOLEAN) as subscribed_to_site,
            safe_cast(null as string) as subscription_frequency,
            safe_cast(null as INT64) as subscription_price,
            safe_cast(null as string) as subscription_status,
            TIMESTAMP_SECONDS(content_customer_updated_at) as updated_at
        from chargebee_webhook_events.customer_deleted
        union all
        /*                            */
        /*      CUSTOMER UPDATED      */
        /*                            */
        select
            safe_cast(`timestamp` as TIMESTAMP) as `timestamp`,
            safe_cast(_id as STRING) as id,
            safe_cast(content_customer_id as STRING) as customer_id,
            safe_cast(null as STRING) as subscription_id,
            'customer_updated' as event,
            safe_cast(null as STRING) as campaign,
            safe_cast(null as STRING) as city,
            safe_cast(content_customer_billing_address_country as STRING) as country,
            safe_cast(null as STRING) as coupon_code,
            TIMESTAMP_SECONDS(content_customer_created_at) as created_at,
            safe_cast(content_customer_email as STRING) as email,
            safe_cast(content_card_first_name as STRING) as first_name,
            safe_cast(content_card_last_name as STRING) as last_name,
            safe_cast(null as TIMESTAMP) as last_payment_date,
            safe_cast(content_customer_cs_marketing_opt_in as BOOLEAN) as marketing_opt_in,
            concat(safe_cast(content_card_first_name as STRING), ' ', safe_cast(content_card_last_name as STRING)) as name,
            safe_cast(null as TIMESTAMP) as next_payment_date,
            safe_cast(null as STRING) as plan,
            'web' as platform,
            safe_cast(null as STRING) as promotion_code,
            safe_cast(null as STRING) as referrer,
            safe_cast(content_customer_billing_address_state_code as STRING) as region,
            safe_cast(null as BOOLEAN) as registered_to_site,
            'chargebee' as source,
            safe_cast(null as BOOLEAN) as subscribed_to_site,
            safe_cast(null as STRING) as subscription_frequency,
            safe_cast(null as INT64) as subscription_price,
            safe_cast(null as STRING) as subscription_status,
            TIMESTAMP_SECONDS(content_customer_updated_at) as updated_at
        from chargebee_webhook_events.customer_changed
        union all
        /*                                        */
        /*        CUSTOMER PRODUCT CREATED        */
        /*  CUSTOMER PRODUCT FREE TRIAL CREATED   */
        /*                                        */
        select
            safe_cast(`timestamp` as TIMESTAMP) as `timestamp`,
            safe_cast(_id as STRING) as id,
            safe_cast(content_customer_id as STRING) as customer_id,
            safe_cast(content_subscription_id as STRING) as subscription_id,
            CASE
                WHEN event = 'subscription_created' AND content_subscription_status = 'in_trial' THEN 'customer_product_free_trial_created'
                ELSE 'customer_product_created'
            END AS event,
            safe_cast(null as STRING) as campaign,
            safe_cast(null as STRING) as city,
            safe_cast(content_customer_billing_address_country as STRING) as country,
            safe_cast(null as STRING) as coupon_code,
            TIMESTAMP_SECONDS(content_customer_created_at) as created_at,
            safe_cast(content_customer_email as STRING) as email,
            safe_cast(content_card_first_name as STRING) as first_name,
            safe_cast(content_card_last_name as STRING) as last_name,
            safe_cast(null as TIMESTAMP) as last_payment_date,
            safe_cast(content_customer_cs_marketing_opt_in as BOOLEAN) as marketing_opt_in,
            concat(safe_cast(content_card_first_name as STRING), ' ', safe_cast(content_card_last_name as STRING)) as name,
            TIMESTAMP_SECONDS(content_subscription_next_billing_at) as next_payment_date,
            coalesce(
                safe_cast(content_subscription_subscription_items_0_item_price_id as STRING),
                safe_cast(json_extract_scalar(json_extract(json_extract(content_subscription_subscription_items, '$[0]'), '$.item_price_id')) as STRING)
            ) as plan,
            safe_cast(content_subscription_channel as STRING) as platform,
            safe_cast(null as STRING) as promotion_code,
            safe_cast(null as STRING) as referrer,
            safe_cast(content_customer_billing_address_state_code as STRING) as region,
            safe_cast(null as BOOLEAN) as registered_to_site,
            'chargebee' as source,
            safe_cast(null as BOOLEAN) as subscribed_to_site,
            CASE content_subscription_billing_period_unit
                WHEN 'month' THEN 'monthly'
                WHEN 'year' THEN 'yearly'
                ELSE safe_cast(content_subscription_billing_period_unit as STRING)
            END AS subscription_frequency,
            coalesce(
                safe_cast(content_subscription_subscription_items_0_unit_price as INT64),
                safe_cast(json_extract_scalar(json_extract(json_extract(content_subscription_subscription_items, '$[0]'), '$.unit_price')) as INT64)
            ) as subscription_price,
            CASE content_subscription_status
                WHEN 'active' THEN 'enabled'
                WHEN 'in_trial' THEN 'free_trial'
                WHEN 'cancelled' THEN 'cancelled'
                WHEN 'non_renewing' THEN 'non_renewing'
                WHEN 'disabled' THEN 'disabled'
                WHEN 'paused' THEN 'paused'
                ELSE 'unknown'
            END AS subscription_status,
            TIMESTAMP_SECONDS(content_customer_updated_at) as updated_at
        from chargebee_webhook_events.subscription_created
        union all
        /*                                        */
        /*  CUSTOMER PRODUCT FREE TRIAL CONVERTED */
        /*                                        */
        select
            safe_cast(`timestamp` as TIMESTAMP) as `timestamp`,
            safe_cast(_id as STRING) as id,
            safe_cast(content_customer_id as STRING) as customer_id,
            safe_cast(content_subscription_id as STRING) as subscription_id,
            'customer_product_free_trial_converted' as event,
            safe_cast(null as STRING) as campaign,
            safe_cast(null as STRING) as city,
            safe_cast(content_customer_billing_address_country as STRING) as country,
            safe_cast(null as STRING) as coupon_code,
            TIMESTAMP_SECONDS(content_customer_created_at) as created_at,
            safe_cast(content_customer_email as STRING) as email,
            safe_cast(content_card_first_name as STRING) as first_name,
            safe_cast(content_card_last_name as STRING) as last_name,
            TIMESTAMP_SECONDS(content_invoice_paid_at) as last_payment_date,
            safe_cast(content_customer_cs_marketing_opt_in as BOOLEAN) as marketing_opt_in,
            concat(safe_cast(content_card_first_name as STRING), ' ', safe_cast(content_card_last_name as STRING)) as name,
            TIMESTAMP_SECONDS(content_subscription_next_billing_at) as next_payment_date,
            coalesce(
                safe_cast(content_subscription_subscription_items_0_item_price_id as STRING),
                safe_cast(json_extract_scalar(json_extract(json_extract(content_subscription_subscription_items, '$[0]'), '$.item_price_id')) as STRING)
            ) as plan,
            safe_cast(content_subscription_channel as STRING) as platform,
            safe_cast(null as STRING) as promotion_code,
            safe_cast(null as STRING) as referrer,
            safe_cast(content_customer_billing_address_state_code as STRING) as region,
            safe_cast(null as BOOLEAN) as registered_to_site,
            'chargebee' as source,
            safe_cast(null as BOOLEAN) as subscribed_to_site,
            CASE content_subscription_billing_period_unit
                WHEN 'month' THEN 'monthly'
                WHEN 'year' THEN 'yearly'
                ELSE safe_cast(content_subscription_billing_period_unit as STRING)
            END AS subscription_frequency,
            coalesce(
                safe_cast(content_subscription_subscription_items_0_unit_price as INT64),
                safe_cast(json_extract_scalar(json_extract(json_extract(content_subscription_subscription_items, '$[0]'), '$.unit_price')) as INT64)
            ) as subscription_price,
            CASE content_subscription_status
                WHEN 'active' THEN 'enabled'
                WHEN 'in_trial' THEN 'free_trial'
                WHEN 'cancelled' THEN 'cancelled'
                WHEN 'non_renewing' THEN 'non_renewing'
                WHEN 'disabled' THEN 'disabled'
                WHEN 'paused' THEN 'paused'
                ELSE 'unknown'
            END AS subscription_status,
            TIMESTAMP_SECONDS(content_customer_updated_at) as updated_at
        from chargebee_webhook_events.subscription_activated
        where safe_cast(content_subscription_due_invoices_count as INT64) = 0
            union all
        /*                                        */
        /*  CUSTOMER PRODUCT FREE TRIAL CONVERTED */
        /*                DUNNING                 */
        /*                                        */
        select
            safe_cast(a.`timestamp` as TIMESTAMP) as `timestamp`,
            safe_cast(a.id as STRING) as id,
            safe_cast(a.content_customer_id as STRING) as customer_id,
            safe_cast(a.content_subscription_id as STRING) as subscription_id,
            'customer_product_free_trial_converted' as event,
            safe_cast(null as STRING) as campaign,
            safe_cast(null as STRING) as city,
            safe_cast(a.content_customer_billing_address_country as STRING) as country,
            safe_cast(null as STRING) as coupon_code,
            TIMESTAMP_SECONDS(a.content_customer_created_at) as created_at,
            safe_cast(a.content_customer_email as STRING) as email,
            safe_cast(a.content_card_first_name as STRING) as first_name,
            safe_cast(a.content_card_last_name as STRING) as last_name,
            TIMESTAMP_SECONDS(a.content_invoice_paid_at) as last_payment_date,
            safe_cast(a.content_customer_cs_marketing_opt_in as BOOLEAN) as marketing_opt_in,
            concat(safe_cast(a.content_card_first_name as STRING), ' ', safe_cast(a.content_card_last_name as STRING)) as name,
            TIMESTAMP_SECONDS(a.content_subscription_next_billing_at) as next_payment_date,
            coalesce(
                safe_cast(a.content_subscription_subscription_items_0_item_price_id as STRING),
                safe_cast(json_extract_scalar(json_extract(json_extract(a.content_subscription_subscription_items, '$[0]'), '$.item_price_id')) as STRING)
            ) as plan,
            safe_cast(a.content_subscription_channel as STRING) as platform,
            safe_cast(null as STRING) as promotion_code,
            safe_cast(null as STRING) as referrer,
            safe_cast(a.content_customer_billing_address_state_code as STRING) as region,
            safe_cast(null as BOOLEAN) as registered_to_site,
            'chargebee' as source,
            safe_cast(null as BOOLEAN) as subscribed_to_site,
            CASE a.content_subscription_billing_period_unit
                WHEN 'month' THEN 'monthly'
                WHEN 'year' THEN 'yearly'
                ELSE safe_cast(a.content_subscription_billing_period_unit as STRING)
            END AS subscription_frequency,
            coalesce(
                safe_cast(a.content_subscription_subscription_items_0_unit_price as INT64),
                safe_cast(json_extract_scalar(json_extract(json_extract(a.content_subscription_subscription_items, '$[0]'), '$.unit_price')) as INT64)
            ) as subscription_price,
            CASE a.content_subscription_status
                WHEN 'active' THEN 'enabled'
                WHEN 'in_trial' THEN 'free_trial'
                WHEN 'cancelled' THEN 'cancelled'
                WHEN 'non_renewing' THEN 'non_renewing'
                WHEN 'disabled' THEN 'disabled'
                WHEN 'paused' THEN 'paused'
                ELSE 'unknown'
            END AS subscription_status,
            TIMESTAMP_SECONDS(a.content_customer_updated_at) as updated_at
        from chargebee_webhook_events.subscription_activated a
        inner join chargebee_webhook_events.payment_succeeded b
        on a.content_invoice_id = b.content_invoice_id
        where safe_cast(a.content_subscription_due_invoices_count as INT64) >= 1
        union all
        /*                                        */
        /*        CUSTOMER PRODUCT CREATED        */
        /*              REACQUISITION             */
        /*                                        */
        select
            safe_cast(`timestamp` as TIMESTAMP) as `timestamp`,
            safe_cast(_id as STRING) as id,
            safe_cast(content_customer_id as STRING) as customer_id,
            safe_cast(content_subscription_id as STRING) as subscription_id,
            'customer_product_created' as event,
            safe_cast(null as STRING) as campaign,
            safe_cast(null as STRING) as city,
            safe_cast(content_customer_billing_address_country as STRING) as country,
            safe_cast(null as STRING) as coupon_code,
            TIMESTAMP_SECONDS(content_customer_created_at) as created_at,
            safe_cast(content_customer_email as STRING) as email,
            safe_cast(content_card_first_name as STRING) as first_name,
            safe_cast(content_card_last_name as STRING) as last_name,
            TIMESTAMP_SECONDS(content_invoice_paid_at) as last_payment_date,
            safe_cast(content_customer_cs_marketing_opt_in as BOOLEAN) as marketing_opt_in,
            concat(safe_cast(content_card_first_name as STRING), ' ', safe_cast(content_card_last_name as STRING)) as name,
            TIMESTAMP_SECONDS(content_subscription_next_billing_at) as next_payment_date,
            coalesce(
                safe_cast(content_subscription_subscription_items_0_item_price_id as STRING),
                safe_cast(json_extract_scalar(json_extract(json_extract(content_subscription_subscription_items, '$[0]'), '$.item_price_id')) as STRING)
            ) as plan,
            safe_cast(content_subscription_channel as STRING) as platform,
            safe_cast(null as STRING) as promotion_code,
            safe_cast(null as STRING) as referrer,
            safe_cast(content_customer_billing_address_state_code as STRING) as region,
            safe_cast(null as BOOLEAN) as registered_to_site,
            'chargebee' as source,
            safe_cast(null as BOOLEAN) as subscribed_to_site,
            CASE content_subscription_billing_period_unit
                WHEN 'month' THEN 'monthly'
                WHEN 'year' THEN 'yearly'
                ELSE safe_cast(content_subscription_billing_period_unit as STRING)
            END AS subscription_frequency,
            coalesce(
                safe_cast(content_subscription_subscription_items_0_unit_price as INT64),
                safe_cast(json_extract_scalar(json_extract(json_extract(content_subscription_subscription_items, '$[0]'), '$.unit_price')) as INT64)
            ) as subscription_price,
            CASE content_subscription_status
                WHEN 'active' THEN 'enabled'
                WHEN 'in_trial' THEN 'free_trial'
                WHEN 'cancelled' THEN 'cancelled'
                WHEN 'non_renewing' THEN 'non_renewing'
                WHEN 'disabled' THEN 'disabled'
                WHEN 'paused' THEN 'paused'
                ELSE 'unknown'
            END AS subscription_status,
            TIMESTAMP_SECONDS(content_customer_updated_at) as updated_at
        from chargebee_webhook_events.subscription_reactivated
        union all
        /*                                        */
        /*        CUSTOMER PRODUCT RENEWED        */
        /*                                        */
        select
            safe_cast(`timestamp` as TIMESTAMP) as `timestamp`,
            safe_cast(_id as STRING) as id,
            safe_cast(content_customer_id as STRING) as customer_id,
            safe_cast(content_subscription_id as STRING) as subscription_id,
            'customer_product_renewed' as event,
            safe_cast(null as STRING) as campaign,
            safe_cast(null as STRING) as city,
            safe_cast(content_customer_billing_address_country as STRING) as country,
            safe_cast(null as STRING) as coupon_code,
            TIMESTAMP_SECONDS(content_customer_created_at) as created_at,
            safe_cast(content_customer_email as STRING) as email,
            safe_cast(content_card_first_name as STRING) as first_name,
            safe_cast(content_card_last_name as STRING) as last_name,
            TIMESTAMP_SECONDS(content_invoice_paid_at) as last_payment_date,
            safe_cast(content_customer_cs_marketing_opt_in as BOOLEAN) as marketing_opt_in,
            concat(safe_cast(content_card_first_name as STRING), ' ', safe_cast(content_card_last_name as STRING)) as name,
            TIMESTAMP_SECONDS(content_subscription_next_billing_at) as next_payment_date,
            coalesce(
                safe_cast(content_subscription_subscription_items_0_item_price_id as STRING),
                safe_cast(json_extract_scalar(json_extract(json_extract(content_subscription_subscription_items, '$[0]'), '$.item_price_id')) as STRING)
            ) as plan,
            safe_cast(content_subscription_channel as STRING) as platform,
            safe_cast(null as STRING) as promotion_code,
            safe_cast(null as STRING) as referrer,
            safe_cast(content_customer_billing_address_state_code as STRING) as region,
            safe_cast(null as BOOLEAN) as registered_to_site,
            'chargebee' as source,
            safe_cast(null as BOOLEAN) as subscribed_to_site,
            CASE content_subscription_billing_period_unit
                WHEN 'month' THEN 'monthly'
                WHEN 'year' THEN 'yearly'
                ELSE safe_cast(content_subscription_billing_period_unit as STRING)
            END AS subscription_frequency,
            coalesce(
                safe_cast(content_subscription_subscription_items_0_unit_price as INT64),
                safe_cast(json_extract_scalar(json_extract(json_extract(content_subscription_subscription_items, '$[0]'), '$.unit_price')) as INT64)
            ) as subscription_price,
            CASE content_subscription_status
                WHEN 'active' THEN 'enabled'
                WHEN 'in_trial' THEN 'free_trial'
                WHEN 'cancelled' THEN 'cancelled'
                WHEN 'non_renewing' THEN 'non_renewing'
                WHEN 'disabled' THEN 'disabled'
                WHEN 'paused' THEN 'paused'
                ELSE 'unknown'
            END AS subscription_status,
            TIMESTAMP_SECONDS(content_customer_updated_at) as updated_at
        from chargebee_webhook_events.subscription_renewed
            union all
        /*                                        */
        /*      CUSTOMER PRODUCT CANCELLED        */
        /*  CUSTOMER PRODUCT FREE TRIAL EXPIRED   */
        /*                                        */
        select
            safe_cast(`timestamp` as TIMESTAMP) as `timestamp`,
            safe_cast(_id as STRING) as id,
            safe_cast(content_customer_id as STRING) as customer_id,
            safe_cast(content_subscription_id as STRING) as subscription_id,
            CASE
                WHEN (content_subscription_cancelled_at - content_subscription_trial_end) < 10000 THEN 'customer_product_free_trial_expired'
                ELSE 'customer_product_cancelled'
            END AS event,
            safe_cast(null as STRING) as campaign,
            safe_cast(null as STRING) as city,
            safe_cast(content_customer_billing_address_country as STRING) as country,
            safe_cast(null as STRING) as coupon_code,
            TIMESTAMP_SECONDS(content_customer_created_at) as created_at,
            safe_cast(content_customer_email as STRING) as email,
            safe_cast(content_card_first_name as STRING) as first_name,
            safe_cast(content_card_last_name as STRING) as last_name,
            TIMESTAMP_SECONDS(content_subscription_current_term_start) as last_payment_date,
            safe_cast(content_customer_cs_marketing_opt_in as BOOLEAN) as marketing_opt_in,
            concat(safe_cast(content_card_first_name as STRING), ' ', safe_cast(content_card_last_name as STRING)) as name,
            safe_cast(null as TIMESTAMP) as next_payment_date,
            coalesce(
                safe_cast(content_subscription_subscription_items_0_item_price_id as STRING),
                safe_cast(json_extract_scalar(json_extract(json_extract(content_subscription_subscription_items, '$[0]'), '$.item_price_id')) as STRING)
            ) as plan,
            safe_cast(content_subscription_channel as STRING) as platform,
            safe_cast(null as STRING) as promotion_code,
            safe_cast(null as STRING) as referrer,
            safe_cast(content_customer_billing_address_state_code as STRING) as region,
            safe_cast(null as BOOLEAN) as registered_to_site,
            'chargebee' as source,
            safe_cast(null as BOOLEAN) as subscribed_to_site,
            CASE content_subscription_billing_period_unit
                WHEN 'month' THEN 'monthly'
                WHEN 'year' THEN 'yearly'
                ELSE safe_cast(content_subscription_billing_period_unit as STRING)
            END AS subscription_frequency,
            coalesce(
                safe_cast(content_subscription_subscription_items_0_unit_price as INT64),
                safe_cast(json_extract_scalar(json_extract(json_extract(content_subscription_subscription_items, '$[0]'), '$.unit_price')) as INT64)
            ) as subscription_price,
            CASE content_subscription_status
                WHEN 'active' THEN 'enabled'
                WHEN 'in_trial' THEN 'free_trial'
                WHEN 'cancelled' THEN 'cancelled'
                WHEN 'non_renewing' THEN 'non_renewing'
                WHEN 'disabled' THEN 'disabled'
                WHEN 'paused' THEN 'paused'
                ELSE 'unknown'
            END AS subscription_status,
            TIMESTAMP_SECONDS(content_customer_updated_at) as updated_at
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
            safe_cast(`timestamp` as TIMESTAMP) as `timestamp`,
            safe_cast(_id as STRING) as id,
            safe_cast(content_customer_id as STRING) as customer_id,
            safe_cast(content_subscription_id as STRING) as subscription_id,
            CASE
                WHEN (content_subscription_cancelled_at - content_subscription_activated_at) < 2419200 THEN 'customer_product_free_trial_expired'
                ELSE 'customer_product_cancelled'
            END AS event,
            safe_cast(null as STRING) as campaign,
            safe_cast(null as STRING) as city,
            safe_cast(content_customer_billing_address_country as STRING) as country,
            safe_cast(null as STRING) as coupon_code,
            TIMESTAMP_SECONDS(content_customer_created_at) as created_at,
            safe_cast(content_customer_email as STRING) as email,
            safe_cast(content_card_first_name as STRING) as first_name,
            safe_cast(content_card_last_name as STRING) as last_name,
            TIMESTAMP_SECONDS(content_subscription_current_term_start) as last_payment_date,
            safe_cast(content_customer_cs_marketing_opt_in as BOOLEAN) as marketing_opt_in,
            concat(safe_cast(content_card_first_name as STRING), ' ', safe_cast(content_card_last_name as STRING)) as name,
            safe_cast(null as TIMESTAMP) as next_payment_date,
            coalesce(
                safe_cast(content_subscription_subscription_items_0_item_price_id as STRING),
                safe_cast(json_extract_scalar(json_extract(json_extract(content_subscription_subscription_items, '$[0]'), '$.item_price_id')) as STRING)
            ) as plan,
            safe_cast(content_subscription_channel as STRING) as platform,
            safe_cast(null as STRING) as promotion_code,
            safe_cast(null as STRING) as referrer,
            safe_cast(content_customer_billing_address_state_code as STRING) as region,
            safe_cast(null as BOOLEAN) as registered_to_site,
            'chargebee' as source,
            safe_cast(null as BOOLEAN) as subscribed_to_site,
            CASE content_subscription_billing_period_unit
                WHEN 'month' THEN 'monthly'
                WHEN 'year' THEN 'yearly'
                ELSE safe_cast(content_subscription_billing_period_unit as STRING)
            END AS subscription_frequency,
            coalesce(
                safe_cast(content_subscription_subscription_items_0_unit_price as INT64),
                safe_cast(json_extract_scalar(json_extract(json_extract(content_subscription_subscription_items, '$[0]'), '$.unit_price')) as INT64)
            ) as subscription_price,
            'expired' as subscription_status,
            TIMESTAMP_SECONDS(content_customer_updated_at) as updated_at
        from chargebee_webhook_events.subscription_cancelled
        where content_subscription_cancel_reason_code in ('Not Paid', 'No Card', 'Fraud Review Failed', 'Non Compliant EU Customer', 'Tax Calculation Failed', 'Currency incompatible with Gateway', 'Non Compliant Customer')
            union all
        /*                                        */
        /*        CUSTOMER PRODUCT PAUSED         */
        /*                                        */
        select
            safe_cast(`timestamp` as TIMESTAMP) as `timestamp`,
            safe_cast(_id as STRING) as id,
            safe_cast(content_customer_id as STRING) as customer_id,
            safe_cast(content_subscription_id as STRING) as subscription_id,
            'customer_product_paused' as event,
            safe_cast(null as STRING) as campaign,
            safe_cast(null as STRING) as city,
            safe_cast(content_customer_billing_address_country as STRING) as country,
            safe_cast(null as STRING) as coupon_code,
            TIMESTAMP_SECONDS(content_customer_created_at) as created_at,
            safe_cast(content_customer_email as STRING) as email,
            safe_cast(content_card_first_name as STRING) as first_name,
            safe_cast(content_card_last_name as STRING) as last_name,
            TIMESTAMP_SECONDS(content_subscription_current_term_start) as last_payment_date,
            safe_cast(content_customer_cs_marketing_opt_in as BOOLEAN) as marketing_opt_in,
            concat(safe_cast(content_card_first_name as STRING), ' ', safe_cast(content_card_last_name as STRING)) as name,
            safe_cast(null as TIMESTAMP) as next_payment_date,
            coalesce(
                safe_cast(content_subscription_subscription_items_0_item_price_id as STRING),
                safe_cast(json_extract_scalar(json_extract(json_extract(content_subscription_subscription_items, '$[0]'), '$.item_price_id')) as STRING)
            ) as plan,
            safe_cast(content_subscription_channel as STRING) as platform,
            safe_cast(null as STRING) as promotion_code,
            safe_cast(null as STRING) as referrer,
            safe_cast(content_customer_billing_address_state_code as STRING) as region,
            safe_cast(null as BOOLEAN) as registered_to_site,
            'chargebee' as source,
            safe_cast(null as BOOLEAN) as subscribed_to_site,
            CASE content_subscription_billing_period_unit
                WHEN 'month' THEN 'monthly'
                WHEN 'year' THEN 'yearly'
                ELSE safe_cast(content_subscription_billing_period_unit as STRING)
            END AS subscription_frequency,
            coalesce(
                safe_cast(content_subscription_subscription_items_0_unit_price as INT64),
                safe_cast(json_extract_scalar(json_extract(json_extract(content_subscription_subscription_items, '$[0]'), '$.unit_price')) as INT64)
            ) as subscription_price,
            CASE content_subscription_status
                WHEN 'active' THEN 'enabled'
                WHEN 'in_trial' THEN 'free_trial'
                WHEN 'cancelled' THEN 'cancelled'
                WHEN 'non_renewing' THEN 'non_renewing'
                WHEN 'disabled' THEN 'disabled'
                WHEN 'paused' THEN 'paused'
                ELSE 'unknown'
            END AS subscription_status,
            TIMESTAMP_SECONDS(content_customer_updated_at) as updated_at
        from chargebee_webhook_events.subscription_paused
        union all
        /*                                        */
        /*        CUSTOMER PRODUCT RESUMED        */
        /*                                        */
        select
            safe_cast(`timestamp` as TIMESTAMP) as `timestamp`,
            safe_cast(_id as STRING) as id,
            safe_cast(content_customer_id as STRING) as customer_id,
            safe_cast(content_subscription_id as STRING) as subscription_id,
            'customer_product_resumed' as event,
            safe_cast(null as STRING) as campaign,
            safe_cast(null as STRING) as city,
            safe_cast(content_customer_billing_address_country as STRING) as country,
            safe_cast(null as STRING) as coupon_code,
            TIMESTAMP_SECONDS(content_customer_created_at) as created_at,
            safe_cast(content_customer_email as STRING) as email,
            safe_cast(content_card_first_name as STRING) as first_name,
            safe_cast(content_card_last_name as STRING) as last_name,
            TIMESTAMP_SECONDS(content_subscription_current_term_start) as last_payment_date,
            safe_cast(content_customer_cs_marketing_opt_in as BOOLEAN) as marketing_opt_in,
            concat(safe_cast(content_card_first_name as STRING), ' ', safe_cast(content_card_last_name as STRING)) as name,
            TIMESTAMP_SECONDS(content_subscription_next_billing_at) as next_payment_date,
            coalesce(
                safe_cast(content_subscription_subscription_items_0_item_price_id as STRING),
                safe_cast(json_extract_scalar(json_extract(json_extract(content_subscription_subscription_items, '$[0]'), '$.item_price_id')) as STRING)
            ) as plan,
            safe_cast(content_subscription_channel as STRING) as platform,
            safe_cast(null as STRING) as promotion_code,
            safe_cast(null as STRING) as referrer,
            safe_cast(content_customer_billing_address_state_code as STRING) as region,
            safe_cast(null as BOOLEAN) as registered_to_site,
            'chargebee' as source,
            safe_cast(null as BOOLEAN) as subscribed_to_site,
            CASE content_subscription_billing_period_unit
                WHEN 'month' THEN 'monthly'
                WHEN 'year' THEN 'yearly'
                ELSE safe_cast(content_subscription_billing_period_unit as STRING)
            END AS subscription_frequency,
            coalesce(
                safe_cast(content_subscription_subscription_items_0_unit_price as INT64),
                safe_cast(json_extract_scalar(json_extract(json_extract(content_subscription_subscription_items, '$[0]'), '$.unit_price')) as INT64)
            ) as subscription_price,
            CASE content_subscription_status
                WHEN 'active' THEN 'enabled'
                WHEN 'in_trial' THEN 'free_trial'
                WHEN 'cancelled' THEN 'cancelled'
                WHEN 'non_renewing' THEN 'non_renewing'
                WHEN 'disabled' THEN 'disabled'
                WHEN 'paused' THEN 'paused'
                ELSE 'unknown'
            END AS subscription_status,
            TIMESTAMP_SECONDS(content_customer_updated_at) as updated_at
        from chargebee_webhook_events.subscription_resumed
        union all
        /*                                        */
        /*      CUSTOMER PRODUCT CHARGE FAILED    */
        /*                                        */
        select
            safe_cast(`timestamp` as TIMESTAMP) as `timestamp`,
            safe_cast(_id as STRING) as id,
            safe_cast(content_customer_id as STRING) as customer_id,
            safe_cast(content_subscription_id as STRING) as subscription_id,
            'customer_product_charge_failed' as event,
            safe_cast(null as STRING) as campaign,
            safe_cast(null as STRING) as city,
            safe_cast(content_customer_billing_address_country as STRING) as country,
            safe_cast(null as STRING) as coupon_code,
            TIMESTAMP_SECONDS(content_customer_created_at) as created_at,
            safe_cast(content_customer_email as STRING) as email,
            safe_cast(content_card_first_name as STRING) as first_name,
            safe_cast(content_card_last_name as STRING) as last_name,
            TIMESTAMP_SECONDS(content_transaction_updated_at) as last_payment_date,
            safe_cast(content_customer_cs_marketing_opt_in as BOOLEAN) as marketing_opt_in,
            concat(safe_cast(content_card_first_name as STRING), ' ', safe_cast(content_card_last_name as STRING)) as name,
            TIMESTAMP_SECONDS(content_subscription_next_billing_at) as next_payment_date,
            coalesce(
                safe_cast(content_subscription_subscription_items_0_item_price_id as STRING),
                safe_cast(json_extract_scalar(json_extract(json_extract(content_subscription_subscription_items, '$[0]'), '$.item_price_id')) as STRING)
            ) as plan,
            safe_cast(content_subscription_channel as STRING) as platform,
            safe_cast(null as STRING) as promotion_code,
            safe_cast(null as STRING) as referrer,
            safe_cast(content_customer_billing_address_state_code as STRING) as region,
            safe_cast(null as BOOLEAN) as registered_to_site,
            'chargebee' as source,
            safe_cast(null as BOOLEAN) as subscribed_to_site,
            CASE content_subscription_billing_period_unit
                WHEN 'month' THEN 'monthly'
                WHEN 'year' THEN 'yearly'
                ELSE safe_cast(content_subscription_billing_period_unit as STRING)
            END AS subscription_frequency,
            coalesce(
                safe_cast(content_subscription_subscription_items_0_unit_price as INT64),
                safe_cast(json_extract_scalar(json_extract(json_extract(content_subscription_subscription_items, '$[0]'), '$.unit_price')) as INT64)
            ) as subscription_price,
            CASE content_subscription_status
                WHEN 'active' THEN 'enabled'
                WHEN 'in_trial' THEN 'free_trial'
                WHEN 'cancelled' THEN 'cancelled'
                WHEN 'non_renewing' THEN 'non_renewing'
                WHEN 'disabled' THEN 'disabled'
                WHEN 'paused' THEN 'paused'
                ELSE 'unknown'
            END AS subscription_status,
            TIMESTAMP_SECONDS(content_customer_updated_at) as updated_at
        from chargebee_webhook_events.payment_failed
            union all
        /*                                        */
        /*    CUSTOMER PRODUCT SET CANCELLATION   */
        /*                                        */
        select
            safe_cast(`timestamp` as TIMESTAMP) as `timestamp`,
            safe_cast(_id as STRING) as id,
            safe_cast(content_customer_id as STRING) as customer_id,
            safe_cast(content_subscription_id as STRING) as subscription_id,
            'customer_product_set_cancellation' as event,
            safe_cast(null as STRING) as campaign,
            safe_cast(null as STRING) as city,
            safe_cast(content_customer_billing_address_country as STRING) as country,
            safe_cast(null as STRING) as coupon_code,
            TIMESTAMP_SECONDS(content_customer_created_at) as created_at,
            safe_cast(content_customer_email as STRING) as email,
            safe_cast(content_card_first_name as STRING) as first_name,
            safe_cast(content_card_last_name as STRING) as last_name,
            TIMESTAMP_SECONDS(content_subscription_current_term_start) as last_payment_date,
            safe_cast(content_customer_cs_marketing_opt_in as BOOLEAN) as marketing_opt_in,
            concat(safe_cast(content_card_first_name as STRING), ' ', safe_cast(content_card_last_name as STRING)) as name,
            safe_cast(null as TIMESTAMP) as next_payment_date,
            coalesce(
                safe_cast(content_subscription_subscription_items_0_item_price_id as STRING),
                safe_cast(json_extract_scalar(json_extract(json_extract(content_subscription_subscription_items, '$[0]'), '$.item_price_id')) as STRING)
            ) as plan,
            safe_cast(content_subscription_channel as STRING) as platform,
            safe_cast(null as STRING) as promotion_code,
            safe_cast(null as STRING) as referrer,
            safe_cast(content_customer_billing_address_state_code as STRING) as region,
            safe_cast(null as BOOLEAN) as registered_to_site,
            'chargebee' as source,
            safe_cast(null as BOOLEAN) as subscribed_to_site,
            CASE content_subscription_billing_period_unit
                WHEN 'month' THEN 'monthly'
                WHEN 'year' THEN 'yearly'
                ELSE safe_cast(content_subscription_billing_period_unit as STRING)
            END AS subscription_frequency,
            coalesce(
                safe_cast(content_subscription_subscription_items_0_unit_price as INT64),
                safe_cast(json_extract_scalar(json_extract(json_extract(content_subscription_subscription_items, '$[0]'), '$.unit_price')) as INT64)
            ) as subscription_price,
            CASE content_subscription_status
                WHEN 'active' THEN 'enabled'
                WHEN 'in_trial' THEN 'free_trial'
                WHEN 'cancelled' THEN 'cancelled'
                WHEN 'non_renewing' THEN 'non_renewing'
                WHEN 'disabled' THEN 'disabled'
                WHEN 'paused' THEN 'paused'
                ELSE 'unknown'
            END AS subscription_status,
            TIMESTAMP_SECONDS(content_customer_updated_at) as updated_at
        from chargebee_webhook_events.subscription_cancellation_scheduled
        union all
        /*                                          */
        /*  CUSTOMER PRODUCT UNDO SET CANCELLATION  */
        /*                                          */
        select
            safe_cast(`timestamp` as TIMESTAMP) as `timestamp`,
            safe_cast(_id as STRING) as id,
            safe_cast(content_customer_id as STRING) as customer_id,
            safe_cast(content_subscription_id as STRING) as subscription_id,
            'customer_product_undo_set_cancellation' as event,
            safe_cast(null as STRING) as campaign,
            safe_cast(null as STRING) as city,
            safe_cast(content_customer_billing_address_country as STRING) as country,
            safe_cast(null as STRING) as coupon_code,
            TIMESTAMP_SECONDS(content_customer_created_at) as created_at,
            safe_cast(content_customer_email as STRING) as email,
            safe_cast(content_card_first_name as STRING) as first_name,
            safe_cast(content_card_last_name as STRING) as last_name,
            TIMESTAMP_SECONDS(content_subscription_current_term_start) as last_payment_date,
            safe_cast(content_customer_cs_marketing_opt_in as BOOLEAN) as marketing_opt_in,
            concat(safe_cast(content_card_first_name as STRING), ' ', safe_cast(content_card_last_name as STRING)) as name,
            TIMESTAMP_SECONDS(content_subscription_next_billing_at) as next_payment_date,
            coalesce(
                safe_cast(content_subscription_subscription_items_0_item_price_id as STRING),
                safe_cast(json_extract_scalar(json_extract(json_extract(content_subscription_subscription_items, '$[0]'), '$.item_price_id')) as STRING)
            ) as plan,
            safe_cast(content_subscription_channel as STRING) as platform,
            safe_cast(null as STRING) as promotion_code,
            safe_cast(null as STRING) as referrer,
            safe_cast(content_customer_billing_address_state_code as STRING) as region,
            safe_cast(null as BOOLEAN) as registered_to_site,
            'chargebee' as source,
            safe_cast(null as BOOLEAN) as subscribed_to_site,
            CASE content_subscription_billing_period_unit
                WHEN 'month' THEN 'monthly'
                WHEN 'year' THEN 'yearly'
                ELSE safe_cast(content_subscription_billing_period_unit as STRING)
            END AS subscription_frequency,
            coalesce(
                safe_cast(content_subscription_subscription_items_0_unit_price as INT64),
                safe_cast(json_extract_scalar(json_extract(json_extract(content_subscription_subscription_items, '$[0]'), '$.unit_price')) as INT64)
            ) as subscription_price,
            CASE content_subscription_status
                WHEN 'active' THEN 'enabled'
                WHEN 'in_trial' THEN 'free_trial'
                WHEN 'cancelled' THEN 'cancelled'
                WHEN 'non_renewing' THEN 'non_renewing'
                WHEN 'disabled' THEN 'disabled'
                WHEN 'paused' THEN 'paused'
                ELSE 'unknown'
            END AS subscription_status,
            TIMESTAMP_SECONDS(content_customer_updated_at) as updated_at
        from chargebee_webhook_events.subscription_scheduled_cancellation_removed
        union all
        /*                                        */
        /*      CUSTOMER PRODUCT SET PAUSED       */
        /*                                        */
        select
            safe_cast(`timestamp` as TIMESTAMP) as `timestamp`,
            safe_cast(_id as STRING) as id,
            safe_cast(content_customer_id as STRING) as customer_id,
            safe_cast(content_subscription_id as STRING) as subscription_id,
            'customer_product_set_paused' as event,
            safe_cast(null as STRING) as campaign,
            safe_cast(null as STRING) as city,
            safe_cast(content_customer_billing_address_country as STRING) as country,
            safe_cast(null as STRING) as coupon_code,
            TIMESTAMP_SECONDS(content_customer_created_at) as created_at,
            safe_cast(content_customer_email as STRING) as email,
            safe_cast(content_card_first_name as STRING) as first_name,
            safe_cast(content_card_last_name as STRING) as last_name,
            TIMESTAMP_SECONDS(content_subscription_current_term_start) as last_payment_date,
            safe_cast(content_customer_cs_marketing_opt_in as BOOLEAN) as marketing_opt_in,
            concat(safe_cast(content_card_first_name as STRING), ' ', safe_cast(content_card_last_name as STRING)) as name,
            TIMESTAMP_SECONDS(content_subscription_next_billing_at) as next_payment_date,
            coalesce(
                safe_cast(content_subscription_subscription_items_0_item_price_id as STRING),
                safe_cast(json_extract_scalar(json_extract(json_extract(content_subscription_subscription_items, '$[0]'), '$.item_price_id')) as STRING)
            ) as plan,
            safe_cast(content_subscription_channel as STRING) as platform,
            safe_cast(null as STRING) as promotion_code,
            safe_cast(null as STRING) as referrer,
            safe_cast(content_customer_billing_address_state_code as STRING) as region,
            safe_cast(null as BOOLEAN) as registered_to_site,
            'chargebee' as source,
            safe_cast(null as BOOLEAN) as subscribed_to_site,
            CASE content_subscription_billing_period_unit
                WHEN 'month' THEN 'monthly'
                WHEN 'year' THEN 'yearly'
                ELSE safe_cast(content_subscription_billing_period_unit as STRING)
            END AS subscription_frequency,
            coalesce(
                safe_cast(content_subscription_subscription_items_0_unit_price as INT64),
                safe_cast(json_extract_scalar(json_extract(json_extract(content_subscription_subscription_items, '$[0]'), '$.unit_price')) as INT64)
            ) as subscription_price,
            CASE content_subscription_status
                WHEN 'active' THEN 'enabled'
                WHEN 'in_trial' THEN 'free_trial'
                WHEN 'cancelled' THEN 'cancelled'
                WHEN 'non_renewing' THEN 'non_renewing'
                WHEN 'disabled' THEN 'disabled'
                WHEN 'paused' THEN 'paused'
                ELSE 'unknown'
            END AS subscription_status,
            TIMESTAMP_SECONDS(content_customer_updated_at) as updated_at
        from chargebee_webhook_events.subscription_pause_scheduled
        union all
        /*                                        */
        /*      CUSTOMER PRODUCT SET PAUSED       */
        /*                                        */
        select
            safe_cast(`timestamp` as TIMESTAMP) as `timestamp`,
            safe_cast(_id as STRING) as id,
            safe_cast(content_customer_id as STRING) as customer_id,
            safe_cast(content_subscription_id as STRING) as subscription_id,
            'customer_product_undo_set_paused' as event,
            safe_cast(null as STRING) as campaign,
            safe_cast(null as STRING) as city,
            safe_cast(content_customer_billing_address_country as STRING) as country,
            safe_cast(null as STRING) as coupon_code,
            TIMESTAMP_SECONDS(content_customer_created_at) as created_at,
            safe_cast(content_customer_email as STRING) as email,
            safe_cast(content_card_first_name as STRING) as first_name,
            safe_cast(content_card_last_name as STRING) as last_name,
            TIMESTAMP_SECONDS(content_subscription_current_term_start) as last_payment_date,
            safe_cast(content_customer_cs_marketing_opt_in as BOOLEAN) as marketing_opt_in,
            concat(safe_cast(content_card_first_name as STRING), ' ', safe_cast(content_card_last_name as STRING)) as name,
            TIMESTAMP_SECONDS(content_subscription_next_billing_at) as next_payment_date,
            coalesce(
                safe_cast(content_subscription_subscription_items_0_item_price_id as STRING),
                safe_cast(json_extract_scalar(json_extract(json_extract(content_subscription_subscription_items, '$[0]'), '$.item_price_id')) as STRING)
            ) as plan,
            safe_cast(content_subscription_channel as STRING) as platform,
            safe_cast(null as STRING) as promotion_code,
            safe_cast(null as STRING) as referrer,
            safe_cast(content_customer_billing_address_state_code as STRING) as region,
            safe_cast(null as BOOLEAN) as registered_to_site,
            'chargebee' as source,
            safe_cast(null as BOOLEAN) as subscribed_to_site,
            CASE content_subscription_billing_period_unit
                WHEN 'month' THEN 'monthly'
                WHEN 'year' THEN 'yearly'
                ELSE safe_cast(content_subscription_billing_period_unit as STRING)
            END AS subscription_frequency,
            coalesce(
                safe_cast(content_subscription_subscription_items_0_unit_price as INT64),
                safe_cast(json_extract_scalar(json_extract(json_extract(content_subscription_subscription_items, '$[0]'), '$.unit_price')) as INT64)
            ) as subscription_price,
            CASE content_subscription_status
                WHEN 'active' THEN 'enabled'
                WHEN 'in_trial' THEN 'free_trial'
                WHEN 'cancelled' THEN 'cancelled'
                WHEN 'non_renewing' THEN 'non_renewing'
                WHEN 'disabled' THEN 'disabled'
                WHEN 'paused' THEN 'paused'
                ELSE 'unknown'
            END AS subscription_status,
            TIMESTAMP_SECONDS(content_customer_updated_at) as updated_at
        from chargebee_webhook_events.subscription_scheduled_pause_removed
    )
    , distinct_events as (
      select * from (select *, row_number() over (partition by id order by "timestamp") as rn from event_mapping) where rn = 1
    )
    , id_mapping as (
      select *
      , ott_user_id as user_id
      from ${chargebee_vimeo_ott_id_mapping.SQL_TABLE_NAME}
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
