view: customer_info_v2 {
  derived_table:{
  sql: with chargebee_info as (
      select
        content_customer_id as chargebee_id
        ,content_customer_phone as phone_number
        ,content_customer_email as email
        ,content_customer_first_name as first_name
        ,content_customer_last_name as last_name
        ,'https://upentertainment.chargebee.com/d/customers/'||content_invoice_customer_id::varchar as chargebee_link
      from chargebee_webhook_events.subscription_created
      where {% condition email_filter %} email {% endcondition %}


    ),

    chargebee_join_date as (
    SELECT
      content_customer_email as email
      ,min(date(timestamp)) as chargebee_join_date
    FROM chargebee_webhook_events.subscription_created
    where {% condition email_filter %} content_customer_email {% endcondition %}
    GROUP BY email
    ),

-- Get most recent 3 payment failure error code
   unique_errors AS (
    SELECT
        content_customer_email,
        content_transaction_error_detail_decline_code AS error_code,
        MAX("timestamp") AS last_occurred_at
    FROM chargebee_webhook_events.payment_failed
    WHERE content_transaction_error_detail_decline_code IS NOT NULL
    and {% condition email_filter %} content_customer_email {% endcondition %}
    GROUP BY 1, 2
),
ranked_errors AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY content_customer_email
            ORDER BY last_occurred_at DESC, error_code
        ) AS rn
    FROM unique_errors
),

chargebee_error_code as (
SELECT
    content_customer_email as email,
    LISTAGG(error_code, ', ')
        WITHIN GROUP (ORDER BY rn) AS error_codes
FROM ranked_errors
WHERE rn <= 3
GROUP BY content_customer_email),

--- end error codes

    chargebee_payments as (
    select
        lower(content_customer_email) as email
        ,content_transaction_amount / 100.0 as amount_paid
        ,timestamp 'epoch' + content_transaction_date * interval '1 second' as transaction_date
        ,content_customer_billing_address_zip as billing_zip
        ,content_card_billing_state as billing_state
        ,lpad(content_card_expiry_month::int::varchar, 2, '0') || '/' || content_card_expiry_year::int::varchar as cc_expiration
        ,content_card_last4 as cc_last4
        ,content_subscription_subscription_items
        ,date(timestamp) as report_date
        ,content_subscription_billing_period_unit as billing_period
        ,row_number() over (partition by lower(trim(content_customer_email)) order by timestamp desc) as rn
    from chargebee_webhook_events.payment_succeeded
    where {% condition email_filter %} content_customer_email {% endcondition %}

    ),

    chargebee_refunds as (
    select
      lower(content_customer_email) as email
      ,(content_invoice_total * -1)/100.0 as amount_paid
      ,date(timestamp) as report_date
    from chargebee_webhook_events.payment_refunded
    where {% condition email_filter %} content_customer_email {% endcondition %}
    ),

    chargebee_most_recent_payment as (
    SELECT * from chargebee_payments
    WHERE rn = 1
    and content_subscription_subscription_items LIKE '%UP%'
    ),

     chargebee_status as (
    select
      customer_email
      ,subscription_status
      ,subscription_billing_period_unit as billing_period
    from http_api.chargebee_subscriptions
    where date(uploaded_at) = current_date -1
      and subscription_subscription_items_0_item_price_id LIKE '%UP%'
      and{% condition email_filter %} customer_email {% endcondition %}

    ),

    chargebee_cancel as (
    SELECT
      content_customer_email
      ,date(timestamp) as report_date
    FROM chargebee_webhook_events.subscription_cancelled
    where content_subscription_subscription_items LIKE '%UP%'
    and {% condition email_filter %} content_customer_email {% endcondition %}
    ),

    chargebee_latest_cancel as (
    select
        lower(trim(content_customer_email)) as email,
        max(report_date) as cancellation_date
    from chargebee_cancel
    group by 1
),
/*
chargebee_billing_dates as (
    select
        lower(trim(s.customer_email)) as email,
        s.subscription_status,
        s.billing_period,
        p.transaction_date as last_payment_date,

        case
            when s.subscription_status in ('cancelled', 'paused')
                then c.cancellation_date
            else null
        end as cancellation_date,

        case
            when s.subscription_status in ('non_renewing', 'active')
                then case
                    when s.billing_period = 'month'
                        then dateadd(month, 1, p.transaction_date)::date
                    when s.billing_period = 'year'
                        then dateadd(year, 1, p.transaction_date)::date
                end
            else null
        end as next_billing_date

    from chargebee_status s
    left join chargebee_most_recent_payment p
        on lower(trim(s.customer_email)) = lower(trim(p.email))
    left join chargebee_latest_cancel c
        on lower(trim(s.customer_email)) = c.email
),
*/
  vimeo_payment_history as (
  select
    email
    ,date(event_occurred_at) as report_date
  from customers.new_customers
  where subscription_frequency != 'custom' and event_type in ( 'Direct to Paid', 'Free Trial to Paid')
  and {% condition email_filter %} email {% endcondition %}

  UNION ALL
  select
    email
    ,date(timestamp) as report_date
  from vimeo_ott_webhook.customer_product_renewed
  where {% condition email_filter %} email {% endcondition %}
),

payment_history AS (
    SELECT
        LOWER(TRIM(email)) AS email,
        transaction_date::DATE AS payment_date
    FROM chargebee_payments

    UNION ALL

    SELECT
        LOWER(TRIM(email)) AS email,
        report_date AS payment_date
    FROM vimeo_payment_history
),
latest_payment AS (
    SELECT
        email,
        MAX(payment_date) AS last_payment_date
    FROM payment_history
    GROUP BY email
),


    chargebee_totals as (
    select
        email
        ,sum(amount_paid) as total_net_payments
    from
      (select
        email
        ,amount_paid
      from chargebee_payments
      union all
      select
        email
        ,amount_paid
      from chargebee_refunds)
    group by email
    ),
  chargebee_payment_info as (
  select
      t.email
      ,t.total_net_payments
      ,p.transaction_date as last_successful_billing_date
      ,p.billing_zip
      ,p.billing_state
      ,p.cc_expiration
      ,p.cc_last4
  from chargebee_totals t
  left join chargebee_most_recent_payment p
      on t.email = p.email
    ),

    vimeo_info as (
    select
      first_name
      ,last_name
      ,user_id
      ,email
      ,created_at as customer_created_at

      ,'https://upfaithandfamily.vhx.tv/admin/manage/customers/'||user_id::varchar||'/overview' as vimeo_ott_link
  from vimeo_ott_webhook.customer_created
  where {% condition email_filter %} email {% endcondition %}
    ),

    sign_ins AS (
    SELECT user_email AS email, DATE(timestamp) AS report_date,
           'Amazon_Fire_TV' AS platform
    FROM amazon_fire_tv.sign_in_complete
    where {% condition email_filter %} user_email {% endcondition %}

    UNION ALL

    SELECT user_email, DATE(timestamp), 'Android'
    FROM android.sign_in_complete
    where {% condition email_filter %} user_email {% endcondition %}
    UNION ALL

    SELECT user_email, DATE(timestamp), 'Roku'
    FROM roku.sign_in_complete
    where {% condition email_filter %} user_email {% endcondition %}
    UNION ALL

    SELECT user_email, DATE(timestamp), 'Web'
    FROM javascript.sign_in_complete
    where {% condition email_filter %} user_email {% endcondition %}
    UNION ALL

    SELECT user_email, DATE(timestamp), 'iOS'
    FROM ios.sign_in_complete
    where {% condition email_filter %} user_email {% endcondition %}
    UNION ALL

    SELECT user_email, DATE(timestamp), 'Vizio_TV'
    FROM vizio_tv.sign_in_complete
    where {% condition email_filter %} user_email {% endcondition %}
),

recent_sign_in as (
SELECT
    LOWER(TRIM(email)) AS email,
    MAX(report_date) AS most_recent_sign_in_date,
    LISTAGG(DISTINCT REPLACE(platform, '_', ' '), ', ')
        WITHIN GROUP (ORDER BY REPLACE(platform, '_', ' ')) AS used_platforms
FROM sign_ins
GROUP BY 1
),

final as (

    SELECT
      v.user_id as vimeo_id
      ,c.chargebee_id
      ,v.email
      ,v.vimeo_ott_link
      ,c.chargebee_link
      ,c.phone_number
      ,COALESCE(c.first_name, v.first_name) AS first_name
      ,COALESCE(c.last_name, v.last_name) AS last_name
      ,cp.total_net_payments
      ,cp.last_successful_billing_date
      ,cp.billing_zip
      ,cp.billing_state
      ,cp.cc_expiration
      ,cp.cc_last4
      ,cec.error_codes
      ,lp.last_payment_date
      ,customer_created_at
      --,primary_platform
      ,cjd.chargebee_join_date
      ,clc.cancellation_date as cancel_date
      ,mrp.report_date as chargebee_most_recent_payment_date
      ,mrp.billing_period
      ,s.most_recent_sign_in_date
      ,s.used_platforms
    FROM vimeo_info v
    LEFT JOIN chargebee_info c
    on v.email = c.email
    LEFT JOIN chargebee_payment_info cp
    on v.email = cp.email
    LEFT JOIN chargebee_error_code cec
    on v.email = cec.email
    LEFT JOIN latest_payment lp
    on v.email = lp.email
    LEFT JOIN chargebee_join_date cjd
    on v.email = cjd.email
    LEFT JOIN chargebee_most_recent_payment mrp
    on v.email = mrp.email
    LEFT JOIN chargebee_latest_cancel clc
    on v.email = clc.email
    LEFT JOIN recent_sign_in s
    on v.email = s.email)

    select * ,
    CASE WHEN cancel_date > chargebee_most_recent_payment_date THEN cancel_date
    ELSE NULL
    END AS cancellation_date
    ,CASE
    WHEN cancel_date < chargebee_most_recent_payment_date THEN
        CASE
            WHEN billing_period = 'month'
                THEN DATEADD(month, 1, chargebee_most_recent_payment_date)::DATE
            WHEN billing_period = 'year'
                THEN DATEADD(year, 1, chargebee_most_recent_payment_date)::DATE
            ELSE NULL
        END
    ELSE NULL
END AS next_billing_date

    from final;;
    }
}
