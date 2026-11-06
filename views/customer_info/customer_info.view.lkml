view: customer_info {
  derived_table: {
    sql:
    with chargebee_info as (
      select
        content_customer_id as chargebee_id
        ,content_customer_phone as phone_number
        ,content_customer_email as email
        ,content_customer_first_name as first_name
        ,content_customer_last_name as last_name
        ,'https://upentertainment.chargebee.com/d/customers/'||content_invoice_customer_id::varchar as chargebee_link
      from chargebee_webhook_events.subscription_created


    ),

    chargebee_join_date as (
    SELECT
      content_customer_email as email
      ,min(date(timestamp)) as chargebee_join_date
    FROM chargebee_webhook_events.subscription_created
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
        ,row_number() over (partition by lower(trim(content_customer_email)) order by timestamp desc) as rn
    from chargebee_webhook_events.payment_succeeded

    ),

    chargebee_refunds as (
    select
      lower(content_customer_email) as email
      ,(content_invoice_total * -1)/100.0 as amount_paid
      ,date(timestamp) as report_date
    from chargebee_webhook_events.payment_refunded
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

    ),

    chargebee_cancel as (
    SELECT
      content_customer_email
      ,date(timestamp) as report_date
    FROM chargebee_webhook_events.subscription_cancelled
    where content_subscription_subscription_items LIKE '%UP%'
    ),

    chargebee_latest_cancel as (
    select
        lower(trim(content_customer_email)) as email,
        max(report_date) as cancellation_date
    from chargebee_cancel
    group by 1
),

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

  vimeo_payment_history as (
  select
    email
    ,date(event_occurred_at) as report_date
  from customers.new_customers
  where subscription_frequency != 'custom' and event_type in ( 'Direct to Paid', 'Free Trial to Paid')

  UNION ALL
  select
    email
    ,date(timestamp) as report_date
  from vimeo_ott_webhook.customer_product_renewed
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
      ,customer_created_at
      ,CASE
        WHEN platform in ('api','web') THEN 'web'
        ELSE REPLACE(platform, '_', ' ')
      END AS primary_platform
      ,'https://upfaithandfamily.vhx.tv/admin/manage/customers/'||user_id::varchar||'/overview' as vimeo_ott_link
  from customers.all_customers where report_date = current_date -1 and action != 'follow'
    ),

    sign_ins AS (
    SELECT user_email AS email, DATE(timestamp) AS report_date,
           'Amazon_Fire_TV' AS platform
    FROM amazon_fire_tv.sign_in_complete

    UNION ALL

    SELECT user_email, DATE(timestamp), 'Android'
    FROM android.sign_in_complete

    UNION ALL

    SELECT user_email, DATE(timestamp), 'Roku'
    FROM roku.sign_in_complete

    UNION ALL

    SELECT user_email, DATE(timestamp), 'Web'
    FROM javascript.sign_in_complete

    UNION ALL

    SELECT user_email, DATE(timestamp), 'iOS'
    FROM ios.sign_in_complete

    UNION ALL

    SELECT user_email, DATE(timestamp), 'Vizio_TV'
    FROM vizio_tv.sign_in_complete
),

recent_sign_in as (
SELECT
    LOWER(TRIM(email)) AS email,
    MAX(report_date) AS most_recent_sign_in_date,
    LISTAGG(DISTINCT REPLACE(platform, '_', ' '), ', ')
        WITHIN GROUP (ORDER BY REPLACE(platform, '_', ' ')) AS used_platforms
FROM sign_ins
GROUP BY 1
)

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
      ,primary_platform
      ,cjd.chargebee_join_date
      ,cbd.next_billing_date
      ,cbd.cancellation_date
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
    LEFT JOIN chargebee_billing_dates cbd
    on v.email = cbd.email
    LEFT JOIN recent_sign_in s
    on v.email = s.email

    ;;
  }

  dimension: vimeo_id {
    type: string
    sql:  ${TABLE}.vimeo_id;;

    link: {
      label: "View Customer in Vimeo OTT"
      url: "https://upfaithandfamily.vhx.tv/admin/manage/customers/{{value}}/overview"
    }
  }

  dimension: chargebee_id {
    type: string
    sql:  ${TABLE}.chargebee_id;;

    link: {
      label: "View Customer in Chargbee"
      url: "https://upentertainment.chargebee.com/d/customers/{{ value }}"
    }

  }

  dimension: email {
    type: string
    sql:  ${TABLE}.email;;
  }

  dimension: first_name {
    type: string
    sql:  ${TABLE}.first_name;;
  }

  dimension: last_name {
    type: string
    sql:  ${TABLE}.last_name;;
  }
  dimension: error_codes {
    type: string
    sql:  ${TABLE}.error_codes;;
  }

  dimension: primary_platform {
    type: string
    sql:  ${TABLE}.primary_platform;;
  }

  dimension: last_payment_date {
    type: date
    sql:  ${TABLE}.last_payment_date;;
  }

  dimension: most_recent_sign_in_date {
    type: date
    sql:  ${TABLE}.most_recent_sign_in_date;;
  }

  dimension: chargebee_join_date {
    type: date
    sql:  ${TABLE}.chargebee_join_date;;
  }

  dimension: customer_created_at {
    type: date
    sql:  ${TABLE}.customer_created_at;;
  }

  dimension: cancellation_date {
    type: date
    sql:  ${TABLE}.cancellation_date;;
  }

  dimension: next_billing_date {
    type: date
    sql:  ${TABLE}.next_billing_date;;
  }

  dimension: used_platforms {
    type: string
    sql:  ${TABLE}.used_platforms;;
  }

  dimension: phone_number {
    type: string
    sql:  ${TABLE}.phone_number;;
  }

  dimension: total_net_payments {
    type: number
    label: "Total (Net) Payments"
    value_format_name: usd
    sql: ${TABLE}.total_net_payments ;;
  }

  dimension_group: last_successful_billing {
    type: time
    label: "Last Successful Billing"
    timeframes: [date]
    sql: ${TABLE}.last_successful_billing_date ;;
  }

  dimension: billing_zip {
    type: zipcode
    label: "Billing Address Zip"
    sql: ${TABLE}.billing_zip ;;
  }

  dimension: billing_state {
    type: string
    label: "Billing Address State"
    map_layer_name: us_states
    sql: ${TABLE}.billing_state ;;
  }

  dimension: cc_expiration {
    type: string
    label: "CC Expiration Date"
    sql: ${TABLE}.cc_expiration ;;
  }

  dimension: cc_last4 {
    type: string
    label: "CC Last 4"
    sql: ${TABLE}.cc_last4 ;;
  }



}
