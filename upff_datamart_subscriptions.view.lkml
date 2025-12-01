view: upff_datamart_subscriptions {
    derived_table: {
      sql: with

              /* vimeo customers from all platforms except web and api */
              vimeo_nonweb_customers AS
              (
              SELECT
                ROW_NUMBER() OVER () AS subscription_id
                , 'NULL' AS customer_id
                , user_id
                , CAST(report_date AS TIMESTAMP) AS received_at
                , DATE(customer_created_at) AS created_at
                , NULL AS activated_at
                , frequency
                , status
                , platform
                , marketing_opt_in
                , cast(NULL AS VARCHAR) AS city
                , cast(NULL AS VARCHAR) AS state
                , cast(NULL AS VARCHAR) AS zipcode
                , country
                , 'Vimeo' AS dsource
                , NULL AS amount
              FROM customers.all_customers
              WHERE report_date = current_date
              AND platform NOT in ('web','api')
              ),

              /* chargebee customers from platforms web and api */
              chargebee_web_api_users AS
              (
              SELECT
                cast(a.subscription_id AS INT) AS subscription_id
                , cast(a.customer_id AS VARCHAR) AS customer_id
                , cast(coalesce(b.user_id, NULL) AS INT) AS user_id
                , a.uploaded_at AS received_at
                , CAST(
                    CASE
                      WHEN CAST(a.customer_created_at AS BIGINT) >= 1000000000000
                      THEN DATEADD(millisecond, CAST(a.customer_created_at AS BIGINT), TIMESTAMP 'epoch')
                      ELSE DATEADD(second,      CAST(a.customer_created_at AS BIGINT), TIMESTAMP 'epoch')
                    END
                  AS DATE) AS created_at
                , a.subscription_started_at AS activated_at
                , CASE WHEN subscription_subscription_items_0_amount = 599 THEN 'monthly' ELSE 'yearly' END AS frequency
                , CASE
                    WHEN a.subscription_status = 'in_trial' THEN 'free_trial'
                    WHEN a.subscription_status = 'active' THEN 'enabled'
                    WHEN a.subscription_status = 'cancelled' THEN 'cancelled'
                    WHEN a.subscription_status = 'paused' THEN 'paused'
                    WHEN a.subscription_status = 'refunded' THEN 'refunded'
                  ELSE NULL END AS status
                , a.subscription_channel AS platform
                , a.customer_cs_marketing_opt_in AS marketing_opt_in
                , b.city
                , a.customer_billing_address_state AS state
                , a.customer_billing_address_zip AS zipcode
                , a.customer_billing_address_country AS country
                , 'Chargebee' AS dsource
                , a.subscription_subscription_items_0_amount AS amount
              FROM http_api.chargebee_subscriptions AS a
              LEFT JOIN ${upff_webhook_events.SQL_TABLE_NAME} AS b
              ON a.customer_id = b.customer_id
              AND a.subscription_subscription_items_0_item_price_id in ('UP-Faith-Family-Monthly','UP-Faith-Family-Yearly')
              ),

              /* union of all platforms */
              unionized_customer_data AS (SELECT * FROM vimeo_nonweb_customers UNION ALL SELECT * FROM chargebee_web_api_users),

              /* pulls in content_customer attributes from subscription_created event */
              customer_events AS
              (
              SELECT DISTINCT
                a.user_id AS id
                , a.subscription_id
                , a.created_at
                , b.timestamp AS received_at
                , a.activated_at
                , b.event
                , a.frequency
                , a.status
                , a.amount
              FROM unionized_customer_data AS a
              LEFT JOIN ${upff_webhook_events.SQL_TABLE_NAME} AS b
              ON a.user_id = b.user_id
              ),

              /* ranks event chronologically */
              ranked_events AS
              (
              SELECT
                *
                , ROW_NUMBER() OVER (PARTITION BY subscription_id ORDER BY received_at ASC) AS rn_first
                , ROW_NUMBER() OVER (PARTITION BY subscription_id ORDER BY received_at DESC) AS rn_last
              FROM customer_events
              ),

              /* orders ranked events */
              ranked_events_ordered AS
              (
              SELECT * FROM ranked_events
              WHERE rn_first = 1 OR rn_last = 1
              ORDER BY subscription_id, received_at
              ),

              /* labels ranked events */
              ranked_events_labeled AS
              (
              SELECT
                subscription_id
                , MAX(CASE WHEN rn_first = 1 THEN event END) AS first_event
                , MAX(CASE WHEN rn_last = 1 THEN event END) AS last_event
                , MAX(CASE WHEN rn_first = 1 THEN received_at END) AS first_event_time
                , MAX(CASE WHEN rn_last = 1 THEN received_at END) AS last_event_time
              FROM ranked_events_ordered
              GROUP BY subscription_id
              ),

              /* appends ranked events */
              customer_data_postjoin AS
              (
              SELECT
                a.*
                , b.first_event
                , b.last_event
                , b.first_event_time
                , b.last_event_time
              FROM customer_events AS a
              LEFT JOIN ranked_events_labeled AS b
              ON a.subscription_id = b.subscription_id
              ),

              /* creates lifecyle events */
              customer_lifetime AS
              (
              SELECT
                id AS user_id
                , subscription_id
                , MAX(received_at) AS received_at
                , MIN(CASE WHEN event = 'customer_product_free_trial_created' THEN received_at END) AS trial_start_date
                , MIN(CASE WHEN event = 'customer_product_free_trial_converted' THEN received_at END) AS active_start_date
                , MAX(CASE WHEN status IN ('free_trial_expired', 'customer_product_cancelled') THEN received_at END) AS active_end_date
                , DATEDIFF(month, CAST(first_event_time AS date), CAST(last_event_time AS DATE)) AS active_tenure_months
                , COUNT(DISTINCT CASE WHEN event = 'customer_product_renewed' THEN received_at END) AS total_payments
              FROM customer_data_postjoin
              GROUP BY 1,2,7
              ),

              /* subscriptions */
              subscriptions AS
              (
              SELECT
                a.id
                , a.id AS user_id
                , a.subscription_id
                , a.received_at
                , a.event
                , a.created_at
                , a.activated_at
                , b.active_end_date AS cancel_at
                , a.frequency
                , a.status
                , b.active_end_date AS trial_end_date
                , a.amount
              FROM customer_data_postjoin AS a
              LEFT JOIN customer_lifetime AS b
              ON a.id = b.user_id
              )

              select * from subscriptions ;;
    }

    measure: count {
      type: count
      drill_fields: [detail*]
    }

    dimension: id {
      type: number
      sql: ${TABLE}.id ;;
    }

    dimension: user_id {
      type: number
      sql: ${TABLE}.user_id ;;
      tags: ["user_id"]
    }

    dimension: subscription_id {
      type: string
      sql: ${TABLE}.subscription_id ;;
    }

    dimension_group: received_at {
      type: time
      sql: ${TABLE}.received_at ;;
    }

    dimension: event {
      type: string
      sql: ${TABLE}.event ;;
    }

    dimension_group: created_at {
      type: time
      sql: ${TABLE}.created_at ;;
    }

  dimension_group: activated_at {
    type: time
    sql: ${TABLE}.activated_at ;;
  }

    dimension_group: cancel_at {
      type: time
      sql: ${TABLE}.cancel_at ;;
    }

    dimension: frequency {
      type: string
      sql: ${TABLE}.frequency ;;
    }

    dimension: status {
      type: string
      sql: ${TABLE}.status ;;
    }

  dimension: amount {
    type: number
    sql: ${TABLE}.amount ;;
  }

    dimension_group: trial_end_date {
      type: time
      sql: ${TABLE}.trial_end_date ;;
    }

    set: detail {
      fields: [
        id,
        user_id,
        subscription_id,
        received_at_time,
        event,
        created_at_time,
        activated_at_time,
        cancel_at_time,
        frequency,
        status,
        trial_end_date_time,
        amount
      ]
    }
  }
