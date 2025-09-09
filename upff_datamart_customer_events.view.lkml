view: upff_datamart_customer_events {
    derived_table: {
      sql: with

              /* vimeo customers from all platforms except web and api */
              vimeo_nonweb_customers AS
              (
              SELECT
                'NULL' AS subscription_id
                , 'NULL' AS customer_id
                , user_id
                , CAST(report_date AS TIMESTAMP) AS received_at
                , DATE(customer_created_at) AS created_at
                , frequency
                , status
                , platform
                , marketing_opt_in
                , cast(NULL AS VARCHAR) AS city
                , cast(NULL AS VARCHAR) AS state
                , cast(NULL AS VARCHAR) AS zipcode
                , country
                , 'Vimeo' AS dsource
              FROM customers.all_customers
              WHERE report_date = current_date
              AND platform NOT in ('web','api')
              ),

              /* chargebee customers from platforms web and api */
              chargebee_web_api_users AS
              (
              SELECT
                cast(a.subscription_id AS VARCHAR) AS subscription_id
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
              FROM http_api.chargebee_subscriptions AS a
              LEFT JOIN ${upff_webhook_events.SQL_TABLE_NAME} AS b
              ON a.customer_id = b.customer_id
              AND a.subscription_subscription_items_0_item_price_id in ('UP-Faith-Family-Monthly','UP-Faith-Family-Yearly')
              ),

              /* union of all platforms */
              unionized_customer_data AS (SELECT * FROM vimeo_nonweb_customers UNION ALL SELECT * FROM chargebee_web_api_users),

              /* customer_events table */
              customer_events AS
              (
              SELECT DISTINCT
                a.user_id AS id
                , a.subscription_id
                , b.timestamp AS received_at
                , b.event
                , a.status
                , b.first_name
                , b.last_name
                , b.email
              FROM unionized_customer_data AS a
              LEFT JOIN ${upff_webhook_events.SQL_TABLE_NAME} AS b
              ON a.user_id = b.user_id
              )

              select * from customer_events ;;
    }

    measure: count {
      type: count
      drill_fields: [detail*]
    }

    dimension: id {
      type: number
      sql: ${TABLE}.id ;;
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

    dimension: status {
      type: string
      sql: ${TABLE}.status ;;
    }

    dimension: first_name {
      type: string
      sql: ${TABLE}.first_name ;;
    }

    dimension: last_name {
      type: string
      sql: ${TABLE}.last_name ;;
    }

    dimension: email {
      type: string
      sql: ${TABLE}.email ;;
    }

    set: detail {
      fields: [
        id,
        subscription_id,
        received_at_time,
        event,
        status,
        first_name,
        last_name,
        email
      ]
    }
  }
