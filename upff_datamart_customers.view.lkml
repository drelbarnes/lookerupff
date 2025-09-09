view: upff_datamart_customers {
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

              /* pulls in content_customer attributes from subscription_created event */
              chargebee_enhanced_data AS
              (
              SELECT
                a.*
                , b._id AS id
                , b.anonymous_id
                , b.content_customer_cf_branding
                , b.content_customer_cf_news_letter
                , b.content_customer_cf_promotions
                , b.content_customer_cf_streaming
              FROM unionized_customer_data AS a
              LEFT JOIN chargebee_webhook_events.subscription_created AS b
              ON a.customer_id = b.content_customer_id
              ),

              /* customer table */
              customers AS
              (
              SELECT
                user_id
                , anonymous_id
                , received_at
                , created_at
                , platform
                , marketing_opt_in
                , city
                , state
                , zipcode
                , country
                , content_customer_cf_branding
                , content_customer_cf_news_letter
                , content_customer_cf_promotions
                , content_customer_cf_streaming
              FROM chargebee_enhanced_data
              )

              select * from customers ;;
    }

    measure: count {
      type: count
      drill_fields: [detail*]
    }

    dimension: user_id {
      type: number
      sql: ${TABLE}.user_id ;;
    }

    dimension: anonymous_id {
      type: string
      sql: ${TABLE}.anonymous_id ;;
    }

    dimension_group: received_at {
      type: time
      sql: ${TABLE}.received_at ;;
    }

    dimension: created_at {
      type: date
      sql: ${TABLE}.created_at ;;
    }

    dimension: platform {
      type: string
      sql: ${TABLE}.platform ;;
    }

    dimension: marketing_opt_in {
      type: string
      sql: ${TABLE}.marketing_opt_in ;;
    }

    dimension: city {
      type: string
      sql: ${TABLE}.city ;;
    }

    dimension: state {
      type: string
      sql: ${TABLE}.state ;;
    }

    dimension: zipcode {
      type: string
      sql: ${TABLE}.zipcode ;;
    }

    dimension: country {
      type: string
      sql: ${TABLE}.country ;;
    }

    dimension: content_customer_cf_branding {
      type: string
      sql: ${TABLE}.content_customer_cf_branding ;;
    }

    dimension: content_customer_cf_news_letter {
      type: string
      sql: ${TABLE}.content_customer_cf_news_letter ;;
    }

    dimension: content_customer_cf_promotions {
      type: string
      sql: ${TABLE}.content_customer_cf_promotions ;;
    }

    dimension: content_customer_cf_streaming {
      type: string
      sql: ${TABLE}.content_customer_cf_streaming ;;
    }

    set: detail {
      fields: [
        user_id,
        anonymous_id,
        received_at_time,
        created_at,
        platform,
        marketing_opt_in,
        city,
        state,
        zipcode,
        country,
        content_customer_cf_branding,
        content_customer_cf_news_letter,
        content_customer_cf_promotions,
        content_customer_cf_streaming
      ]
    }
  }
