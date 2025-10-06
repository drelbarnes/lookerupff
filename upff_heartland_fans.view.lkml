view: upff_heartland_fans {
    derived_table: {
      sql: with

              chargebee AS
              (
              SELECT
                customer_id AS user_id
                , customer_email AS email
                , customer_cs_marketing_opt_in AS marketing_opt_in
                , CASE
                    WHEN subscription_status = 'in_trial' THEN 'free_trial'
                    WHEN subscription_status = 'active' THEN 'enabled'
                    ELSE subscription_status
                  END AS subscription_status
                , CASE
                    WHEN subscription_billing_period_unit ='month' THEN 'monthly'
                    ELSE 'yearly'
                  END AS subscription_frequency
                , subscription_subscription_items_0_item_price_id AS plan_name
                ,'web' AS platform
              FROM http_api.chargebee_subscriptions
              WHERE subscription_subscription_items_0_item_price_id like 'UP%'
              AND DATE(TIMESTAMP) = CURRENT_DATE
              ),

              vimeo AS
              (
              SELECT
                CAST(user_id AS VARCHAR) AS user_id
                , email
                , marketing_opt_in
                , status AS subscription_status
                , frequency AS subscription_frequency
                , CASE
                    WHEN frequency = 'monthly' THEN 'UP-Faith-Family-Monthly'
                    ELSE 'UP-Faith-Family-Yearly'
                  END AS plan_name
                , platform
              FROM customers.all_customers
              WHERE action = 'subscription'
              AND report_date = current_date
              AND platform NOT in ('api')
              ),

              customers_updated_event AS
              (
              SELECT * FROM chargebee
              UNION ALL
              SELECT * FROM vimeo
              ),

              target_audience AS
              (
              SELECT DISTINCT
                user_id
              FROM ${redshift_allfirst_play_p1_less_granular.SQL_TABLE_NAME}
              WHERE collection = 'Heartland - Season 18'
              AND min_count > 10
              GROUP BY user_id
              HAVING count(DISTINCT episode) >= 8
              ),

              target_behavior AS
              (
              SELECT
                *
              FROM ${redshift_allfirst_play_p1_less_granular.SQL_TABLE_NAME}
              WHERE user_id in (SELECT user_id FROM target_audience)
              ),

              target_flags_p0 AS
              (
              SELECT
                *
                , CASE WHEN series != 'Heartland' THEN 1 ELSE 0 END AS other_flag
                , CASE WHEN series = 'Heartland' THEN 1 ELSE 0 END AS heartland_flag
                , CASE WHEN collection = 'Heartland - Season 18' THEN 1 ELSE 0 END AS hl_s17_flag
              FROM target_behavior
              ),

              target_flags_p1 AS
              (
              SELECT
                user_id
                , sum(other_flag) AS sum_other_flag
                , sum(heartland_flag) AS sum_heartland_flag
                , sum(hl_s17_flag) as sum_hl_s18_flag
              FROM target_flags_p0
              GROUP BY user_id
              ),

              target_flags_p2 AS
              (
              SELECT
                user_id
                , sum_other_flag
                , sum_heartland_flag
                , CASE WHEN sum_other_flag = 0 THEN 1 ELSE 0 END AS heartland_only_flag
                , sum_hl_s18_flag
              FROM target_flags_p1
              ),

              play_analysis AS
              (
              SELECT
                b.*
                , a.sum_other_flag
                , a.sum_heartland_flag
                , a.heartland_only_flag
                , a.sum_hl_s18_flag
              FROM target_flags_p2 AS a
              LEFT JOIN ${redshift_allfirst_play_p1_less_granular.SQL_TABLE_NAME} AS b
              ON a.user_id = b.user_id
              ),

              audience_view_flags AS
              (
              SELECT
                user_id
                , sum_other_flag
                , sum_heartland_flag
                , heartland_only_flag
                , sum_hl_s18_flag
              FROM play_analysis
              ),

              audience_trans_flags AS
              (
              SELECT
                a.*
                , b.email
                , b.marketing_opt_in
                , b.subscription_frequency
                , b.subscription_status
              FROM audience_view_flags AS a
              LEFT JOIN customers_updated_event AS b
              ON a.user_id = b.user_id
              ),

              final_set AS
              (
              SELECT DISTINCT
                *
              FROM audience_trans_flags
              WHERE heartland_only_flag in (0,1)
              )

              select * from final_set ;;
    }

    measure: count {
      type: count
      drill_fields: [detail*]
    }

    dimension: user_id {
      type: string
      sql: ${TABLE}.user_id ;;
    }

    dimension: sum_other_flag {
      type: number
      sql: ${TABLE}.sum_other_flag ;;
    }

    dimension: sum_heartland_flag {
      type: number
      sql: ${TABLE}.sum_heartland_flag ;;
    }

    dimension: heartland_only_flag {
      type: number
      sql: ${TABLE}.heartland_only_flag ;;
    }

    dimension: sum_hl_s18_flag {
      type: number
      sql: ${TABLE}.sum_hl_s18_flag ;;
    }

    dimension: email {
      type: string
      sql: ${TABLE}.email ;;
    }

    dimension: marketing_opt_in {
      type: string
      sql: ${TABLE}.marketing_opt_in ;;
    }

    dimension: subscription_frequency {
      type: string
      sql: ${TABLE}.subscription_frequency ;;
    }

    dimension: subscription_status {
      type: string
      sql: ${TABLE}.subscription_status ;;
    }

    set: detail {
      fields: [
        user_id,
        sum_other_flag,
        sum_heartland_flag,
        heartland_only_flag,
        sum_hl_s18_flag,
        email,
        marketing_opt_in,
        subscription_frequency,
        subscription_status
      ]
    }
  }
