view: redshift_allfirst_play_r0_upff_webhook_events {
    derived_table: {
      sql: with

              vimeo_purchase_event_p0 AS
              (
              SELECT
                user_id
                , event AS topic
                , email
                , marketing_opt_in AS moptin
                , subscription_status
                , plan AS subscription_frequency
                , platform
                , ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY timestamp ASC) AS event_num
                , DATE(timestamp) AS date_stamp
              /* ---- UPDATE DAILY PDT HERE ---- */
              FROM looker_scratch.lr$rmlrb1720602127589_upff_webhook_events
              WHERE user_id <> '0'
              AND user_id ~ '^[0-9]*$'
              AND DATE(timestamp) < CURRENT_DATE
              ORDER BY
                user_id
                , DATE(timestamp)
              ),

              vimeo_purchase_event_q0 AS
              (
              SELECT
                user_id
                , event AS topic
                , email
                , marketing_opt_in AS moptin
                , subscription_status
                , plan AS subscription_frequency
                , platform
                , ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY timestamp DESC) AS event_num
                , DATE(timestamp) AS date_stamp
              /* ---- UPDATE DAILY PDT HERE ---- */
              FROM looker_scratch.lr$rmlrb1720602127589_upff_webhook_events
              WHERE user_id <> '0'
              AND user_id ~ '^[0-9]*$'
              AND DATE(timestamp) < CURRENT_DATE
              ORDER BY
                user_id
                , DATE(timestamp)
              ),

              distinct_purchase_event AS
              (
              SELECT DISTINCT
                user_id
                , topic
                , extract(month FROM date_stamp) AS month
                , extract(year FROM date_stamp) AS year
              FROM vimeo_purchase_event_p0
              ),

              audience_first_event AS
              (
              SELECT
                user_id
                , min(date_stamp) AS first_event_date
              FROM vimeo_purchase_event_p0
              GROUP BY user_id
              ),

              audience_last_event AS
              (
              SELECT
                user_id
                , email
                , topic
                , moptin
                , platform
                , subscription_frequency
                , date_stamp AS last_event
              FROM vimeo_purchase_event_q0
              WHERE event_num = 1
              ),

              customers_updated_event AS
              (
              SELECT
                b.user_id
                , b.email
                , b.moptin
                , b.platform
                , b.topic AS vimeo_status
                , b.subscription_frequency
                , b.last_event AS last_event_date
                , c.first_event_date
                , DATEDIFF(day, c.first_event_date, last_event) AS tenure_days
              FROM audience_last_event AS b
              LEFT JOIN audience_first_event AS c
              ON b.user_id = c.user_id
              )

              select * from customers_updated_event ;;

      distribution_style: "even"
      sortkeys: ["user_id", "video_id"]
      datagroup_trigger: redshift_upff_datagroup

    }

    measure: count {
      type: count
      drill_fields: [detail*]
    }

    dimension: user_id {
      type: string
      sql: ${TABLE}.user_id ;;
    }

    dimension: email {
      type: string
      sql: ${TABLE}.email ;;
    }

    dimension: moptin {
      type: string
      sql: ${TABLE}.moptin ;;
    }

    dimension: platform {
      type: string
      sql: ${TABLE}.platform ;;
    }

    dimension: vimeo_status {
      type: string
      sql: ${TABLE}.vimeo_status ;;
    }

    dimension: subscription_frequency {
      type: string
      sql: ${TABLE}.subscription_frequency ;;
    }

    dimension: last_event_date {
      type: date
      sql: ${TABLE}.last_event_date ;;
    }

    dimension: first_event_date {
      type: date
      sql: ${TABLE}.first_event_date ;;
    }

    dimension: tenure_days {
      type: number
      sql: ${TABLE}.tenure_days ;;
    }

    set: detail {
      fields: [
        user_id,
        email,
        moptin,
        platform,
        vimeo_status,
        subscription_frequency,
        last_event_date,
        first_event_date,
        tenure_days
      ]
    }
  }

  # # You can specify the table name if it's different from the view name:
  # sql_table_name: my_schema_name.tester ;;
  #
  # # Define your dimensions and measures here, like this:
  # dimension: user_id {
  #   description: "Unique ID for each user that has ordered"
  #   type: number
  #   sql: ${TABLE}.user_id ;;
  # }
  #
  # dimension: lifetime_orders {
  #   description: "The total number of orders for each user"
  #   type: number
  #   sql: ${TABLE}.lifetime_orders ;;
  # }
  #
  # dimension_group: most_recent_purchase {
  #   description: "The date when each user last ordered"
  #   type: time
  #   timeframes: [date, week, month, year]
  #   sql: ${TABLE}.most_recent_purchase_at ;;
  # }
  #
  # measure: total_lifetime_orders {
  #   description: "Use this for counting lifetime orders across many users"
  #   type: sum
  #   sql: ${lifetime_orders} ;;
  # }

# view: redshift_allfirst_play_r0_upff_webhook_events {
#   # Or, you could make this view a derived table, like this:
#   derived_table: {
#     sql: SELECT
#         user_id as user_id
#         , COUNT(*) as lifetime_orders
#         , MAX(orders.created_at) as most_recent_purchase_at
#       FROM orders
#       GROUP BY user_id
#       ;;
#   }
#
#   # Define your dimensions and measures here, like this:
#   dimension: user_id {
#     description: "Unique ID for each user that has ordered"
#     type: number
#     sql: ${TABLE}.user_id ;;
#   }
#
#   dimension: lifetime_orders {
#     description: "The total number of orders for each user"
#     type: number
#     sql: ${TABLE}.lifetime_orders ;;
#   }
#
#   dimension_group: most_recent_purchase {
#     description: "The date when each user last ordered"
#     type: time
#     timeframes: [date, week, month, year]
#     sql: ${TABLE}.most_recent_purchase_at ;;
#   }
#
#   measure: total_lifetime_orders {
#     description: "Use this for counting lifetime orders across many users"
#     type: sum
#     sql: ${lifetime_orders} ;;
#   }
# }
