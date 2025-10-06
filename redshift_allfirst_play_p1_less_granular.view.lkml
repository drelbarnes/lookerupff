view: redshift_allfirst_play_p1_less_granular {
    derived_table: {
      sql: with

/*
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
              FROM ${upff_webhook_events.SQL_TABLE_NAME}
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
              FROM ${upff_webhook_events.SQL_TABLE_NAME}
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
              FROM audience_last_event AS b
              LEFT JOIN audience_first_event AS c
              ON b.user_id = c.user_id
              ),
*/
              play_data_global AS
              (
              SELECT
                user_id
                , "timestamp" AS ts
                , video_id
                , collection
                , type
                , series
                , title
                , source
                , episode
              FROM ${redshift_allfirst_play_p0.SQL_TABLE_NAME}
              WHERE user_id <> '0'
              AND user_id ~ '^[0-9]*$'
              AND DATE(timestamp) >= '2020-01-01'
              AND DATE(timestamp) <= CURRENT_DATE
              ),

              plays_most_granular AS
              (
              SELECT
                user_id
                , ROW_NUMBER() over (PARTITION BY user_id, DATE(ts), video_id ORDER BY DATE(ts)) AS min_count
                , ts
                , collection
                , type
                , video_id
                , series
                , title
                , source
                , episode
              FROM play_data_global
              ),

              plays_max_duration AS
              (
              SELECT
                user_id
                , video_id
                , DATE(ts) AS ds
                , MAX(min_count) AS min_count
              FROM plays_most_granular
              GROUP BY 1,2,3
              ),

              plays_less_granular AS
              (
              SELECT
                a.*, ROW_NUMBER() OVER (PARTITION BY a.user_id ORDER BY a.ts) AS play_number
              FROM plays_most_granular as a
              INNER JOIN plays_max_duration as b
              ON a.user_id = b.user_id
              AND a.video_id = b.video_id
              AND DATE(a.ts) = b.ds
              AND a.min_count = b.min_count
              )

              SELECT * FROM plays_less_granular ;;

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

    dimension: min_count {
      type: number
      sql: ${TABLE}.min_count ;;
    }

    dimension_group: ts {
      type: time
      sql: ${TABLE}.ts ;;
    }

    dimension: collection {
      type: string
      sql: ${TABLE}.collection ;;
    }

    dimension: type {
      type: string
      sql: ${TABLE}.type ;;
    }

    dimension: video_id {
      type: number
      sql: ${TABLE}.video_id ;;
    }

    dimension: series {
      type: string
      sql: ${TABLE}.series ;;
    }

    dimension: title {
      type: string
      sql: ${TABLE}.title ;;
    }

    dimension: source {
      type: string
      sql: ${TABLE}.source ;;
    }

    dimension: episode {
      type: number
      sql: ${TABLE}.episode ;;
    }

    dimension: play_number {
      type: number
      sql: ${TABLE}.play_number ;;
    }

    set: detail {
      fields: [
        user_id,
        min_count,
        ts_time,
        collection,
        type,
        video_id,
        series,
        title,
        source,
        episode,
        play_number
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

# view: redshift_allfirst_play_p1_less_granuar {
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
