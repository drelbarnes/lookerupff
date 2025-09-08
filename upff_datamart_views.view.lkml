  view: sql_runner_query {
    derived_table: {
      sql: with

              plays_less_granular AS
              (
              SELECT
                *
              FROM ${allfirst_play_p1_less_granular.SQL_TABLE_NAME}
              ),

              /* views */
              views AS
              (
              SELECT
                video_id
                , user_id
                , ts AS streamed_at
                , title
                , collection
                , episode
                , series
                , type
                , source AS platform
                , min_count
              FROM plays_less_granular
              )

              select * from views limit 1000 ;;
    }

    measure: count {
      type: count
      drill_fields: [detail*]
    }

    dimension: video_id {
      type: number
      sql: ${TABLE}.video_id ;;
    }

    dimension: user_id {
      type: string
      sql: ${TABLE}.user_id ;;
    }

    dimension_group: streamed_at {
      type: time
      sql: ${TABLE}.streamed_at ;;
    }

    dimension: title {
      type: string
      sql: ${TABLE}.title ;;
    }

    dimension: collection {
      type: string
      sql: ${TABLE}.collection ;;
    }

    dimension: episode {
      type: number
      sql: ${TABLE}.episode ;;
    }

    dimension: series {
      type: string
      sql: ${TABLE}.series ;;
    }

    dimension: type {
      type: string
      sql: ${TABLE}.type ;;
    }

    dimension: platform {
      type: string
      sql: ${TABLE}.platform ;;
    }

    dimension: min_count {
      type: number
      sql: ${TABLE}.min_count ;;
    }

    set: detail {
      fields: [
        video_id,
        user_id,
        streamed_at_time,
        title,
        collection,
        episode,
        series,
        type,
        platform,
        min_count
      ]
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
}

# view: upff_datamart_views {
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
