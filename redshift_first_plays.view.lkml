view: redshift_first_plays {
    derived_table: {
      sql:    with

              play_data_global AS
              (
              SELECT
                *
              FROM ${redshift_allfirst_play_p0.SQL_TABLE_NAME}
              WHERE user_id <> '0'
              AND user_id ~ '^[0-9]*$'
              AND user_id is NOT NULL
              AND title is NOT NULL
              AND DATE("TIMESTAMP") >= '2024-01-01'
              ),

              plays_most_granular AS
              (
              SELECT
                user_id
                , ROW_NUMBER() OVER (PARTITION BY user_id, DATE("TIMESTAMP"), collection ORDER BY DATE("TIMESTAMP")) AS min_count
                , "TIMESTAMP" AS ts
                , collection
                , TYPE AS media_type
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
                a.*
                , ROW_NUMBER() OVER (PARTITION BY a.user_id ORDER BY a.ts) AS play_number
              FROM plays_most_granular AS a
              INNER JOIN plays_max_duration AS b
              ON a.user_id = b.user_id
              AND a.video_id = b.video_id
              AND DATE(a.ts) = b.ds
              AND a.min_count = b.min_count
              ),

              first_plays AS
              (
              SELECT
                collection
                , COUNT(DISTINCT user_id) AS play_count
              FROM plays_less_granular
              WHERE play_number = 1
              AND ts BETWEEN {% date_start date_filter %} AND {% date_end date_filter %}
              GROUP BY 1
              )

              select * from first_plays order by play_count desc ;;
    }

    filter: date_filter {
      label: "Date Range"
      type: date
    }

    filter: end_date {
      label: "End Date"
      type: date
    }

    measure: count {
      type: count
      drill_fields: [detail*]
    }

    dimension: collection {
      type: string
      sql: ${TABLE}.collection ;;
    }

    dimension: play_count {
      type: number
      sql: ${TABLE}.play_count ;;
    }

    set: detail {
      fields: [
        collection,
        play_count
      ]
    }
 }
