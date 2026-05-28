view: upff_series_benchmark_7d_30d {
    derived_table: {
      sql:    with

              universe AS
              (
              SELECT
                *, DATE(TIMESTAMP) AS ds
              FROM ${redshift_timeupdate.SQL_TABLE_NAME}
              WHERE {% condition collection_filter %} collection {% endcondition %}
              AND DATE(TIMESTAMP) >= {% parameter start_date %}
              ),

              completion_rates AS
              (
              with

              a AS (SELECT episode, ROUND(CASE WHEN timecode > duration THEN 100.0 ELSE 100.0 * timecode / NULLIF(duration, 0) END, 1) AS completion_rate FROM universe),

              b AS (SELECT episode AS ep, ROUND(CAST(AVG(completion_rate) / 100.0 AS DECIMAL(10,4)), 2) AS avg_complete_rate FROM a GROUP BY episode)

              select * from b
              ),

              episode_daily_views AS
              (
              SELECT
                episode
                , title
                , video_id
                , DATE(TIMESTAMP) AS ds
                , COUNT(*) AS daily_views
              FROM universe
              GROUP BY 1,2,3,4
              ),

              episode_drop_dates AS
              (
              SELECT
                episode
                , title
                , video_id
                , MIN(ds) AS drop_date
              FROM episode_daily_views
              WHERE daily_views >= 10
              GROUP BY 1,2,3
              ),

              episode_views AS
              (
              SELECT
                u.user_id
                , u.episode
                , u.title
                , u.video_id
                , u.ds
                , d.drop_date
              FROM universe AS u
              LEFT JOIN episode_drop_dates AS d
              ON u.episode = d.episode
              ),

              view_counts AS
              (
              SELECT
                e.episode
                , e.drop_date
                , COUNT(DISTINCT CASE
                  WHEN ds >= drop_date AND ds <= DATEADD(day, 6, drop_date)
                  THEN CAST(video_id AS VARCHAR) || CAST(user_id AS VARCHAR) ||  CAST(ds AS VARCHAR)
                  END) AS views_7d
                , COUNT(DISTINCT CASE
                  WHEN ds >= drop_date AND ds <= DATEADD(day, 29, drop_date)
                  THEN CAST(video_id AS VARCHAR) || CAST(user_id AS VARCHAR) ||  CAST(ds AS VARCHAR)
                  END) AS views_30d
                , COUNT(DISTINCT CASE
                  WHEN ds >= drop_date AND ds <= DATEADD(day, 6, drop_date)
                  THEN user_id
                  END) AS uniques_7d
                , COUNT(DISTINCT CASE
                  WHEN ds >= drop_date AND ds <= DATEADD(day, 29, drop_date)
                  THEN user_id
                  END) AS uniques_30d
                , cr.avg_complete_rate
                , e.title
                , CASE WHEN CURRENT_DATE > DATEADD(day, 7, drop_date) THEN 'yes' ELSE 'no' END AS finalized_7d
                , CASE WHEN CURRENT_DATE > DATEADD(day, 30, drop_date) THEN 'yes' ELSE 'no' END AS finalized_30d
              FROM episode_views AS e
              LEFT JOIN completion_rates AS cr
              ON e.episode = cr.ep
              GROUP BY 1,2,7,8,9
              )

              select * from view_counts order by episode ;;
    }

    parameter: start_date {
      type: string
      default_value: "2024-01-01"
    }

    filter: collection_filter {
      type: string
    }

    measure: count {
      type: count
      drill_fields: [detail*]
    }

    dimension: episode {
      type: number
      sql: ${TABLE}.episode ;;
    }

    dimension: drop_date {
      type: date
      sql: ${TABLE}.drop_date ;;
    }

    dimension: views_7d {
      type: number
      sql: ${TABLE}.views_7d ;;
    }

    dimension: views_30d {
      type: number
      sql: ${TABLE}.views_30d ;;
    }

    dimension: uniques_7d {
      type: number
      sql: ${TABLE}.uniques_7d ;;
    }

    dimension: uniques_30d {
      type: number
      sql: ${TABLE}.uniques_30d ;;
    }

    dimension: avg_complete_rate {
      type: number
      sql: ${TABLE}.avg_complete_rate ;;
    }

    dimension: title {
      type: string
      sql: ${TABLE}.title ;;
    }

    dimension: finalized_7d {
      type: string
      sql: ${TABLE}.finalized_7d ;;
    }

    dimension: finalized_30d {
      type: string
      sql: ${TABLE}.finalized_30d ;;
    }

    measure: pct_change_7d_views {
      type: number
      sql:
      (${views_7d} - LAG(${views_7d}) OVER (ORDER BY ${episode}))
      /
      NULLIF(LAG(${views_7d}) OVER (ORDER BY ${episode}), 0) ;;
      value_format_name: percent_1
    }

    set: detail {
      fields: [
        episode,
        drop_date,
        views_7d,
        views_30d,
        uniques_7d,
        uniques_30d,
        avg_complete_rate,
        title,
        finalized_7d,
        finalized_30d
      ]
    }
  }
