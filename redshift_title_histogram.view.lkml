view: redshift_title_histogram {
    derived_table: {
      sql: {% raw %} with

              target_views AS
              (
              SELECT
                user_id, title, timecode, duration, DATE(timestamp) AS ds
              FROM looker_scratch.lr$rmc5u1778819459658_redshift_timeupdate
              WHERE title = 'Instant Nanny'
              AND DATE(timestamp) >= '2026-05-01'
              AND DATE(timestamp) <= '2026-05-15'
              ),

              qualified_views AS
              (
              SELECT
                user_id, ds, title, duration, round(max(timecode) / 60, 0) AS max_view
              FROM target_views
              GROUP BY 1,2,3,4
              HAVING max_view <= duration
              ),

              histogram AS
              (
              SELECT
                count(user_id), max_view
              FROM qualified_views
              GROUP BY 2
              ORDER BY 2 ASC
              )

              select * from histogram {% endraw %} ;;
    }

    measure: count {
      type: count
      drill_fields: [detail*]
    }

    dimension: count_ {
      type: number
      sql: ${TABLE}.count ;;
    }

    dimension: max_view {
      type: number
      sql: ${TABLE}.max_view ;;
    }

    set: detail {
      fields: [
        count_,
        max_view
      ]
    }
  }
