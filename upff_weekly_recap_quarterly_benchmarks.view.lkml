view: upff_weekly_recap_quarterly_benchmarks {
    derived_table: {
      sql:

    WITH benchmarks AS (
      SELECT
        CAST('2025-4Q' AS VARCHAR(7)) AS quarter,
        CAST(584584 AS BIGINT) AS total_views,
        CAST(100026 AS BIGINT) AS total_uniques,
        CAST(419504 AS BIGINT) AS series_views,
        CAST(71874 AS BIGINT) AS series_uniques,
        CAST(134746 AS BIGINT) AS movie_views,
        CAST(46980 AS BIGINT) AS movie_uniques,
        CAST(24828 AS BIGINT) AS other_views,
        CAST(11109 AS BIGINT) AS other_uniques
 /*     UNION ALL SELECT
        CAST('2026-1Q'),
        CAST(600000 AS BIGINT),
        CAST(250000 AS BIGINT),
        CAST(400000 AS BIGINT),
        CAST(20000 AS BIGINT),
        CAST(150000 AS BIGINT),
        CAST(20000 AS BIGINT),
        CAST(15000 AS BIGINT),
        CAST(10000 AS BIGINT)
      UNION ALL SELECT
        CAST('2026-2Q'),
        CAST(600000 AS BIGINT),
        CAST(250000 AS BIGINT),
        CAST(400000 AS BIGINT),
        CAST(20000 AS BIGINT),
        CAST(150000 AS BIGINT),
        CAST(20000 AS BIGINT),
        CAST(15000 AS BIGINT),
        CAST(10000 AS BIGINT)
      UNION ALL SELECT
        CAST('2026-3Q'),
        CAST(600000 AS BIGINT),
        CAST(250000 AS BIGINT),
        CAST(400000 AS BIGINT),
        CAST(20000 AS BIGINT),
        CAST(150000 AS BIGINT),
        CAST(20000 AS BIGINT),
        CAST(15000 AS BIGINT),
        CAST(10000 AS BIGINT)
      UNION ALL SELECT
        CAST('2026-4Q'),
        CAST(600000 AS BIGINT),
        CAST(250000 AS BIGINT),
        CAST(400000 AS BIGINT),
        CAST(20000 AS BIGINT),
        CAST(150000 AS BIGINT),
        CAST(20000 AS BIGINT),
        CAST(15000 AS BIGINT),
        CAST(10000 AS BIGINT)
  */
    )



              select * from benchmarks ;;
    }

    measure: count {
      type: count
      drill_fields: [detail*]
    }

    dimension: quarter {
      type: string
      sql: ${TABLE}.quarter ;;
    }

    measure: total_views {
      type: sum
      sql: ${TABLE}.total_views ;;
    }

    measure: total_uniques {
      type: sum
      sql: ${TABLE}.total_uniques ;;
    }

    measure: series_views {
      type: sum
      sql: ${TABLE}.series_views ;;
    }

    measure: series_uniques {
      type: sum
      sql: ${TABLE}.series_uniques ;;
    }

    measure: movie_views {
      type: sum
      sql: ${TABLE}.movie_views ;;
    }

    measure: movie_uniques {
      type: sum
      sql: ${TABLE}.movie_uniques ;;
    }

    measure: other_views {
      type: sum
      sql: ${TABLE}.other_views ;;
    }

    measure: other_uniques {
      type: sum
      sql: ${TABLE}.other_uniques ;;
    }

    set: detail {
      fields: [
        quarter,
        total_views,
        total_uniques,
        series_views,
        series_uniques,
        movie_views,
        movie_uniques,
        other_views,
        other_uniques
      ]
    }
  }
