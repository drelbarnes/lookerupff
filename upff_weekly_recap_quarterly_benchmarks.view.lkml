view: upff_weekly_recap_quarterly_benchmarks {
    derived_table: {
      sql:

    WITH benchmarks AS (
      SELECT
        CAST('2025-4Q' AS VARCHAR(7)) AS quarter,
        CAST(590790 AS BIGINT) AS total_views,
        CAST(24703  AS BIGINT) AS total_uniques,
        CAST(423975 AS BIGINT) AS series_views,
        CAST(19664  AS BIGINT) AS series_uniques,
        CAST(139109 AS BIGINT) AS movie_views,
        CAST(16361  AS BIGINT) AS movie_uniques,
        CAST(15022  AS BIGINT) AS other_views,
        CAST(3915   AS BIGINT) AS other_uniques
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
