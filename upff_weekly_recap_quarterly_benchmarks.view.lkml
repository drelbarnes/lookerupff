view: upff_weekly_recap_quarterly_benchmarks {
    derived_table: {
      sql: with

              benchmarks as
              (
              SELECT
                '2025-4Q'AS quarter
                , 590790 AS total_views
                , 24703 AS total_uniques
                , 423975 AS series_views
                , 19664 AS series_uniques
                , 139109 AS movie_views
                , 16361 AS movie_uniques
                , 15022 AS other_views
                , 3915 AS other_uniques
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
