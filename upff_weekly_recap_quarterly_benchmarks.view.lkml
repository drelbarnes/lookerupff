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

    dimension: total_views {
      type: number
      sql: ${TABLE}.total_views ;;
    }

    dimension: total_uniques {
      type: number
      sql: ${TABLE}.total_uniques ;;
    }

    dimension: series_views {
      type: number
      sql: ${TABLE}.series_views ;;
    }

    dimension: series_uniques {
      type: number
      sql: ${TABLE}.series_uniques ;;
    }

    dimension: movie_views {
      type: number
      sql: ${TABLE}.movie_views ;;
    }

    dimension: movie_uniques {
      type: number
      sql: ${TABLE}.movie_uniques ;;
    }

    dimension: other_views {
      type: number
      sql: ${TABLE}.other_views ;;
    }

    dimension: other_uniques {
      type: number
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
