view: svod_view_counts {
    derived_table: {
      sql: with

              minutes as
              (
              select
                year,
                month,
                datetime,
                platform,
                sum(views) as sum_views
              from svod_titles.titles
              group by 1,2,3,4
              )

              select * from minutes ;;
    }

    measure: count {
      type: count
      drill_fields: [detail*]
    }

    dimension: year {
      type: number
      sql: ${TABLE}.year ;;
    }

    dimension: month {
      type: number
      sql: ${TABLE}.month ;;
    }

    dimension: datetime {
      type: date
      datatype: date
      sql: ${TABLE}.datetime ;;
    }

    dimension: platform {
      type: string
      sql: ${TABLE}.platform ;;
    }

    measure: sum_views {
      type: sum
      sql: ${TABLE}.sum_views ;;
    }

    set: detail {
      fields: [
        year,
        month,
        datetime,
        platform,
        sum_views
      ]
    }
  }
