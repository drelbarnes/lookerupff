view: svod_content_type {
    derived_table: {
      sql: with

              views as
              (
              select
                year,
                month,
                datetime,
                platform,
                content_type,
                sum(views) as sum_views
              from svod_titles.titles
              group by 1,2,3,4,5
              )

              select * from views where content_type is not '0' ;;
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

    dimension: content_type {
      type: string
      sql: ${TABLE}.content_type ;;
    }

    measure: sum_views {
      type: sum
      value_format: "#,##0"
      sql: ${TABLE}.sum_views ;;
    }

    set: detail {
      fields: [
        year,
        month,
        datetime,
        platform,
        content_type,
        sum_views
      ]
    }
  }
