view: svod_sub_counts {

    derived_table: {
      sql: with

              subs_p0 as
              (
              select
                date,
                year,
                month,
                safe_cast(amazon as int64) as amazon,
                safe_cast(appletv as int64) as appletv,
                safe_cast(comcast as int64) as comcast,
                safe_cast(cox as int64) as cox,
                safe_cast(d2c as int64) as d2c,
                safe_cast(directv as int64) as directv,
                safe_cast(dish_sling as int64) as dish_sling,
                safe_cast(roku as int64) as roku,
                safe_cast(youtube as int64) as youtube
              from svod_titles.mvpd_subs
              ),

              subs_p1 as
              (
              select
                *
              from subs_p0
              unpivot
                (
                sub_counts for platform in
                  (
                  amazon,
                  comcast,
                  cox,
                  dish_sling,
                  directv,
                  roku,
                  appletv,
                  youtube,
                  d2c
                  )
                )
              )

              select * from subs_p1 ;;
    }

    measure: count {
      type: count
      drill_fields: [detail*]
    }

    dimension: date {
      type: date
      datatype: date
      sql: ${TABLE}.date ;;
    }

    dimension: year {
      type: number
      sql: ${TABLE}.year ;;
    }

    dimension: month {
      type: number
      sql: ${TABLE}.month ;;
    }

    measure: sub_counts {
      type: sum
      value_format: "#,##0"
      sql: ${TABLE}.sub_counts ;;
    }

    dimension: platform {
      type: string
      sql: ${TABLE}.platform ;;
    }

    set: detail {
      fields: [
        date,
        year,
        month,
        sub_counts,
        platform
      ]
    }
  }
