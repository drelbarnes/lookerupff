view: redshift_title_histogram {
    derived_table: {
      sql:

              with

              target_views AS
              (
              SELECT
                user_id, title, timecode, duration, DATE(timestamp) AS ds
              FROM ${redshift_timeupdate.SQL_TABLE_NAME}
              WHERE 1=1
              AND {% condition title_filter %} title {% endcondition %}
              AND {% condition date_filter %} DATE(timestamp) {% endcondition %}
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
                count(user_id) AS viewers, max_view
              FROM qualified_views
              GROUP BY 2
              ORDER BY 2 ASC
              )

              select * from histogram

             ;;
    }

    filter: title_filter {
      type: string
    }

    filter: date_filter {
      type: date
    }

    measure: count {
      type: count
      drill_fields: [detail*]
    }

    measure: view_count {
      type: sum
      sql: ${TABLE}.view_count ;;
    }

    measure: viewers {
      type: sum
      sql: ${TABLE}.viewers ;;
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
