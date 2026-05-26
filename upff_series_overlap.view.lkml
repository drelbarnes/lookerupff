view: upff_series_overlap {
    derived_table: {
      sql:    with

              a AS
              (
              SELECT
                user_id
                , collection
              FROM ${redshift_timeupdate.SQL_TABLE_NAME}
              WHERE collection in
                (
                {% condition title_filter_a %} title {% endcondition %},
                {% condition title_filter_b %} title {% endcondition %}
                )
              AND
                {% condition date_filter %} DATE(timestamp) {% endcondition %}
              ),

              b AS
              (
              SELECT
                user_id
                , collection
                , CASE WHEN collection = {% condition title_filter_a %} title {% endcondition %} THEN 1 ELSE 0 END AS series1_flag
                , CASE WHEN collection = {% condition title_filter_b %} title {% endcondition %} THEN 1 ELSE 0 END AS series2_flag
              FROM a
              ),

              c AS
              (
              SELECT
                user_id
                , MAX(series1_flag) AS max_s1_flag
                , MAX(series2_flag) AS max_s2_flag
              FROM b
              GROUP BY user_id
              ),

              d AS
              (
              SELECT
                user_id
                , max_s1_flag
                , max_s2_flag
                , CASE
                    WHEN max_s1_flag = 1 AND max_s2_flag = 0 THEN '1 only'
                    WHEN max_s2_flag = 1 AND max_s1_flag = 0 THEN '2 only'
                    WHEN max_s1_flag = 1 AND max_s2_flag = 1 THEN 'both'
                    ELSE 'neither'
                  END AS set_membership_flag
              FROM c
              ),

              e AS
              (
              SELECT
                set_membership_flag AS label
                , COUNT(*) AS user_count
                , ROUND(COUNT(*)::DECIMAL / SUM(COUNT(*)) OVER (), 2) AS pct_total_users
              FROM d
              WHERE set_membership_flag in ('1 only', '2 only', 'both')
              GROUP BY set_membership_flag
              ORDER BY
                CASE
                  WHEN set_membership_flag = '1 only' THEN 1
                  WHEN set_membership_flag = '2 only' THEN 2
                  WHEN set_membership_flag = 'both' THEN 3
                END
              )

              select * from e ;;
    }

    filter: title_filter_a {
      type: string
    }

    filter: title_filter_b {
      type: string
    }

    filter: date_filter {
      type: date
    }

    measure: count {
      type: count
      drill_fields: [detail*]
    }

    dimension: label {
      type: string
      sql: ${TABLE}.label ;;
    }

    dimension: user_count {
      type: number
      sql: ${TABLE}.user_count ;;
    }

    measure: pct_total_users {
      type: sum
      sql: ${TABLE}.pct_total_users ;;
      value_format_name: percent_2
    }

    set: detail {
      fields: [
        label,
        user_count,
        pct_total_users
      ]
    }
}
