view: upff_series_overlap {
    derived_table: {
      sql: {% raw %} with

              a AS
              (
              SELECT
                user_id
                , collection
              FROM looker_scratch.lr$rmc5u1779595405461_redshift_timeupdate
              WHERE collection in
                ('Sugarcreek Amish Mysteries - Season 1', 'Blue Skies - Season 1')
              ),

              b AS
              (
              SELECT
                user_id
                , collection
                , CASE WHEN collection = 'Sugarcreek Amish Mysteries - Season 1' THEN 1 ELSE 0 END AS series1_flag
                , CASE WHEN collection = 'Blue Skies - Season 1' THEN 1 ELSE 0 END AS series2_flag
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
              GROUP BY set_membership_flag, user_count
              ORDER BY
                CASE
                  WHEN set_membership_flag = '1 only' THEN 1
                  WHEN set_membership_flag = '2 only' THEN 2
                  WHEN set_membership_flag = 'both' THEN 3
                END
              )

              select * from e {% endraw %} ;;
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

    dimension: pct_total_users {
      type: number
      sql: ${TABLE}.pct_total_users ;;
    }

    measure: pct_total_users_measure {
      type: number
      sql: 1.0 * ${user_count} / SUM(${user_count}) OVER () ;;
      value_format_name: percent_2
    }

    dimension: stack_group {
      type: string
      sql: 'All Users' ;;
    }

    set: detail {
      fields: [
        label,
        user_count,
        pct_total_users
      ]
    }
}
