view: upff_series_dropoff {
    derived_table: {
      sql:    with

              episode_views AS
              (
              SELECT
                user_id
                , episode
              FROM looker_scratch.lr$rmc5u1779769415826_redshift_timeupdate
              WHERE {% condition collection_filter %} collection {% endcondition %}
              AND {% condition date_filter %} DATE(timestamp) {% endcondition %}
              ),

              user_progress AS
              (
              SELECT
                user_id
                , COUNT(DISTINCT episode) AS episodes_watched
                , MAX(episode) AS highest_episode_watched
              FROM episode_views
              GROUP BY user_id
              ),

              series_info AS
              (
              SELECT
                COUNT(DISTINCT episode) AS total_episodes
              FROM episode_views
              ),

              final AS
              (
              SELECT
                u.user_id
                , u.episodes_watched
                , u.highest_episode_watched
                , s.total_episodes
                , CASE
                    WHEN u.episodes_watched = s.total_episodes THEN 'completed all'
                    WHEN u.episodes_watched = u.highest_episode_watched THEN 'completed_' || u.highest_episode_watched
                    ELSE 'completed_out_of_pattern'
                  END AS completion_indicator
              FROM user_progress AS u
              CROSS JOIN series_info AS s
              ),

              analysis AS
              (
              SELECT
                completion_indicator
                , COUNT(DISTINCT user_id) AS number_unique_viewers
                , ROUND(COUNT(DISTINCT user_id)::DECIMAL / SUM(COUNT(DISTINCT user_id)) OVER (), 2) AS percent_of_total
              FROM final
              GROUP BY completion_indicator
              )

              select * from analysis ;;
    }

    filter: collection_filter {
      type: string
    }

    filter: date_filter {
      type: date
    }

    measure: count {
      type: count
      drill_fields: [detail*]
    }

    dimension: completion_indicator {
      type: string
      sql: ${TABLE}.completion_indicator ;;
    }

    measure: number_unique_viewers {
      type: sum
      sql: ${TABLE}.number_unique_viewers ;;
      value_format_name: decimal_0
    }

    measure: percent_of_total {
      type: sum
      sql: ${TABLE}.percent_of_total ;;
      value_format: "0%"
    }

    set: detail {
      fields: [
        completion_indicator,
        number_unique_viewers,
        percent_of_total
      ]
    }

}
