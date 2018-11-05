explore: bigquery_churn_model_firstplay {}
view: bigquery_churn_model_firstplay {
  derived_table: {
    sql: WITH
      a AS (
      SELECT
        id AS video_id,
        CASE
          WHEN series LIKE '%Heartland%' THEN 'Heartland'
          WHEN series LIKE '%Bringing Up Bates%' THEN 'Bringing Up Bates'
          ELSE 'Other'
        END AS content
      FROM
        svod_titles.titles_id_mapping),

      web AS (
      SELECT
        user_id,
        'web' AS source,
        timestamp,
        CASE
          WHEN upper(title) LIKE '%HEARTLAND%' THEN 'Heartland'
          WHEN upper(title) LIKE '%BRINGING UP BATES%' THEN 'Bringing Up Bates'
          ELSE 'Other'
        END AS content
      FROM
        javascript.firstplay
      WHERE
        user_id IS NOT NULL),

      droid AS (
      SELECT
        user_id,
        'android' AS source,
        timestamp,
        content
      FROM
        android.firstplay
      INNER JOIN
        a
      ON
        firstplay.video_id=a.video_id),

      apple AS (
      SELECT
        user_id,
        'ios' AS source,
        timestamp,
        content
      FROM
        ios.firstplay
      INNER JOIN
        a
      ON
        SAFE_CAST(firstplay.video_id AS int64)=SAFE_CAST(a.video_id AS int64))


      SELECT user_id,
             (timestamp) as timestamp,
             (case when content="Other" then 1 else 0 end) as other_plays,
             (case when content="Bringing Up Bates" then 1 else 0 end) as bates_plays,
             (case when content="Heartland" then 1 else 0 end) as heartland_plays
      FROM web
      UNION ALL
      SELECT user_id,
             (timestamp) as timestamp,
             (case when content="Other" then 1 else 0 end) as other_plays,
             (case when content="Bringing Up Bates" then 1 else 0 end) as bates_plays,
             (case when content="Heartland" then 1 else 0 end) as heartland_plays
      FROM droid
      UNION ALL
      SELECT user_id,
             (timestamp) as timestamp,
             (case when content="Other" then 1 else 0 end) as other_plays,
             (case when content="Bringing Up Bates" then 1 else 0 end) as bates_plays,
             (case when content="Heartland" then 1 else 0 end) as heartland_plays
      FROM apple ;;}

  dimension: user_id {
    primary_key: yes
    tags: ["user_id"]
    type: string
    sql: ${TABLE}.user_id ;;
  }

  dimension_group: timestamp {
    type: time
    timeframes: [
      raw,
      time,
      date,
      week,
      month,
      quarter,
      year
    ]
    sql: ${TABLE}.timestamp ;;
  }

  measure: other_plays {
    type: sum
    sql: ${TABLE}.other_plays ;;
  }

  measure: bates_plays {
    type: sum
    sql: ${TABLE}.bates_plays ;;
  }

  measure: heartland_plays {
    type: sum
    sql: ${TABLE}.heartland_plays ;;
  }



}
