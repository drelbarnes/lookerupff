view: bigquery_churn_model_timeupdate {
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
    END AS content,
    current_time as timecode
  FROM
    javascript.timeupdate
  WHERE
    user_id IS NOT NULL),

  android AS (
  SELECT
    user_id,
    'android' AS source,
    timestamp,
    content,
    timecode
  FROM
    android.timeupdate
  INNER JOIN
    a
  ON
    timeupdate.video_id=a.video_id),

  ios AS (
  SELECT
    user_id,
    'ios' AS source,
    timestamp,
    content,
    timecode
  FROM
    ios.timeupdate
  INNER JOIN
    a
  ON
    SAFE_CAST(timeupdate.video_id AS int64)=SAFE_CAST(a.video_id AS int64))


  SELECT user_id,
         source,
         timestamp,
         case when content="Other" then safe_cast(safe_cast(timecode as string) as int64) else 0 end as other_duration,
         case when content="Heartland" then safe_cast(safe_cast(timecode as string) as int64) else 0 end as heartland_duration,
         case when content="Bringing Up Bates" then safe_cast(safe_cast(timecode as string) as int64) else 0 end as bates_duration
  FROM web
  UNION ALL
  SELECT user_id,
         source,
         timestamp,
         case when content="Other" then safe_cast(safe_cast(timecode as string) as int64) else 0 end as other_duration,
         case when content="Heartland" then safe_cast(safe_cast(timecode as string) as int64) else 0 end as heartland_duration,
         case when content="Bringing Up Bates" then safe_cast(safe_cast(timecode as string) as int64) else 0 end as bates_duration
  FROM android
  UNION ALL
  SELECT user_id,
         source,
         timestamp,
         case when content="Other" then safe_cast(safe_cast(timecode as string) as int64) else 0 end as other_duration,
         case when content="Heartland" then safe_cast(safe_cast(timecode as string) as int64) else 0 end as heartland_duration,
         case when content="Bringing Up Bates" then safe_cast(safe_cast(timecode as string) as int64) else 0 end as bates_duration
   FROM ios;;
  }

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

  measure: other_duration {
    type: sum
    sql: ${TABLE}.other_duration ;;
  }

  measure: bates_duration {
    type: sum
    sql: ${TABLE}.bates_duration ;;
  }

  measure: heartland_duration {
    type: sum
    sql: ${TABLE}.heartland_duration ;;
  }
}
