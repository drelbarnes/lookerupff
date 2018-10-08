view: bigquery_derived_all_firstplay {

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
      WHEN title LIKE '%Heartland%' THEN 'Heartland'
      WHEN title LIKE '%Bringing Up Bates%' THEN 'Bringing Up Bates'
      ELSE 'Other'
    END AS content
  FROM
    javascript.firstplay
  WHERE
    user_id IS NOT NULL),

  android AS (
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

  ios AS (
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
    SAFE_CAST(firstplay.video_id AS int64)=SAFE_CAST(a.video_id AS int64)),

  b AS (
  SELECT * FROM web
  UNION ALL
  SELECT * FROM android
  UNION ALL
  SELECT * FROM ios)

SELECT
  b.*,
  platform,
  case when content = 'Heartland' then 1 else 0 end as watched_heartland,
  case when content = 'Bringing Up Bates' then 1 else 0 end as watched_bates,
  case when content = 'Other' then 1 else 0 end as watched_other
FROM
  b LEFT JOIN customers.subscribers ON SAFE_CAST(user_id AS int64)=SAFE_CAST(customer_id AS int64) ;;
  }

  dimension: status {
    type: string
    sql: ${TABLE}.status ;;
  }

  dimension: platform {
    type: string
    sql: ${TABLE}.platform ;;
  }

  dimension: source {
    type: string
    sql: ${TABLE}.source ;;
  }

  dimension: user_id {
    primary_key: yes
    tags: ["user_id"]
    type: string
    sql: ${TABLE}.user_id ;;
  }

  dimension: content {
    type: string
    sql: ${TABLE}.content ;;
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

  dimension: timecode {
    type: number
    sql:  ${bigquery_subscribers_timeupdate.timecode_count};;
  }

  dimension: watched_heartland {
    type: number
    sql: ${TABLE}.watched_heartland ;;
  }

  dimension: watched_bringing_up_bates {
    type: number
    sql: ${TABLE}.watched_bates ;;
  }

  dimension: watched_other {
    type: number
    sql:  ${TABLE}.watched_other;;
  }

  measure: watched_heartland_total {
    type: sum
    sql: ${TABLE}.watched_heartland ;;
  }

  measure: watched_bates_total {
    type: sum
    sql: ${TABLE}.watched_bates ;;
  }

  measure: watched_other_total {
    type: sum
    sql:  ${TABLE}.watched_other;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  measure: number_of_platforms_by_user {
    type: count_distinct
    sql: ${source};;
  }


# ----- Sets of fields for drilling ------
  set: detail {
    fields: [
      platform,
      user_id
    ]
  }
}
