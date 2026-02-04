view: watch_time {
  derived_table: {
    sql:

WITH a AS (

  -- WEB
  SELECT
    CAST(a.video_id AS BIGINT) AS video_id,
    user_id,
    'Web' AS source,
    'video_content_playing' AS event_type,
    timestamp AS timestamp
  FROM javascript.video_content_playing a
  WHERE 1=1
    {% if start_date._parameter_value != "NULL" %}
    AND DATE(received_at) >= {% parameter start_date %}
    {% endif %}

    {% if end_date._parameter_value != "NULL" %}
    AND DATE(received_at) <= {% parameter end_date %}
    {% endif %}

  UNION ALL

  -- IOS
  SELECT
    CAST(a.video_id AS BIGINT),
    user_id,
    'iOS',
    'video_content_playing',
    timestamp
  FROM ios.video_content_playing a
  WHERE 1=1
    {% if start_date._parameter_value != "NULL" %}
    AND DATE(received_at) >= {% parameter start_date %}
    {% endif %}

    {% if end_date._parameter_value != "NULL" %}
    AND DATE(received_at) <= {% parameter end_date %}
    {% endif %}

  UNION ALL

  -- ANDROID
  SELECT
    CAST(a.video_id AS BIGINT),
    user_id,
    'Android',
    'video_content_playing',
    timestamp
  FROM android.video_content_playing a
  WHERE 1=1
    {% if start_date._parameter_value != "NULL" %}
    AND DATE(received_at) >= {% parameter start_date %}
    {% endif %}

    {% if end_date._parameter_value != "NULL" %}
    AND DATE(received_at) <= {% parameter end_date %}
    {% endif %}

  UNION ALL

  -- FIRE TV
  SELECT
    CAST(a.video_id AS BIGINT),
    user_id,
    'FireTV',
    'video_content_playing',
    timestamp
  FROM amazon_fire_tv.video_content_playing a
  WHERE 1=1
    {% if start_date._parameter_value != "NULL" %}
    AND DATE(received_at) >= {% parameter start_date %}
    {% endif %}

    {% if end_date._parameter_value != "NULL" %}
    AND DATE(received_at) <= {% parameter end_date %}
    {% endif %}

  UNION ALL

  -- ROKU
  SELECT
    CAST(a.video_id AS BIGINT),
    user_id,
    'Roku',
    'video_content_playing',
    timestamp
  FROM roku.video_content_playing a
  WHERE 1=1
    {% if start_date._parameter_value != "NULL" %}
    AND DATE(received_at) >= {% parameter start_date %}
    {% endif %}

    {% if end_date._parameter_value != "NULL" %}
    AND DATE(received_at) <= {% parameter end_date %}
    {% endif %}

    UNION ALL

    SELECT
    CAST(a.video_id AS BIGINT),
    user_id,
    'iOS',
    'video_content_playing',
    timestamp
  FROM ios.firstplay a
  WHERE 1=1
    {% if start_date._parameter_value != "NULL" %}
    AND DATE(received_at) >= {% parameter start_date %}
    {% endif %}

    {% if end_date._parameter_value != "NULL" %}
    AND DATE(received_at) <= {% parameter end_date %}
    {% endif %}

    UNION ALL

    SELECT
    CAST(a.video_id AS BIGINT),
    user_id,
    'Android',
    'video_content_playing',
    timestamp
  FROM android.firstplay a
  WHERE 1=1
    {% if start_date._parameter_value != "NULL" %}
    AND DATE(received_at) >= {% parameter start_date %}
    {% endif %}

    {% if end_date._parameter_value != "NULL" %}
    AND DATE(received_at) <= {% parameter end_date %}
    {% endif %}


),

-- =====================================
-- DURATION LOGIC
-- =====================================
watch_events AS (
  SELECT
    user_id,
    video_id,
    COUNT(*) AS heartbeat_events
  FROM a
  WHERE user_id IS NOT NULL
    AND user_id <> '0'
  GROUP BY 1,2
),

watch_duration AS (
  SELECT
    user_id
    ,SUM(heartbeat_events) AS total_watch_minutes
  FROM watch_events
  where heartbeat_events > 2
  GROUP BY 1
),

filter_users as (
SELECT
  user_id,
  total_watch_minutes
FROM watch_duration
where total_watch_minutes >= {% parameter watch_time %})

select
  b.email
  ,b.user_id
  ,a.total_watch_minutes
  ,b.platform
FROM filter_users a
LEFT JOIN (select platform,CAST(user_id AS VARCHAR(255)) as user_id, email from customers.all_customers where report_date = current_date - 1 and action != 'follow') b
ON a.user_id = b.user_id
;;

}

  parameter: start_date {
    type: date
    default_value: "30 days ago"
  }

  parameter: end_date {
    type: date
  }

  parameter: watch_time{
    type: number
    default_value: "5"
  }

  dimension: total_watch_minutes{
    type: number
    sql: ${TABLE}.total_watch_minutes;;
  }

  dimension: email {
    type: string
    sql: ${TABLE}.email ;;
}

  dimension: user_id {
    type: string
    sql: ${TABLE}.user_id ;;
  }

  dimension: platform{
    type: string
    sql: ${TABLE}.platform ;;
  }
}
