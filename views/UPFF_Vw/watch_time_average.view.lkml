view: watch_time_average {
  derived_table: {
    sql:
    WITH
chargebee_monthly_users AS (
    SELECT
        customer_email AS email
    FROM http_api.chargebee_subscriptions
    WHERE DATE(timestamp) = current_date - 1
      AND subscription_billing_period_unit = 'month'
      AND customer_cs_marketing_opt_in = 'true'
      AND subscription_status = 'active'
),

vimeo AS (
    SELECT
        CAST(user_id AS VARCHAR(255)) AS user_id,
        email
    FROM customers.all_customers
    WHERE report_date = current_date - 1
      AND action != 'follow'
      AND platform = 'api'
),

monthly_users AS (
    SELECT
        cb.email,
        v.user_id
    FROM chargebee_monthly_users cb
    LEFT JOIN vimeo v
      ON cb.email = v.email
    WHERE v.user_id IS NOT NULL
      AND v.user_id <> '0'
),

a AS (
    -- WEB
    SELECT
        mu.user_id,
        'Web' AS source,
        a.timestamp AS event_ts
    FROM javascript.video_content_playing a
    JOIN monthly_users mu
      ON a.user_id = mu.user_id
    WHERE 1=1
    {% if start_date._parameter_value != "NULL" %}
      AND a.received_at >= {% parameter start_date %}
    {% endif %}
    {% if end_date._parameter_value != "NULL" %}
      AND a.received_at < DATEADD(day, 1, {% parameter end_date %})
    {% endif %}

    UNION ALL

    -- IOS
    SELECT
        mu.user_id,
        'iOS' AS source,
        a.timestamp AS event_ts
    FROM ios.video_content_playing a
    JOIN monthly_users mu
      ON a.user_id = mu.user_id
    WHERE 1=1
    {% if start_date._parameter_value != "NULL" %}
      AND a.received_at >= {% parameter start_date %}
    {% endif %}
    {% if end_date._parameter_value != "NULL" %}
      AND a.received_at < DATEADD(day, 1, {% parameter end_date %})
    {% endif %}

    UNION ALL

    -- ANDROID
    SELECT
        mu.user_id,
        'Android' AS source,
        a.timestamp AS event_ts
    FROM android.video_content_playing a
    JOIN monthly_users mu
      ON a.user_id = mu.user_id
    WHERE 1=1
    {% if start_date._parameter_value != "NULL" %}
      AND a.received_at >= {% parameter start_date %}
    {% endif %}
    {% if end_date._parameter_value != "NULL" %}
      AND a.received_at < DATEADD(day, 1, {% parameter end_date %})
    {% endif %}

    UNION ALL

    -- FIRE TV
    SELECT
        mu.user_id,
        'FireTV' AS source,
        a.timestamp AS event_ts
    FROM amazon_fire_tv.video_content_playing a
    JOIN monthly_users mu
      ON a.user_id = mu.user_id
    WHERE 1=1
    {% if start_date._parameter_value != "NULL" %}
      AND a.received_at >= {% parameter start_date %}
    {% endif %}
    {% if end_date._parameter_value != "NULL" %}
      AND a.received_at < DATEADD(day, 1, {% parameter end_date %})
    {% endif %}

    UNION ALL

    -- ROKU
    SELECT
        mu.user_id,
        'Roku' AS source,
        a.timestamp AS event_ts
    FROM roku.video_content_playing a
    JOIN monthly_users mu
      ON a.user_id = mu.user_id
    WHERE 1=1
    {% if start_date._parameter_value != "NULL" %}
      AND a.received_at >= {% parameter start_date %}
    {% endif %}
    {% if end_date._parameter_value != "NULL" %}
      AND a.received_at < DATEADD(day, 1, {% parameter end_date %})
    {% endif %}

    UNION ALL

    -- IOS FIRSTPLAY
    SELECT
        mu.user_id,
        'iOS' AS source,
        a.timestamp AS event_ts
    FROM ios.firstplay a
    JOIN monthly_users mu
      ON a.user_id = mu.user_id
    WHERE 1=1
    {% if start_date._parameter_value != "NULL" %}
      AND a.received_at >= {% parameter start_date %}
    {% endif %}
    {% if end_date._parameter_value != "NULL" %}
      AND a.received_at < DATEADD(day, 1, {% parameter end_date %})
    {% endif %}

    UNION ALL

    -- ANDROID FIRSTPLAY
    SELECT
        mu.user_id,
        'Android' AS source,
        a.timestamp AS event_ts
    FROM android.firstplay a
    JOIN monthly_users mu
      ON a.user_id = mu.user_id
    WHERE 1=1
    {% if start_date._parameter_value != "NULL" %}
      AND a.received_at >= {% parameter start_date %}
    {% endif %}
    {% if end_date._parameter_value != "NULL" %}
      AND a.received_at < DATEADD(day, 1, {% parameter end_date %})
    {% endif %}
),

watch_events AS (
    SELECT
        user_id,
        DATE_TRUNC('week', event_ts) AS week_start,
        COUNT(*) AS heartbeat_events
    FROM a
    GROUP BY 1,2
),

date_bounds AS (
    SELECT
        DATE_TRUNC('week', {% parameter start_date %}) AS min_week,
        DATE_TRUNC('week', {% parameter end_date %})   AS max_week
),

total_weeks AS (
    SELECT
        DATEDIFF(week, min_week, max_week) + 1 AS num_weeks
    FROM date_bounds
),

qualified_weeks AS (
    SELECT
        user_id,
        COUNT(*) AS weeks_meeting_threshold
    FROM watch_events
    WHERE CAST(heartbeat_events AS INT)>= CAST({% parameter watch_time %} as INT)
    GROUP BY 1
),

filter_users AS (
    SELECT
        q.user_id,
        t.num_weeks,
        q.weeks_meeting_threshold
    FROM qualified_weeks q
    CROSS JOIN total_weeks t
    -- Meaningful filter: met threshold in EVERY week of the selected range
    WHERE CAST(q.weeks_meeting_threshold AS INT) = CAST(t.num_weeks AS INT)
)

SELECT
    mu.email,
    f.user_id,
    f.num_weeks,
    f.weeks_meeting_threshold
FROM filter_users f
JOIN monthly_users mu
  ON f.user_id = mu.user_id;;
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

  dimension:weeks_meeting_threshold{
    type: number
    sql: ${TABLE}.weeks_meeting_threshold;;
  }

  dimension: num_weeks{
    type: number
    sql: ${TABLE}.num_weeks;;
  }

  dimension: email {
    type: string
    sql: ${TABLE}.email ;;
  }

  dimension: user_id {
    type: string
    sql: ${TABLE}.user_id ;;
  }
}
