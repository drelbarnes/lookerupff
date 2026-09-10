view: watch_rate {
  derived_table: {
    sql:
    WITH a AS (

    -- WEB
    SELECT
        CAST(video_id AS BIGINT) AS video_id,
        CAST(user_id AS VARCHAR(255)) AS user_id,
        'Web' AS source,
        'video_content_playing' AS event_type,
        received_at,
        TO_CHAR(received_at::TIMESTAMP, 'YYYY-MM') AS month
    FROM javascript.video_content_playing
    WHERE received_at::TIMESTAMP::DATE >= '2026-06-01'

    UNION ALL

    -- IOS
    SELECT
        CAST(video_id AS BIGINT),
        CAST(user_id AS VARCHAR(255)),
        'iOS',
        'video_content_playing',
        received_at,
        TO_CHAR(received_at::TIMESTAMP, 'YYYY-MM')
    FROM ios.video_content_playing
    WHERE received_at::TIMESTAMP::DATE >= '2026-06-01'

    UNION ALL

    -- ANDROID
    SELECT
        CAST(video_id AS BIGINT),
        CAST(user_id AS VARCHAR(255)),
        'Android',
        'video_content_playing',
        received_at,
        TO_CHAR(received_at::TIMESTAMP, 'YYYY-MM')
    FROM android.video_content_playing
    WHERE received_at::TIMESTAMP::DATE >= '2026-06-01'

    UNION ALL

    -- FIRE TV
    SELECT
        CAST(video_id AS BIGINT),
        CAST(user_id AS VARCHAR(255)),
        'FireTV',
        'video_content_playing',
        received_at,
        TO_CHAR(received_at::TIMESTAMP, 'YYYY-MM')
    FROM amazon_fire_tv.video_content_playing
    WHERE received_at::TIMESTAMP::DATE >= '2026-06-01'

    UNION ALL

    -- ROKU
    SELECT
        CAST(video_id AS BIGINT),
        CAST(user_id AS VARCHAR(255)),
        'Roku',
        'video_content_playing',
        received_at,
        TO_CHAR(received_at::TIMESTAMP, 'YYYY-MM')
    FROM roku.video_content_playing
    WHERE received_at::TIMESTAMP::DATE >= '2026-06-01'

    UNION ALL

    -- IOS FIRST PLAY
    SELECT
        CAST(video_id AS BIGINT),
        CAST(user_id AS VARCHAR(255)),
        'iOS',
        'video_content_playing',
        received_at,
        TO_CHAR(received_at::TIMESTAMP, 'YYYY-MM')
    FROM ios.firstplay
    WHERE received_at::TIMESTAMP::DATE >= '2026-06-01'

    UNION ALL

    -- ANDROID FIRST PLAY
    SELECT
        CAST(video_id AS BIGINT),
        CAST(user_id AS VARCHAR(255)),
        'Android',
        'video_content_playing',
        received_at,
        TO_CHAR(received_at::TIMESTAMP, 'YYYY-MM')
    FROM android.firstplay
    WHERE received_at::TIMESTAMP::DATE >= '2026-06-01'
),

chargebee AS (
    SELECT DISTINCT
        TO_CHAR(uploaded_at::TIMESTAMP, 'YYYY-MM') AS month,
        customer_email AS email
    FROM http_api.chargebee_subscriptions
    WHERE subscription_status = 'active'
      AND uploaded_at::TIMESTAMP::DATE >= '2026-06-01'
),

vimeo AS (
    SELECT DISTINCT
        CAST(user_id AS VARCHAR(255)) AS user_id,
        email,
        TO_CHAR(report_date::TIMESTAMP, 'YYYY-MM') AS month
    FROM customers.all_customers
    WHERE status = 'enabled'
      AND report_date::TIMESTAMP::DATE >= '2026-06-01'
),

chargebee_vimeo_id AS (
    SELECT DISTINCT
        b.user_id,
        c.month
    FROM chargebee c
    LEFT JOIN vimeo b
        ON c.email = b.email
       AND c.month = b.month
),

users AS (
    SELECT
        user_id,
        month
    FROM chargebee_vimeo_id
    WHERE user_id IS NOT NULL

    UNION

    SELECT
        user_id,
        month
    FROM vimeo
    WHERE user_id IS NOT NULL
),

matched_users AS (
    SELECT DISTINCT
        u.user_id,
        u.month,
        'yes' AS have_watched
    FROM users u
    INNER JOIN a
        ON u.user_id = a.user_id
       AND u.month = a.month
),

unmatched_users AS (
    SELECT DISTINCT
        u.user_id,
        u.month,
        'no' AS have_watched
    FROM users u
    LEFT JOIN a
        ON u.user_id = a.user_id
       AND u.month = a.month
    WHERE a.user_id IS NULL
)

SELECT
    user_id,
    month,
    have_watched
FROM matched_users

UNION ALL

SELECT
    user_id,
    month,
    have_watched
FROM unmatched_users
;;

  }

  dimension: user_id {
    type: string
    sql: ${TABLE}.user_id ;;
  }

  dimension: month {
    type: string
    sql: ${TABLE}.month ;;
  }

  dimension: have_watched {
    type: string
    sql: ${TABLE}.have_watched ;;
  }


  }
