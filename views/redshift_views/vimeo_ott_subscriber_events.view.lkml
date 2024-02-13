view: vimeo_ott_subscriber_events {
  derived_table: {
    sql:
    WITH purchase_events AS (
    SELECT
        user_id,
        subscription_status,
        event,
        platform,
        COALESCE(NULLIF(subscription_frequency, 'custom'), 'monthly') AS subscription_frequency,
        "timestamp"
    FROM
        ${vimeo_ott_webhook_events.SQL_TABLE_NAME}
    WHERE
        event <> 'customer_created'
    ),
    max_events AS (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY user_id, platform, "timestamp"::date ORDER BY "timestamp" DESC) AS rn
        FROM
            purchase_events
    ),
    distinct_events AS (
        SELECT
            user_id,
            subscription_status,
            event,
            platform,
            subscription_frequency,
            "timestamp"::date AS "date",
            "timestamp",
            LAG(event) OVER (PARTITION BY user_id, platform ORDER BY "timestamp") AS previous_event
        FROM
            max_events
        WHERE
            rn = 1
    ),
    charge_failed_flags as (
      select *
      , CASE WHEN event = 'customer_product_charge_failed' and previous_event = 'customer_product_charge_failed' THEN TRUE ELSE FALSE END AS charge_failed_flag
      from distinct_events
    ),
    users AS (
        SELECT
            user_id,
            platform,
            MIN("timestamp"::date) AS min_date,
            current_date AS max_date
        FROM
            purchase_events
        GROUP BY
            user_id, platform
    ),
    dates AS (
        SELECT
            DISTINCT "timestamp"::date AS "date"
        FROM
            ${vimeo_ott_webhook_events.SQL_TABLE_NAME}
    ),
    exploded_dates_per_user AS (
        SELECT
            u.user_id,
            u.platform,
            d."date"
        FROM
            users u
            CROSS JOIN dates d
        WHERE
            d."date" BETWEEN u.min_date AND u.max_date
    ),
    join_events AS (
        SELECT
            ed."date",
            ed.user_id,
            de."timestamp",
            de.subscription_status,
            de.event,
            de.previous_event,
            de.platform,
            de.subscription_frequency
        FROM
            exploded_dates_per_user ed
            LEFT JOIN (select * from charge_failed_flags where charge_failed_flag is false) de ON ed.user_id = de.user_id AND ed."date" = de."date" AND ed.platform = de.platform
    )
      , status_groups AS (
        SELECT
            *,
            COUNT(subscription_status) OVER (PARTITION BY user_id ORDER BY "date", "timestamp" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS status_group
        FROM
            join_events
      ),
      customer_record_p0 AS (
          SELECT
              "date",
              user_id,
              LAST_VALUE("timestamp" IGNORE NULLS) OVER (PARTITION BY user_id, status_group) AS event_timestamp,
              LAST_VALUE(subscription_status IGNORE NULLS) OVER (PARTITION BY user_id, status_group) AS subscription_status,
              LAST_VALUE(event IGNORE NULLS) OVER (PARTITION BY user_id, status_group) AS event,
              LAST_VALUE(previous_event IGNORE NULLS) OVER (PARTITION BY user_id, status_group) AS previous_event,
              LAST_VALUE(platform IGNORE NULLS) OVER (PARTITION BY user_id, status_group) AS platform,
              LAST_VALUE(subscription_frequency IGNORE NULLS) OVER (PARTITION BY user_id, status_group) AS subscription_frequency,
              status_group
          FROM
              status_groups
          GROUP BY
              "date", "timestamp", subscription_status, event, previous_event, platform, subscription_frequency, user_id, status_group
      ),
      customer_record AS (
          SELECT
            user_id,
            "date",
            event_timestamp,
            subscription_status,
            event,
            previous_event,
            platform,
            subscription_frequency
          FROM
            customer_record_p0
          group by 1,2,3,4,5,6,7,8
      )
      select *, MD5(user_id || platform || event_timestamp::text) AS unique_id from customer_record where "date" is not null
       ;;
      datagroup_trigger: upff_acquisition_reporting
      distribution_style: all
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension_group: timestamp {
    type: time
    sql: ${TABLE}.timestamp ;;
  }

  dimension: date {
    type: date
    sql: ${TABLE}.date ;;
  }

  dimension: user_id {
    type: string
    sql: ${TABLE}.user_id ;;
  }

  dimension: subscription_status {
    type: string
    sql: ${TABLE}.subscription_status ;;
  }

  dimension: event {
    type: string
    sql: ${TABLE}.event ;;
  }

  dimension: previous_event {
    type: string
    sql: ${TABLE}.previous_event ;;
  }

  dimension: platform {
    type: string
    sql: ${TABLE}.platform ;;
  }

  dimension: subscription_frequency {
    type: string
    sql: ${TABLE}.subscription_frequency ;;
  }

  dimension: row {
    type: number
    primary_key: yes
    sql: ${TABLE}.row ;;
  }

  set: detail {
    fields: [
      timestamp_date,
      user_id,
      subscription_status,
      event,
      platform,
      subscription_frequency,
      row
    ]
  }
}
