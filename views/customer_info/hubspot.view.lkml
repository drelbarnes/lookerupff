view: hubspot {
  derived_table: {
    sql:
    WITH hubspot AS (
    SELECT
        recipient AS email,
        created AS event_date,
        subject,
        sent_by_id,
        type,
        CASE
            WHEN email_campaign_id = 271603444 THEN 'transactional'
            ELSE 'marketing'
        END AS email_type
    FROM hubspot.email_events
    WHERE type IN ('SENT', 'OPEN', 'CLICK')
),

hubspot_most_recent AS (
    SELECT
        email,
        event_date,
        subject,
        sent_by_id,
        type,
        email_type
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY email
                ORDER BY event_date DESC
            ) AS rn
        FROM hubspot
        WHERE type = 'SENT'
    ) ranked
    WHERE rn <= 3
),

hubspot_engagement AS (
    SELECT
        sent_by_id,
        MAX(CASE WHEN type = 'OPEN' THEN event_date END) AS open_date,
        MAX(CASE WHEN type = 'CLICK' THEN event_date END) AS click_date
    FROM hubspot
    WHERE type IN ('OPEN', 'CLICK')
      AND sent_by_id IN (
          SELECT sent_by_id
          FROM hubspot_most_recent
      )
    GROUP BY sent_by_id
)

SELECT
    s.email,
    DATE(s.event_date) AS sent_date,
    s.subject,
    s.email_type,
    DATE(e.open_date) AS open_date,
    DATE(e.click_date) AS click_date
FROM hubspot_most_recent s
LEFT JOIN hubspot_engagement e
    ON s.sent_by_id = e.sent_by_id;;
  }

  dimension: email {
    type: string
    sql: ${TABLE}.email ;;
  }

  dimension: subject {
    type: string
    label: "Campaign Name"
    sql: ${TABLE}.subject ;;
  }

  dimension: email_type {
    type: string
    sql: ${TABLE}.email_type ;;
  }

  dimension: sent_date {
    type: date
    sql: ${TABLE}.sent_date ;;
  }

  dimension: click_date {
    type: date
    sql: ${TABLE}.click_date ;;
  }

  dimension: open_date {
    type: date
    sql: ${TABLE}.open_date ;;
  }
}
