view: free_trials {
  derived_table: {
    sql:
      ,users as (
      SELECT
        created_at
        ,user_id
        ,platform
      FROM ${gaither_analytics_v2.SQL_TABLE_NAME}
      WHERE platform != 'Chargebee'
      and status in ( 'in_trial','free_trial')

      UNION ALL

      SELECT
        date(DATEADD(HOUR, -4, received_at)) as created_at,
        content_subscription_id::VARCHAR AS user_id,
        /*
        CASE
        WHEN content_subscription_billing_period_unit = 'month' THEN 'monthly'::VARCHAR
        ELSE 'yearly'::VARCHAR
        END AS billing_period,
        */
        'web'::VARCHAR AS platform
        FROM chargebee_webhook_events.subscription_created
        WHERE content_subscription_subscription_items LIKE '%Gai%'
      )

      SELECT
        *
      FROM users
    ;;
    sql_trigger_value: SELECT TO_CHAR(DATEADD(minute, -555, GETDATE()), 'YYYY-MM-DD');;
    #sql_trigger_value:  SELECT TO_CHAR(DATE_TRUNC('day', CURRENT_TIMESTAMP) + INTERVAL '9 hours 45 minutes', 'YYYY-MM-DD');;
    distribution: "created_at"
    sortkeys: ["created_at"]
  }
  dimension: user_id {
    type: string
    sql:  ${TABLE}.user_id ;;
  }

  dimension: platform {
    type: string
    sql:  ${TABLE}.platform ;;
  }

  dimension: created_at {
    type: date
    sql: ${TABLE}.created_at ;;
  }

  measure: total_free_trials {
    type: count_distinct
    sql: ${TABLE}.user_id  ;;
  }
}
