view: free_trials {
  derived_table: {
    sql:
    with chargebee as (

      SELECT
        date(DATEADD(HOUR, -4, received_at)) as created_at,
        content_customer_id::VARCHAR AS user_id,
        /*
        CASE
        WHEN content_subscription_billing_period_unit = 'month' THEN 'monthly'::VARCHAR
        ELSE 'yearly'::VARCHAR
        END AS billing_period,
        */
        'web'::VARCHAR AS platform
        FROM chargebee_webhook_events.subscription_created
        WHERE content_subscription_subscription_items LIKE '%Gai%'
      ),


    chargebee_bundle as (
      select count(*) as count
      ,content_customer_id as user_id
      ,DATE(DATEADD(HOUR, -4, received_at)) AS created_at
      from chargebee_webhook_events.subscription_created
      group by 2,3
      ),

    chargebee_joined as (
    SELECT
      a.user_id
      ,a.created_at
      ,b.count
    FROM chargebee a
    LEFT JOIN chargebee_bundle b
    ON a.user_id = b.user_id and a.created_at = b.created_at

    ),

     users as (
      SELECT
        created_at
        ,user_id
        ,platform
      FROM ${gaither_analytics_v2.SQL_TABLE_NAME}
      WHERE platform != 'Chargebee'
      and status in ( 'in_trial','free_trial')

      UNION ALL
      SELECT
        created_at
        ,user_id
        ,CASE
          WHEN count = 1 THEN 'web_nobundle'
          ELSE 'web_bundle'
        END AS platform
      FROM chargebee_joined
     )

      SELECT
        *
      FROM users
    ;;
    sql_trigger_value: SELECT TO_CHAR(DATEADD(minute, -545, GETDATE()), 'YYYY-MM-DD');;
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
