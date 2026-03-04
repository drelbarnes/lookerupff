view: reacquisition {
  derived_table: {
    sql:
      with users as (
        SELECT
          content_subscription_id as user_id
          ,'web' as platform
          /*
          ,CASE
            WHEN content_subscription_billing_period_unit = 'month' THEN 'monthly'
            ELSE 'yearly'
          END AS billing_period
          */
          ,date(DATEADD(HOUR, -5, timestamp)) as report_date

        FROM chargebee_webhook_events.subscription_reactivated
        WHERE content_subscription_subscription_items like '%Gai%'

        UNION ALL

        SELECT
            content_subscription_id AS user_id
            ,'web' AS platform
            /*
            ,CASE
              WHEN content_subscription_billing_period_unit = 'month' THEN 'monthly'
              ELSE 'yearly'
            END AS billing_period
*/
            ,date(DATEADD(HOUR, -5, timestamp)) AS report_date
        FROM chargebee_webhook_events.subscription_resumed
        WHERE content_subscription_subscription_items LIKE '%Gai%'

        UNION ALL

        SELECT
          user_id
          ,platform
          ,report_date
        FROM ${gaither_analytics_v2.SQL_TABLE_NAME}
        WHERE platform != 'Chargbee'
        and re_acquisition = 'Yes'
      )

      SELECT
        count(distinct user_id)
        ,report_date
        ,platform
      FROM users
      GROUP BY 1,2
    ;;
  }
}
