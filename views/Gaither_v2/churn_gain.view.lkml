view: churn_gain {
  derived_table: {
    sql:
      ,get_churn AS (
      SELECT * FROM ${churn.SQL_TABLE_NAME}
      ),

      re_acquisitions AS ( select * from ${reacquisition.SQL_TABLE_NAME}
      ),



      get_sub_count as (
        SELECT * FROM  ${sub_count.SQL_TABLE_NAME}
      ),

      trial_conversion AS
      (select * from ${trial_converted.SQL_TABLE_NAME}
      ),

      free_trial as
      (select
      COUNT(DISTINCT user_id)
      ,created_at as report_date
      ,platform
      from ${free_trials.SQL_TABLE_NAME}
      GROUP BY 2,3
      ),

      converted_sum as (
      SELECT
        SUM(user_count) as user_count
        ,report_date
      FROM trial_conversion
      group by 2
      ),
      rolling_converted as (
        SELECT
            report_date,
            SUM(user_count)
              OVER (
                ORDER BY report_date
                ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
              ) AS rolling_converted
        FROM converted_sum
      ),

      dunning AS (
      SELECT
      content_subscription_id::VARCHAR AS user_id,
      'charge_failed'::VARCHAR AS status,
      /*CASE
      WHEN content_subscription_billing_period_unit = 'month' THEN 'monthly'::VARCHAR
      ELSE 'yearly'::VARCHAR
      END AS billing_period, */
      DATE("timestamp") AS report_date,
      'web'::VARCHAR AS platform
      FROM chargebee_webhook_events.subscription_cancelled
      WHERE (content_subscription_cancel_reason_code in ('Not Paid', 'No Card', 'Fraud Review Failed', 'Non Compliant EU Customer', 'Tax Calculation Failed', 'Currency incompatible with Gateway', 'Non Compliant Customer') and (content_subscription_cancelled_at - content_subscription_activated_at) > 1900800) AND content_subscription_subscription_items LIKE '%Gai%'
      ),

      dunning_count as (
        SELECT
          COUNT(DISTINCT user_id) as user_count
          ,report_date
          ,platform
        FROM dunning
        GROUP BY 2,3
      ),
      result as (

      SELECT
      *
      ,'reacquisition'AS status
      FROM re_acquisitions

      UNION ALL
      SELECT
       *
      ,'converted'AS status
      FROM trial_conversion

      /*
      UNION ALL
      SELECT
       rolling_converted as user_count
      ,
      ,'rolling_conversion'AS status
      FROM rolling_converted
      */
      UNION ALL
      SELECT
        user_count
        ,report_date
        ,platform
        ,'churn' as status
      FROM get_churn

      UNION ALL
      SELECT
        rolling_churn_30_days as user_count
        ,report_date
        ,platform
        ,'rolling_churn' as status
      FROM churn

      UNION ALL
      SELECT
        prior_31_days_subs as user_count
        ,report_date
        ,platform
        ,'rolling_total' as status
      FROM get_sub_count

      UNION ALL
      SELECT
         user_count
        ,report_date
        ,platform
        ,'total' as status
      FROM get_sub_count


      UNION ALL
      SELECT
    user_count
    ,month_max_date as report_date
    ,'all' as platform
    ,'month_end'as status
FROM (
    SELECT
        user_count,
        report_date,
        MAX(report_date) OVER (
            PARTITION BY DATE_TRUNC('month', report_date)
        ) AS month_max_date
    FROM get_sub_count
) t
WHERE report_date = month_max_date

      UNION ALL
      SELECT
        *
        ,'dunning'as status
      FROM dunning_count

      UNION ALL
      SELECT
        *
        ,'free_trial' as status
      FROM free_trial
        ),

    spend as (
      SELECT
      *
      FROM ${cpft.SQL_TABLE_NAME}
    ),

    daily_spend AS (
  SELECT
      date_start,
      SUM(spend) AS spend
  FROM spend
  GROUP BY date_start
),
    rolling_spend as (
    SELECT
            date_start as report_date,
            SUM(spend)
              OVER (
                ORDER BY date_start
                ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
              ) AS rolling_spend
              from daily_spend
    ),

    cpa as (
    SELECT
    a.rolling_spend/b.rolling_converted as cpa
    ,a.report_date
    FROM rolling_spend a
    LEFT JOIN rolling_converted b
    on a.report_date = b.report_date
    )
      SELECT
    a.user_count,
    a.report_date,
    a.platform,
    a.status,
    (
      SELECT cpa
      FROM cpa b
      WHERE b.report_date = a.report_date
    ) AS cpa
FROM result a
    ;;
    sql_trigger_value: SELECT TO_CHAR(DATEADD(minute, -645, GETDATE()), 'YYYY-MM-DD');;
    #sql_trigger_value:  SELECT TO_CHAR(DATE_TRUNC('day', CURRENT_TIMESTAMP) + INTERVAL '9 hours 45 minutes', 'YYYY-MM-DD');;
    distribution: "report_date"
    sortkeys: ["report_date"]
  }
  dimension: date {
    type: date
    primary_key: yes
    sql:  ${TABLE}.report_date ;;
  }
  dimension_group: report_date {
    type: time

    timeframes: [date, week,month]
    sql: ${TABLE}.report_date ;;
    convert_tz: yes  # Adjust for timezone conversion if needed
  }



  dimension: user_id {
    type: string
    sql: ${TABLE}.user_id ;;
    tags: ["user_id"]
  }

  dimension: user_count {
    type: number
    sql: ${TABLE}.user_count ;;
  }



  dimension: platform{
    type: string
    sql: ${TABLE}.platform ;;
  }

  dimension: status{
    type: string
    sql: ${TABLE}.status ;;
  }

  measure: paid_total {
    type: sum
    sql: ${user_count} ;;
    filters: [status: "total"]
  }

  measure: cpa {
    type: max
    sql: ${TABLE}.cpa;;
  }
  measure: free_trial {
    type: sum
    sql: ${user_count} ;;
    filters: [status: "free_trial"]
  }

  measure: churn_count {
    type: sum
    sql: ${user_count} ;;
    filters: [status: "churn"]
  }
  measure: dunning_count {
    type: sum
    sql: ${user_count} ;;
    filters: [status: "dunning"]

  }
  measure: converted_count {
    type: sum
    sql: ${user_count} ;;
    filters: [status: "converted"]

  }

  measure: reacquisition_count {
    type: sum
    sql: ${user_count} ;;
    filters: [status: "reacquisition"]

  }

  measure: rolling_churn_count {
    type: sum
    sql: ${user_count} ;;
    filters: [status: "rolling_churn"]

  }

  measure: rolling_total_count {
    type: sum
    sql: ${user_count} ;;
    filters: [status: "rolling_total"]

  }

  measure: month_end_count {
    type: sum
    sql: ${user_count} ;;
    filters: [status: "month_end"]

  }


  }
