view: churn_gain_historical {
  derived_table: {
    sql:

    with historical as (
      SELECT * FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY churn_gain_report_date_date,platform,billing_period
                ORDER BY received_at DESC NULLS LAST
            ) AS rn
        FROM looker.upff_churn_gain_v2
        ) AS ranked
      WHERE rn = 1
      )
      SELECT
        free_trials_converted AS user_count
        ,churn_gain_report_date_date
        ,billing_period
        ,'converted' AS status
        ,platform
      FROM historical

      UNION ALL
      SELECT
        (voluntary_churn) * -1 AS user_count
        ,churn_gain_report_date_date
        ,billing_period
        ,'churn' AS status
        ,platform
      FROM historical

      UNION ALL
      SELECT
        (paused_churn) * -1 AS user_count
        ,churn_gain_report_date_date
        ,billing_period
        ,'paused' AS status
        ,platform
      FROM historical

      UNION ALL
      SELECT
        involuntary_churn  * -1 AS user_count
        ,churn_gain_report_date_date
        ,billing_period
        ,'dunning' AS status
        ,platform
      FROM historical

      UNION ALL
      SELECT
        new_paid_count AS user_count
        ,churn_gain_report_date_date
        ,billing_period
        ,'new_paid' AS status
        ,platform
      FROM historical

      UNION ALL
      SELECT
        reacquisition AS user_count
        ,churn_gain_report_date_date
        ,billing_period
        ,'reacquisition' AS status
        ,platform
      FROM historical






      ;;
  }
}
