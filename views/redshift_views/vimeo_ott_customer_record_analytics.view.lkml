view: vimeo_ott_customer_record_analytics {
  # Or, you could make this view a derived table, like this:
  derived_table: {
    sql: with analytics AS (
        SELECT
          date(sc.report_date)-1 as "timestamp",
          sc.platform,
          sc.frequency,
          COUNT(DISTINCT CASE WHEN sc.state_change = 'free_trial_created' THEN sc.user_id END) AS free_trial_created,
          COUNT(DISTINCT CASE WHEN sc.state_change = 'free_trial_converted' THEN sc.user_id END) AS free_trial_converted,
          COUNT(DISTINCT CASE WHEN sc.state_change = 'free_trial_churn' THEN sc.user_id END) AS free_trial_churn,
          COUNT(DISTINCT CASE WHEN sc.state_change = 'paying_created' THEN sc.user_id END) AS paying_created,
          COUNT(DISTINCT CASE WHEN sc.state_change = 'paying_churn' THEN sc.user_id END) AS paying_churn,
          COUNT(DISTINCT CASE WHEN sc.state_change = 'paused_created' THEN sc.user_id END) AS paused_created,
          -- Correct the calculation for total_paying and total_free_trials
          COUNT(DISTINCT CASE WHEN sc.status = 'enabled' THEN sc.user_id END) AS total_paying,
          COUNT(DISTINCT CASE WHEN sc.status = 'free_trial' THEN sc.user_id END) AS total_free_trials
        FROM
          ${vimeo_ott_customer_record.SQL_TABLE_NAME} sc
        GROUP BY
          sc.report_date,
          sc.platform,
          sc.frequency
      )
      , expanded_analytics as (
        select *
        , LAG(free_trial_created, 14) over (partition by platform, frequency order by date("timestamp")) as new_trials_14_days_prior
        , sum(paying_churn) over (partition by platform, frequency order by date("timestamp") ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) as churn_30_days
        , LAG(total_paying, 30) over (partition by platform, frequency order by date("timestamp")) as paying_30_days_prior
        from analytics
      )
      select * from expanded_analytics
      ;;
  }
}
