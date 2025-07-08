view: ltv {
  derived_table: {
    sql:  with chargebee_subscriptions as (
    select * from `up-faith-and-family-216419.http_api.chargebee_subscriptions` ),

      -- get daily status of each user
      chargebee_raw as(
      SELECT
      date(uploaded_at) as report_date
      ,subscription_id as user_id
      ,Case
      WHEN subscription_status = 'non_renewing' THEN 'active'
      ELSE subscription_status
      END AS status
      ,'Chargebee' as platform
      ,ROW_NUMBER() OVER (PARTITION BY subscription_id, uploaded_at ORDER BY uploaded_at DESC) AS rn
      ,CASE
        WHEN subscription_subscription_items_0_item_price_id like '%Yearly%' THEN 'year'
        ELSE 'month'
      END AS sub_type
      FROM chargebee_subscriptions
      WHERE subscription_subscription_items_0_item_price_id LIKE '%UP%' --AND user_id = '169lVgUM2Z9p4XpWB'

      ),
      chargebee_subs as(
      select
      *
      from chargebee_raw
      where rn=1.  -- select the report with most recent date for each day
      ),
      -- get trial start date for each user
      chargebee_trial_start as (
      SELECT
      --subtract 5 hour delay to get actual time
      TIMESTAMP_SUB(received_at, INTERVAL 5 HOUR) AS created_at

      -- add_day will be used as condition to add one more day for gap due to time difference
      ,CASE
      WHEN EXTRACT(HOUR FROM received_at)<5 THEN 'Yes'
      ELSE 'No'
      END AS add_day
      ,content_subscription_id as user_id
      ,CASE
        WHEN content_subscription_subscription_items like '%Monthly%' THEN 'month'
        else 'year'
      END AS sub_type
      FROM `up-faith-and-family-216419.chargebee_webhook_events.subscription_created`
      WHERE content_subscription_subscription_items like '%UP%'
      ),
      -- get trial convertion data for each user
      chargebee_trial_converted as(
      SELECT
      date(received_at) as report_date
      ,content_subscription_id as user_id
      ,'Yes' as trials_converted
      FROM `up-faith-and-family-216419.chargebee_webhook_events.subscription_activated`
      WHERE content_subscription_subscription_items like '%UP%'

      ),
      -- get sub cancelled data for each user
      chargebee_sub_cancelled as(
      SELECT
      date(received_at) as report_date
      ,content_subscription_id as user_id
      ,'Yes' as sub_cancelled
      FROM `up-faith-and-family-216419.chargebee_webhook_events.subscription_cancelled`
      WHERE content_subscription_subscription_items like '%UP%'
      ),
      -- get re-acquition data for each user
      chargebee_re_acquisition as(
      SELECT
      date(received_at) as report_date
      ,content_subscription_id as user_id
      ,'Yes' as re_acquisition
      ,date(received_at) as re_acquisition_date
      FROM `up-faith-and-family-216419.chargebee_webhook_events.subscription_reactivated`
      WHERE content_subscription_subscription_items like '%UP%'
      ),

      --left join trial start data to daily report for created date
      join_trial_start as(
      SELECT
      a.*
      ,b.created_at
      ,b.add_day
      FROM chargebee_subs as a
      LEFT JOIN chargebee_trial_start b
      ON a.user_id = b.user_id
      ),
      -- manually add data for trial start date, daily report does not include this
      join_trial_start2 as(
      SELECT
      CAST(report_date AS DATE) AS report_date
      ,user_id
      ,status
      , platform
      , sub_type
      ,CAST(created_at AS DATE) AS created_at
      FROM join_trial_start
      UNION ALL
      -- join day for trial start date
      SELECT
      CAST(created_at AS DATE) as report_date
      ,user_id
      ,'in_trial' as status
      ,'Chargebee' as platform
      , sub_type
      ,CAST(created_at AS DATE) AS created_at
      FROM chargebee_trial_start
      UNION ALL
      -- join day for trial start date (gap due to time difference)
      SELECT
      CAST(DATE_ADD(created_at, INTERVAL 1 DAY) AS DATE) AS report_date

      ,user_id
      ,'in_trial' as status
      ,'Chargebee' as platform
      , sub_type
      ,CAST(created_at AS DATE) AS created_at
      FROM chargebee_trial_start
      WHERE add_day = 'Yes'

      ),
      -- join data for trial conversion
      join_trial_converted as (
      SELECT
      a.*
      ,b.trials_converted
      FROM join_trial_start2 a
      LEFT JOIN chargebee_trial_converted b
      on a.user_id = b.user_id and a.report_date = b.report_date
      ),
      -- join data for sub cancelled
      join_sub_cancelled as(
      SELECT
      a.*
      ,b.sub_cancelled
      FROM join_trial_converted a
      LEFT JOIN chargebee_sub_cancelled b
      on a.user_id = b.user_id and a.report_date = b.report_date
      ),
      -- join data for re-acquisition
      join_re_acquisition as(
      SELECT
      a.*
      ,b.re_acquisition
      ,b.re_acquisition_date
      FROM join_sub_cancelled a
      LEFT JOIN chargebee_re_acquisition b
      on a.user_id = b.user_id and a.report_date = b.report_date
      ),

      -- mark trial as not converted for subs that got cancelled due to failed charge 7 days after trial ended
      trial_not_converted_dt as(
      SELECT
      *,
      --CASE
      --WHEN  ((DATEDIFF(DAY, date(report_date),date(created_at)) = -21) or (DATEDIFF(DAY, date(report_date),date(created_at)) = -14)) and sub_cancelled IS NOT NULL THEN 'Yes'
      --ELSE 'No'
      --END AS charge_failed
      CASE
      WHEN DATE_DIFF(DATE(created_at), DATE(report_date), DAY) = -7 AND sub_cancelled IS NOT NULL THEN 'Yes'

      ELSE NULL
      END AS trial_not_converted
      FROM join_re_acquisition
      ),
      undo_wrong_subs as (
      SELECT
      date(report_date) as report_date
      ,user_id
      -- fill null values for status
      ,CASE
      WHEN trials_converted is not NULL THEN 'active'
      ELSE status
      END AS status
      ,platform
      ,sub_type
      ,date(created_at) as created_at
      -- fill re-acquisition date using report date, re-acquisition column has to be yes to use this date
      ,CASE
      WHEN re_acquisition_date is NULL THEN report_date
      ELSE re_acquisition_date
      END AS re_acquisition_date
      -- fill null values for trials_converted
      ,CASE
      WHEN trials_converted is NULL THEN 'No'
      ELSE 'Yes'
      END as trials_converted
      -- fill null values for trial_not_converted
      ,CASE
      WHEN trial_not_converted is NULL THEN 'No'
      --WHEN trial_not_converted ='Yes' THEN 'No'
      ELSE 'Yes'
      END as trial_not_converted
      -- undo subs that were marked as cancelled but its actually trials not converted
      ,CASE
      WHEN trial_not_converted is not NULL and sub_cancelled IS NOT NULL THEN 'No'
      WHEN sub_cancelled IS not NULL and trial_not_converted is NULL THEN 'Yes'
      ELSE 'No'
      END AS sub_cancelled
      -- fill null values for re_aqcquisition
      ,CASE
      WHEN re_acquisition is NULL THEN 'No'
      ELSE 'Yes'
      END AS re_acquisition
      FROM trial_not_converted_dt

      ),
      -- create column to mark if user failed to pay after trial conversion
      mark_trials_converted as (
      SELECT *
      ,LAG(trials_converted, 14) OVER (PARTITION BY user_id ORDER BY report_date) AS trials_converted_14_days_ago
      FROM undo_wrong_subs

      ),
      -- create column to mark if user failed to pay after trial conversion
      mark_charge_failed as (
      SELECT *
      ,CASE
      WHEN sub_cancelled = 'Yes' AND trials_converted_14_days_ago = 'Yes' THEN 'Yes'
      ELSE 'No'
      END AS charge_failed
      FROM mark_trials_converted
      ),

      undo_trials_converted as (
      SELECT
      report_date
      ,user_id
      ,status
      ,platform
      ,sub_type
      ,charge_failed
      ,CASE
      WHEN created_at is NULL THEN '1999-01-01'
      ELSE created_at
      END as created_at
      ,re_acquisition_date
      ,CASE
      WHEN MAX(charge_failed) OVER (PARTITION BY user_id) = 'Yes' THEN 'No'
      ELSE trials_converted
      END AS trials_converted
      ,CASE
      WHEN MAX(charge_failed) OVER (PARTITION BY user_id) = 'Yes'  and trials_converted = 'Yes' THEN 'Yes'
      ELSE trial_not_converted
      END AS trial_not_converted
      ,sub_cancelled
      ,re_acquisition


      FROM mark_charge_failed where report_date >='2024-04-01'

      )
      select * from undo_trials_converted;;}

  dimension: user_id {
    type: string
    label: "subscription_id"
    sql:  ${TABLE}.user_id ;;
  }
  dimension: report_date {
    type: string
    sql:  ${TABLE}.report_date ;;
  }
  dimension: created_at {
    type: string
    sql:  ${TABLE}.created_at ;;
  }

  dimension: sub_type {
    type: string
    sql:  ${TABLE}.sub_type;;
    }

    dimension: status {
    type: string
    sql:  ${TABLE}.status;;
    }
  dimension: charge_failed {
    type: string
    sql:  ${TABLE}.charge_failed;;
  }
  dimension: trials_converted {
    type: string
    sql:  ${TABLE}.trials_converted;;
  }
  dimension: trial_not_converted {
    type: string
    sql:  ${TABLE}.trial_not_converted;;
  }
  measure: count_converted {
    type: count_distinct
    # for Chargebee : active,non_rewing
    # for Vimeo : enabled
    filters: [trials_converted: "Yes"]
    sql:${TABLE}.user_id   ;;
  }

  measure: count_not_converted {
    type: count_distinct
    # for Chargebee : active,non_rewing
    # for Vimeo : enabled
    filters: [trial_not_converted: "Yes"]
    sql:${TABLE}.user_id   ;;
  }

  }
