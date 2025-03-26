view: gaither_analytics_v2 {
  derived_table: {
    sql:
    with chargebee_subscriptions as (
    select * from http_api.chargebee_subscriptions),

    vimeo_subscriptions as(
    select * from customers.gaithertvplus_all_customers),

    ------  Chargebee ------
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
    FROM chargebee_subscriptions
    WHERE subscription_subscription_items_0_item_price_id LIKE '%Gaither%'
),
    chargebee_subs as(
    select
        *
    from chargebee_raw
    where rn=1.  -- select the report with most recent date for each day
),
    chargebee_trial_start as (
    SELECT
      --subtract 4 hour delay from actual to storage
      date(DATEADD(HOUR, -4, received_at)) as created_at
      ,content_subscription_id as user_id
    FROM chargebee_webhook_events.subscription_created
    WHERE content_subscription_subscription_items like '%Gaither%'
),
    chargebee_trial_converted as(
    SELECT
      date(received_at) as report_date
      ,content_subscription_id as user_id
      ,'Yes' as trials_converted
    FROM chargebee_webhook_events.subscription_activated
    WHERE content_subscription_subscription_items like '%Gaither%'

),
    chargebee_sub_cancelled as(
    SELECT
      date(received_at) as report_date
      ,content_subscription_id as user_id
      ,'Yes' as sub_cancelled
    FROM chargebee_webhook_events.subscription_cancelled
    WHERE content_subscription_subscription_items like '%Gaither%'
),
    chargebee_re_acquisition as(
    SELECT
      date(received_at) as report_date
      ,content_subscription_id as user_id
      ,'Yes' as re_acquisition
      ,date(received_at) as re_acquisition_date
    FROM chargebee_webhook_events.subscription_reactivated
    WHERE content_subscription_subscription_items like '%Gaither%'
),


    join_trial_start as(
    SELECT
      a.*
      ,b.created_at
    FROM chargebee_subs as a
    LEFT JOIN chargebee_trial_start b
    ON a.user_id = b.user_id
),

    join_trial_converted as (
    SELECT
      a.*
      ,b.trials_converted
    FROM join_trial_start a
    LEFT JOIN chargebee_trial_converted b
    on a.user_id = b.user_id and a.report_date = b.report_date
),

    join_sub_cancelled as(
    SELECT
      a.*
      ,b.sub_cancelled
    FROM join_trial_converted a
    LEFT JOIN chargebee_sub_cancelled b
    on a.user_id = b.user_id and a.report_date = b.report_date
),
    join_re_acquisition as(
    SELECT
      a.*
      ,b.re_acquisition
      ,b.re_acquisition_date
    FROM join_sub_cancelled a
    LEFT JOIN chargebee_re_acquisition b
    on a.user_id = b.user_id and a.report_date = b.report_date
),



  trial_not_converted as(
  SELECT
    *,
  --CASE
    --WHEN  ((DATEDIFF(DAY, date(report_date),date(created_at)) = -21) or (DATEDIFF(DAY, date(report_date),date(created_at)) = -14)) and sub_cancelled IS NOT NULL THEN 'Yes'
    --ELSE 'No'
  --END AS charge_failed
  CASE
    WHEN  ((DATEDIFF(DAY, date(report_date),date(created_at)) = -7) and sub_cancelled IS NOT NULL) THEN 'Yes'
    ELSE NULL
  END AS trial_not_converted
  FROM join_re_acquisition
),
-- undo subs that were marked as cancelled but its actually trials not converted
  undo_wrong_subs as (
  SELECT
    date(report_date) as report_date
    ,user_id
    ,status
    ,platform
    ,date(created_at) as created_at
    ,CASE
      WHEN re_acquisition_date is NULL THEN report_date
      ELSE re_acquisition_date
    END AS re_acquisition_date
    ,CASE
      WHEN trials_converted is NULL THEN 'No'
      ELSE 'Yes'
    END as trials_converted
    ,CASE
      WHEN trial_not_converted is NULL THEN 'No'
      ELSE 'Yes'
    END as trial_not_converted
    ,CASE
      WHEN trial_not_converted is not NULL and sub_cancelled IS NOT NULL THEN 'No'
      WHEN sub_cancelled IS not NULL and trial_not_converted is NULL THEN 'Yes'
      ELSE 'No'
    END AS sub_cancelled
    ,CASE
      WHEN re_acquisition is NULL THEN 'No'
      ELSE 'Yes'
    END AS re_acquisition
    FROM trial_not_converted

),
  mark_trials_converted as (
    SELECT *
    ,LAG(trials_converted, 14) OVER (PARTITION BY user_id ORDER BY report_date) AS trials_converted_14_days_ago
    FROM undo_wrong_subs

),

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
      ,created_at
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
      ,charge_failed

    FROM mark_charge_failed
),


-- unmark subs where it was marked as cancelled, but its actually trial not converted

    ------ Vimeo OTT ------
    vimeo_raw as (
    select
      CAST(user_id AS VARCHAR(255))
      ,CASE
        WHEN status = 'free_trial' THEN 'in_trial'
        WHEN status = 'expired' THEN 'paused'
        WHEN status = 'enabled' THEN 'active'
        ELSE status
      END AS status
      ,platform
      ,date(MIN(report_date) OVER (PARTITION BY user_id)) AS created_at
      ,date(report_date) as report_date
    from vimeo_subscriptions
    where action = 'subscription' and platform not in('api','web')
),

  result2 as (select
  user_id
  ,status
  ,platform
  ,created_at
  ,DATEADD(DAY, 0, report_date) as report_date
  ,DATEADD(DAY, -1, report_date) as re_acquisitions_date
  ,CASE
    WHEN status = 'active' AND LAG(status) OVER (PARTITION BY user_id ORDER BY report_date) ='in_trial'
    THEN 'Yes'
    ELSE 'No'
  END AS trials_converted
  ,CASE
    WHEN status in('cancelled','paused') AND LAG(status) OVER (PARTITION BY user_id ORDER BY report_date) ='in_trial'
    THEN 'Yes'
    ELSE 'No'
  END AS trials_not_converted
  ,CASE
    WHEN status = 'active' AND LAG(status) OVER (PARTITION BY user_id ORDER BY report_date) ='paused'
    THEN 'Yes'
    ELSE 'No'
  END AS re_acquisition
  ,CASE
    WHEN status in('cancelled','paused') AND LAG(status) OVER (PARTITION BY user_id ORDER BY report_date) ='active'
    THEN 'Yes'
    ELSE 'No'
  END AS sub_cancelled

  from vimeo_raw),
  result3 as(
  select *,
  CASE
    WHEN((DATEDIFF(DAY, date(report_date),date(created_at)) = -21) or (DATEDIFF(DAY, date(report_date),date(created_at)) = -20)) and sub_cancelled = 'Yes' THEN 'Yes'
    ELSE 'No'
END AS charge_failed
from result2)

,final as(
SELECT
  user_id,
  status,
  platform,
  date(created_at) as created_at,
  date(report_date) as report_date,
  date(re_acquisitions_date) as re_acquisition_date,

  -- Fix for trials_converted logic
  CASE
    WHEN trials_converted = 'Yes'
      AND user_id IN (
        SELECT user_id
        FROM result3
        WHERE charge_failed = 'Yes'
      )
    THEN 'No'::VARCHAR
    ELSE trials_converted::VARCHAR
  END AS trials_converted,

  -- Fix for trials_not_converted logic
  CASE
    WHEN trials_converted = 'Yes'
      AND user_id IN (
        SELECT user_id
        FROM result3
        WHERE charge_failed = 'Yes'
      )
    THEN 'Yes'::VARCHAR
    ELSE trials_not_converted::VARCHAR
  END AS trial_not_converted,

  re_acquisition,
  CASE
    WHEN sub_cancelled = 'Yes'
      AND user_id IN (
        SELECT user_id
        FROM result3
        WHERE charge_failed = 'Yes'
      )
    THEN 'No'::VARCHAR
    ELSE sub_cancelled::VARCHAR
  END AS sub_cancelled
  ,charge_failed

FROM result3
),
final_join as (
select
  report_date
  ,user_id
  ,status
  ,platform
  ,created_at
  ,re_acquisition_date
  ,trials_converted
  ,trial_not_converted
  ,sub_cancelled
  ,re_acquisition
  ,charge_failed
  from undo_trials_converted
UNION ALL
select
  report_date
  ,user_id
  ,status
  ,platform
  ,created_at
  ,re_acquisition_date
  ,trials_converted
  ,trial_not_converted
  ,sub_cancelled
  ,re_acquisition
  ,charge_failed
  from final)
select *
  from final_join;;
  }

  dimension: date {
    type: date
    sql:  ${TABLE}.report_date ;;
  }
  dimension_group: report_date {
    type: time
    timeframes: [date, week]
    sql: ${TABLE}.report_date ;;
    convert_tz: yes  # Adjust for timezone conversion if needed
  }

  dimension: re_acquisitions_date {
    type: date
    sql: ${TABLE}.re_acquisition_date ;;
  }

  dimension: user_id {
    type: string
    sql:  ${TABLE}.user_id ;;
  }

  dimension: status {
    type:  string
    sql: ${TABLE}.status ;;
  }

  dimension: platform {
    type: string
    sql: ${TABLE}.platform ;;
  }
  dimension: created_at {
    type: date
    sql: ${TABLE}.created_at ;;
  }

  dimension: trials_converted {
    type: string
    sql: ${TABLE}.trials_converted ;;
  }

  dimension: trials_not_converted {
    type: string
    sql: ${TABLE}.trial_not_converted ;;
  }

  dimension: re_acquisitions {
    type: string
    sql:  ${TABLE}.re_acquisition ;;
  }

  dimension: user_cancelled {
    type: string
    sql:  ${TABLE}.sub_cancelled ;;
  }

  dimension: charge_failed {
    type: string
    sql:  ${TABLE}.charge_failed ;;
  }

  measure: total_paying {
    type: count_distinct
    # for Chargebee : active,non_rewing
    # for Vimeo : enabled
    filters: [status: "active,non_renewing,enabled"]
    sql:${TABLE}.user_id   ;;
  }

  measure: total_free_trials {
    type: count_distinct
    # for Chargebee : in_trial
    # for Vimeo : free_trial
    filters: [status: "in_trial,free_trial"]
    sql: ${TABLE}.user_id  ;;
  }

  measure: trials_converted_count {
    type: count_distinct
    filters: [trials_converted: "Yes"]
    sql: ${TABLE}.user_id  ;;
  }
  measure: trials_not_converted_count {
    type: count_distinct
    filters: [trials_not_converted: "Yes"]
    sql: ${TABLE}.user_id  ;;
  }

  measure: re_acquisitions_count {
    type: count_distinct
    filters: [re_acquisitions: "Yes"]
    sql: ${TABLE}.user_id  ;;
  }

  measure: user_cancelled_count {
    type: count_distinct
    filters: [user_cancelled: "Yes"]
    sql: ${TABLE}.user_id  ;;
  }

  measure: charge_failed_count {
    type: count_distinct
    filters: [charge_failed: "Yes"]
    sql: ${TABLE}.user_id  ;;
  }


}
