view: gaither_analytics_v2 {
  derived_table: {
    sql:
    ------ Source Tables ------
    with chargebee_subscriptions as (
    select * from http_api.chargebee_subscriptions),

    vimeo_subscriptions as(
    select * from customers.gaithertvplus_all_customers),

    ------  Chargebee ------
    chargebee_raw as(
    SELECT
      uploaded_at as report_date
      ,subscription_id as user_id
      ,Case
        WHEN subscription_status = 'non_renewing' THEN 'active'
        ELSE subscription_status
      END AS status
      ,'Chargebee' as platform
      ,MIN(uploaded_at) OVER (PARTITION BY subscription_id) AS created_at
      ,ROW_NUMBER() OVER (PARTITION BY subscription_id, uploaded_at ORDER BY uploaded_at DESC) AS rn
    FROM chargebee_subscriptions
    WHERE subscription_subscription_items_0_item_price_id LIKE '%GaitherTV%'
),
    chargebee_subs as(
    select
        *
    from chargebee_raw
    where rn=1.  -- select the report with most recent date for each day
),


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
      ,date(customer_created_at) as created_at
      ,date(report_date) as report_date
    from vimeo_subscriptions
    where action = 'subscription' and platform not in('api','web')
),
  result as(
  select
    user_id
    ,CAST(status AS VARCHAR(255))
    ,platform
    ,date(created_at) as created_at
    ,date(report_date) as report_date
  from chargebee_subs
  union all
  select * from vimeo_raw)

  select
  user_id
  ,status
  ,platform
  ,DATEADD(DAY, -1, created_at) as created_at
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
  END AS re_acquisitions
  ,CASE
    WHEN status in('cancelled','paused') AND LAG(status) OVER (PARTITION BY user_id ORDER BY report_date) ='active'
    THEN 'Yes'
    ELSE 'No'
  END AS cancelled_users
  from result;;
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
    sql: ${TABLE}.re_acquisitions_date ;;
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
    sql: ${TABLE}.trials_not_converted ;;
  }

  dimension: re_acquisitions {
    type: string
    sql:  ${TABLE}.re_acquisitions ;;
  }

  dimension: user_cancelled {
    type: string
    sql:  ${TABLE}.cancelled_users ;;
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


}
