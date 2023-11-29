view: chargebee_analytics {
  derived_table: {
    sql: with webhook_events as (
        select * from ${chargebee_webhook_events.SQL_TABLE_NAME}
      )
      , p0 as (
        SELECT
        uploaded_at
        , subscription_id
        , customer_id
        , a.subscription_status as status
        , subscription_subscription_items_0_object
        , subscription_subscription_items_0_item_type
        , subscription_subscription_items_0_unit_price
        , subscription_subscription_items_0_item_price_id
        , subscription_subscription_items_1_object
        , subscription_subscription_items_1_item_type
        , subscription_subscription_items_1_unit_price
        , subscription_subscription_items_1_item_price_id
        , row_number() over (partition by subscription_id,uploaded_at order by uploaded_at desc, b.timestamp desc) as rn
        , b.*
        FROM `up-faith-and-family-216419.chargebee.subscriptions` a
        left join webhook_events b
        on a.customer_id = b.user_id and a.uploaded_at = date(b.timestamp)
      )
      , p1 as (
        select
        *
        , lag(subscription_status,1) over (partition by subscription_id order by uploaded_at) as previous_status
        from p0
      )
      , p2 as (
        select *
        , sum(case when (rn=1 and subscription_status = previous_status) then 0 else 1 end) over (partition by subscription_id order by uploaded_at) as status_group
        from p1
      )
      , customer_record as (
          select
          uploaded_at
          , subscription_id
        , max(subscription_status) over (partition by subscription_id, status_group) as status
        , sum(ifnull(status_group / nullif(status_group,0),1)) over (partition by subscription_id, status_group order by uploaded_at) as days_at_status
        , count(status_group) over (partition by subscription_id, status_group) as total_days_at_status
        from p2
      )
      , customer_record_analytics as (
        select
        uploaded_at
        ,count(case when days_at_status = 1 and status = "in_trial" then status end) as free_trial_created
        -- , count(case when days_at_status = 1 and status = "" then status end) as free_trial_churn
        -- , count(case when days_at_status = 1 and status = "" then status end) as paying_created
        , count(case when days_at_status = 1 and status = "active" then status end) as free_trial_converted
        , count(case when days_at_status = 1 and status = "cancelled" then status end) as paying_churn
        , count(case when (status = 'active' or status = 'non_renewing') then 1 else null end) as total_paying
        , count(case when (status = 'in_trial') then 1 else null end) as total_free_trials
        from customer_record
        group by 1 order by 1
      )
    , totals as (
      select
      uploaded_at
      , count(case when (status = 'active' or status = 'non_renewing') then 1 else null end) as total_paying
      , count(case when (status = 'in_trial') then 1 else null end) as total_free_trials
      from p0
      group by 1 order by 1
    )
    , webhook_analytics as (
      select
      date(timestamp) as date
      ,count(case when (event = 'customer_product_free_trial_created') then 1 else null end) as free_trial_created
      , count(case when (event = 'customer_product_free_trial_converted') then 1 else null end) as free_trial_converted
      , count(case when (event = 'customer_product_created') then 1 else null end) as paying_created
      , count(case when (event = 'customer_product_cancelled') then 1 else null end) as paying_churn
      from webhook_events
      group by 1 order by 1
    )
    , outer_query as (
      select
      webhook_analytics.date as date
      , free_trial_created
      , free_trial_converted
      , paying_created
      , paying_churn
      , total_paying
      , total_free_trials
      from webhook_analytics
      left join totals
      on webhook_analytics.date = totals.uploaded_at
    )
    select * from outer_query order by date
    ;;
    datagroup_trigger: upff_daily_refresh_datagroup
  }

  dimension_group: date {
    type: time
    sql: ${TABLE}.date ;;
  }

  measure: free_trial_created {
    type: sum
    sql: ${TABLE}.free_trial_created ;;
  }

  measure: free_trial_converted {
    type: sum
    sql: ${TABLE}.free_trial_converted ;;
  }

  measure: paying_created {
    type: sum
    sql: ${TABLE}.paying_created ;;
  }

  measure: paying_churn {
    type: sum
    sql: ${TABLE}.paying_churn ;;
  }

  measure: total_paying {
    type: sum
    sql: ${TABLE}.total_paying ;;
  }

  measure: total_free_trials {
    type: sum
    sql: ${TABLE}.total_free_trials ;;
  }

  measure: total_subscribers {
    type: number
    sql: ${total_free_trials} + ${total_paying} ;;
  }

  measure: count {
    type: count
    drill_fields: [free_trial_created, free_trial_converted, paying_created, paying_churn, total_paying, total_free_trials]
    }

}
