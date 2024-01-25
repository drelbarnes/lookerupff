view: chargebee_analytics {
  derived_table: {
    sql: with webhook_events as (
        select * from ${chargebee_webhook_events.SQL_TABLE_NAME}
      )
      , vimeo_webhook_events as (
        with events as (
          select *
          , row_number() over (partition by user_id, created_at order by created_at desc, timestamp desc) as rn
          from ${gtv_vimeo_webhook_events.SQL_TABLE_NAME}
        )
        , events_1 as (
          select * from events
          -- where rn=1
        )
        select * from events_1
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
    , totals as (
      select
      uploaded_at
      , count(case when (status = 'active' or status = 'non_renewing') then 1 else null end) as total_paying
      , count(case when (status = 'in_trial') then 1 else null end) as total_free_trials
      from p0
      group by 1 order by 1
    )
    , chargebee_webhook_analytics as (
      select
      date(timestamp) as date
      , "web" as platform
      ,count(case when (event = 'customer_product_free_trial_created') then 1 else null end) as free_trial_created
      , count(case when (event = 'customer_product_free_trial_converted') then 1 else null end) as free_trial_converted
      , count(case when (event = 'customer_product_created') then 1 else null end) as paying_created
      , count(case when (event = 'customer_product_cancelled') then 1 else null end) as paying_churn
      from webhook_events
      group by 1,2 order by 1
    )
    , vimeo_webhook_analytics as (
      select
      date(timestamp) as date
      , platform
      ,count(case when (event = 'customer_product_free_trial_created') then 1 else null end) as free_trial_created
      , count(case when (event = 'customer_product_free_trial_converted') then 1 else null end) as free_trial_converted
      , count(case when (event = 'customer_product_created') then 1 else null end) as paying_created
      , count(case when (event = 'customer_product_cancelled') then 1 else null end) as paying_churn
      from vimeo_webhook_events
      where platform != "web"
      group by 1,2 order by 1
    )
    , unionized_analytics as (
      select * from chargebee_webhook_analytics
      union all
      select * from vimeo_webhook_analytics
    )
    , outer_query as (
      select
      timestamp(a.date) as date
      , platform
      , free_trial_created
      , free_trial_converted
      , paying_created
      , paying_churn
      , total_paying
      , total_free_trials
      from unionized_analytics as a
      left join totals as b
      on a.date = b.uploaded_at
    )
    select * from outer_query order by date
    ;;
    datagroup_trigger: chargebee_reporting
  }

  dimension_group: date {
    type: time
    timeframes: [
      raw,
      time,
      date,
      day_of_week,
      week,
      month,
      quarter,
      year
    ]
    sql: ${TABLE}.date ;;
  }

  dimension: platform {
    type: string
    sql: ${TABLE}.platform ;;
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
