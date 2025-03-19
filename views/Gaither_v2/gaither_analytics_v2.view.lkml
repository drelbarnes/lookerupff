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
      ,customer_id as user_id
      ,subscription_status as status
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
    where rn=1.  -- select the report with most recent date
),


    ------ Vimeo OTT ------
    vimeo_raw as (
    select
      CAST(user_id AS VARCHAR(255))
      ,status
      ,platform
      ,date(customer_created_at) as created_at
      ,date(report_date) as report_date
    from vimeo_subscriptions
    where action = 'subscription' and platform not in('api','web')
)


  select
    user_id
    ,status
    ,platform
    ,date(created_at) as created_at
    ,date(report_date) as report_date
  from chargebee_subs
  union all
  select * from vimeo_raw
    ;;
  }

  dimension: date {
    type: date
    sql:  ${TABLE}.report_date ;;
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
}
