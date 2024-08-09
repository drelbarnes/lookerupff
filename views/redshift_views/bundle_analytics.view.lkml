view: bundle_analytics {
  derived_table: {
    sql: with gaithertvplus_events as (
        select * from looker_scratch.lr$rmjjh1723212955173_chargebee_webhook_events
        WHERE plan LIKE '%GaitherTV%'
      )
      , upfaithandfamily_events as (
        select * from looker_scratch.lr$rmjjh1723212955173_chargebee_webhook_events
        WHERE plan LIKE '%UP-Faith-Family%'
      )
      , minno_events as (
        select * from looker_scratch.lr$rmjjh1723212955173_chargebee_webhook_events
        WHERE plan LIKE '%Minno%'
      )
      , gaithertvplus_webhook_analytics as (
        select
        date(timestamp) as date
        , 'web' as platform
        ,count(case when (event = 'customer_product_free_trial_created') then 1 else null end) as free_trial_created
        , count(case when (event = 'customer_product_free_trial_converted') then 1 else null end) as free_trial_converted
        , count(case when (event = 'customer_product_free_trial_expired') then 1 else null end) as free_trial_churn
        , count(case when (event = 'customer_product_created') then 1 else null end) as paying_created
        , count(case when (event = 'customer_product_cancelled') then 1 else null end) as paying_churn
        , count(case when (event = 'customer_product_paused') then 1 else null end) as paused_created
        from gaithertvplus_events
        group by 1,2 order by 1
      )
      , upfaithandfamily_webhook_analytics as (
        select
        date(timestamp) as date
        , 'web' as platform
        ,count(case when (event = 'customer_product_free_trial_created') then 1 else null end) as free_trial_created
        , count(case when (event = 'customer_product_free_trial_converted') then 1 else null end) as free_trial_converted
        , count(case when (event = 'customer_product_free_trial_expired') then 1 else null end) as free_trial_churn
        , count(case when (event = 'customer_product_created') then 1 else null end) as paying_created
        , count(case when (event = 'customer_product_cancelled') then 1 else null end) as paying_churn
        , count(case when (event = 'customer_product_paused') then 1 else null end) as paused_created
        from upfaithandfamily_events
        group by 1,2 order by 1
      )
      , minno_webhook_analytics as (
        select
        date(timestamp) as date
        , 'web' as platform
        ,count(case when (event = 'customer_product_free_trial_created') then 1 else null end) as free_trial_created
        , count(case when (event = 'customer_product_free_trial_converted') then 1 else null end) as free_trial_converted
        , count(case when (event = 'customer_product_free_trial_expired') then 1 else null end) as free_trial_churn
        , count(case when (event = 'customer_product_created') then 1 else null end) as paying_created
        , count(case when (event = 'customer_product_cancelled') then 1 else null end) as paying_churn
        , count(case when (event = 'customer_product_paused') then 1 else null end) as paused_created
        from minno_events
        group by 1,2 order by 1
      )
      , gaithertvplus_subs as (
        with p0 as (
          SELECT
          uploaded_at
          , subscription_id
          , customer_id
          , subscription_status as status
          , subscription_subscription_items_0_object
          , subscription_subscription_items_0_item_type
          , subscription_subscription_items_0_unit_price
          , subscription_subscription_items_0_item_price_id
          , subscription_created_at as created_at
          , subscription_started_at as started_at
          , row_number() over (partition by subscription_id, uploaded_at order by uploaded_at desc) as rn
          FROM http_api.chargebee_subscriptions
          WHERE subscription_subscription_items_0_item_price_id LIKE '%GaitherTV%'
        )
        select
        *
        from p0
        where rn=1
      )
      , upfaithandfamily_subs as (
        with p0 as (
          SELECT
          uploaded_at
          , subscription_id
          , customer_id
          , subscription_status as status
          , subscription_subscription_items_0_object
          , subscription_subscription_items_0_item_type
          , subscription_subscription_items_0_unit_price
          , subscription_subscription_items_0_item_price_id
          , subscription_created_at as created_at
          , subscription_started_at as started_at
          , row_number() over (partition by subscription_id, uploaded_at order by uploaded_at desc) as rn
          FROM http_api.chargebee_subscriptions
          WHERE subscription_subscription_items_0_item_price_id LIKE '%UP-Faith-Family%'
        )
        select
        *
        from p0
        where rn=1
      )
      , minno_subs as (
        with p0 as (
          SELECT
          uploaded_at
          , subscription_id
          , customer_id
          , subscription_status as status
          , subscription_subscription_items_0_object
          , subscription_subscription_items_0_item_type
          , subscription_subscription_items_0_unit_price
          , subscription_subscription_items_0_item_price_id
          , subscription_created_at as created_at
          , subscription_started_at as started_at
          , row_number() over (partition by subscription_id, uploaded_at order by uploaded_at desc) as rn
          FROM http_api.chargebee_subscriptions
          WHERE subscription_subscription_items_0_item_price_id LIKE '%Minno%'
        )
        select
        *
        from p0
        where rn=1
      )
      , gaithertvplus_totals as (
        select
        uploaded_at
        , 'gaithertvplus' as brand
        , count(case when (status = 'active' or status = 'non_renewing') then 1 else null end) as total_paying
        , count(case when (status = 'in_trial') then 1 else null end) as total_free_trials
        from gaithertvplus_subs
        group by 1,2 order by 1
      )
      , upfaithandfamily_totals as (
        select
        uploaded_at
        , 'upfaithandfamily' as brand
        , count(case when (status = 'active' or status = 'non_renewing') then 1 else null end) as total_paying
        , count(case when (status = 'in_trial') then 1 else null end) as total_free_trials
        from upfaithandfamily_subs
        group by 1,2 order by 1
      )
      , minno_totals as (
        select
        uploaded_at
        , 'minno' as brand
        , count(case when (status = 'active' or status = 'non_renewing') then 1 else null end) as total_paying
        , count(case when (status = 'in_trial') then 1 else null end) as total_free_trials
        from minno_subs
        group by 1,2 order by 1
      )
      , gaithertvplus_analytics as (
        select a.*
        , free_trial_created
        , free_trial_converted
        , free_trial_churn
        , paying_created
        , paying_churn
        , paused_created
        from gaithertvplus_totals a
        left join gaithertvplus_webhook_analytics b
        on a.uploaded_at = b.date
      )
      , upfaithandfamily_analytics as (
        select a.*
        , free_trial_created
        , free_trial_converted
        , free_trial_churn
        , paying_created
        , paying_churn
        , paused_created
        from upfaithandfamily_totals a
        left join upfaithandfamily_webhook_analytics b
        on a.uploaded_at = b.date
      )
      , minno_analytics as (
        select a.*
        , free_trial_created
        , free_trial_converted
        , free_trial_churn
        , paying_created
        , paying_churn
        , paused_created
        from minno_totals a
        left join minno_webhook_analytics b
        on a.uploaded_at = b.date
      )
      , gaithertvplus_bundled_subs as (
        select a.uploaded_at
        , a.customer_id
        , a.subscription_id as upfaithandfamily_subscription_id
        , a.status as upfaithandfamily_subscription_status
        , b.subscription_id as gaithertvplus_subscription_id
        , b.status as gaithertvplus_subscription_status
        from upfaithandfamily_subs a
        inner join gaithertvplus_subs b
        on a.uploaded_at = b.uploaded_at
        and a.customer_id = b.customer_id
        and a.started_at = b.started_at
        and a.created_at > b.created_at
        and a.status in ('active', 'non_renewing', 'in_trial')
        and b.status in ('active', 'non_renewing', 'in_trial')
      )
      , upfaithandfamily_gaithertvplus_bundled_subs as (
        select a.uploaded_at
        , a.customer_id
        , a.subscription_id as upfaithandfamily_subscription_id
        , a.status as upfaithandfamily_subscription_status
        , b.subscription_id as gaithertvplus_subscription_id
        , b.status as gaithertvplus_subscription_status
        , c.subscription_id as minno_subscription_id
        , c.status as minno_subscription_status
        , c.started_at as minno_subscription_started_at
        , c.created_at as minno_subscription_created_at
        from upfaithandfamily_subs a
        left join gaithertvplus_subs b
        on a.uploaded_at = b.uploaded_at
        and a.customer_id = b.customer_id
        and a.started_at = b.started_at
        and a.created_at < b.created_at
        and a.status in ('active', 'non_renewing', 'in_trial')
        and b.status in ('active', 'non_renewing', 'in_trial')
        left join minno_subs c
        on a.uploaded_at = c.uploaded_at
        and a.customer_id = c.customer_id
        and a.started_at = c.started_at
        and a.created_at < c.created_at
        and a.status in ('active', 'non_renewing', 'in_trial')
        and c.status in ('active', 'non_renewing', 'in_trial')
      )
      , upfaithandfamily_bundle_analytics as (
        select uploaded_at
        , 'upfaithandfamily' as brand
        , count(case when upfaithandfamily_subscription_status = 'in_trial' and (gaithertvplus_subscription_status = 'in_trial' or minno_subscription_status = 'in_trial') then customer_id end) as total_bundled_upfaithandfamily_trials
        , count(case when upfaithandfamily_subscription_status in ('active', 'non_rewewing') and (gaithertvplus_subscription_status in ('active', 'non_rewewing') or minno_subscription_status in ('active', 'non_rewewing')) then customer_id end) as total_bundled_upfaithandfamily_subscribers
        , count(case when upfaithandfamily_subscription_status = 'in_trial' and gaithertvplus_subscription_status = 'in_trial' then customer_id end) as total_bundled_gaithertvplus_trials
        , count(case when upfaithandfamily_subscription_status in ('active', 'non_rewewing') and gaithertvplus_subscription_status in ('active', 'non_rewewing') then customer_id end) as total_bundled_gaithertvplus_subscribers
        , count(case when upfaithandfamily_subscription_status = 'in_trial' and minno_subscription_status = 'in_trial' then customer_id end) as total_bundled_minno_trials
        , count(case when upfaithandfamily_subscription_status in ('active', 'non_rewewing') and minno_subscription_status in ('active', 'non_rewewing') then customer_id end) as total_bundled_minno_subscribers
        , count(case when upfaithandfamily_subscription_status = 'in_trial' and minno_subscription_status = 'in_trial' and gaithertvplus_subscription_status = 'in_trial' then customer_id end) as total_upentertainment_bundle_trials
        , count(case when upfaithandfamily_subscription_status in ('active', 'non_rewewing') and minno_subscription_status in ('active', 'non_rewewing') then customer_id end) as total_upentertainment_bundle_subscribers
        from upfaithandfamily_gaithertvplus_bundled_subs group by 1 order by uploaded_at limit 5000
      )
      , gaithertvplus_bundle_analytics as (
        select uploaded_at
        , 'gaithertvplus' as brand
        , count(case when gaithertvplus_subscription_status = 'in_trial' and (gaithertvplus_subscription_status = 'in_trial') then customer_id end) as total_bundled_gaithertvplus_trials
        , count(case when gaithertvplus_subscription_status in ('active', 'non_rewewing') and (gaithertvplus_subscription_status in ('active', 'non_rewewing')) then customer_id end) as total_bundled_gaithertvplus_subscribers
        , count(case when gaithertvplus_subscription_status = 'in_trial' and upfaithandfamily_subscription_status = 'in_trial' then customer_id end) as total_bundled_upfaithandfamily_trials
        , count(case when gaithertvplus_subscription_status in ('active', 'non_rewewing') and upfaithandfamily_subscription_status in ('active', 'non_rewewing') then customer_id end) as total_bundled_upfaithandfamily_subscribers
        , null::INT as total_bundled_minno_trials
        , null::INT as total_bundled_minno_subscribers
        , null::INT as total_upentertainment_bundle_trials
        , null::INT as total_upentertainment_bundle_subscribers
        from gaithertvplus_bundled_subs group by 1 order by uploaded_at limit 5000
      )
      , all_brand_bundle_analytics as (
      select
        uploaded_at,
        brand,
        sum(total_bundled_upfaithandfamily_trials) as total_bundled_upfaithandfamily_trials,
        sum(total_bundled_upfaithandfamily_subscribers) as total_bundled_upfaithandfamily_subscribers,
        sum(total_bundled_gaithertvplus_trials) as total_bundled_gaithertvplus_trials,
        sum(total_bundled_gaithertvplus_subscribers) as total_bundled_gaithertvplus_subscribers,
        sum(total_bundled_minno_trials) as total_bundled_minno_trials,
        sum(total_bundled_minno_subscribers) as total_bundled_minno_subscribers,
        sum(total_upentertainment_bundle_trials) as total_upentertainment_bundle_trials,
        sum(total_upentertainment_bundle_subscribers) as total_upentertainment_bundle_subscribers
      from (
        select * from upfaithandfamily_bundle_analytics
        union all
        select * from gaithertvplus_bundle_analytics
      )
      group by 1,2 order by uploaded_at, brand
    )
    select * from all_brand_bundle_analytics order by uploaded_at, brand
    ;;
    datagroup_trigger: upff_event_processing
    distribution_style: all
  }

  dimension: brand {
    type: string
    sql: ${TABLE}.brand ;;
  }

  dimension: date {
    type: date
    sql: ${TABLE}.uploaded_at ;;
  }

  dimension: uploaded_at {
    type: date
    sql: ${TABLE}.uploaded_at ;;
  }


  measure: total_bundled_minno_subscribers {
    type: sum
    sql: ${TABLE}.total_bundled_minno_subscribers ;;
  }

  measure: total_bundled_minno_trials {
    type: sum
    sql: ${TABLE}.total_bundled_minno_trials ;;
  }

  measure: total_bundled_gaithertvplus_subscribers {
    type: sum
    sql: ${TABLE}.total_bundled_gaithertvplus_subscribers ;;
  }

  measure: total_bundled_gaithertvplus_trials {
    type: sum
    sql: ${TABLE}.total_bundled_gaithertvplus_trials ;;
  }

  measure: total_bundled_upfaithandfamily_subscribers {
    type: sum
    sql: ${TABLE}.total_bundled_upfaithandfamily_subscribers ;;
  }

  measure: total_bundled_upfaithandfamily_trials {
    type: sum
    sql: ${TABLE}.total_bundled_upfaithandfamily_trials ;;
  }

  measure: total_upentertainment_bundle_subscribers {
    type: sum
    sql: ${TABLE}.total_upentertainment_bundle_subscribers ;;
  }

  measure: total_upentertainment_bundle_trials {
    type: sum
    sql: ${TABLE}.total_upentertainment_bundle_trials ;;
  }
}
