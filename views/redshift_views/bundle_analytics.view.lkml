view: bundle_analytics {
  derived_table: {
    sql: with gaithertvplus_events as (
        select * from ${chargebee_webhook_events.SQL_TABLE_NAME}
        WHERE plan LIKE '%GaitherTV%'
      )
      , upfaithandfamily_events as (
        select * from ${chargebee_webhook_events.SQL_TABLE_NAME}
        WHERE plan LIKE '%UP-Faith-Family%'
      )
      , minno_events as (
        select * from ${chargebee_webhook_events.SQL_TABLE_NAME}
        WHERE plan LIKE '%Minno%'
      )
      , gaithertvplus_webhook_analytics as (
        select
        date(timestamp) as date
        , 'web' as platform
        , 'gaithertvplus'::VARCHAR as brand
        , count(case when (event = 'customer_product_free_trial_created') then 1 else null end) as free_trial_created
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
        , 'upfaithandfamily'::VARCHAR as brand
        , 'upfaithandfamily_only'::VARCHAR as bundle_type
        , count(case when (event = 'customer_product_free_trial_created') then 1 else null end) as free_trial_created
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
        , 'minno'::VARCHAR as brand
        , count(case when (event = 'customer_product_free_trial_created') then 1 else null end) as free_trial_created
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
        , 'gaithertvplus'::VARCHAR as brand
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
        , 'upfaithandfamily'::VARCHAR as brand
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
        , 'minno'::VARCHAR as brand
        from p0
        where rn=1
      )
      , gaithertvplus_totals as (
        select
        uploaded_at
        , brand
        , count(case when (status = 'active' or status = 'non_renewing') then 1 else null end) as total_paying
        , count(case when (status = 'in_trial') then 1 else null end) as total_free_trials
        from gaithertvplus_subs
        group by 1,2 order by 1
      )
      , upfaithandfamily_totals as (
        select
        uploaded_at
        , brand
        , count(case when (status = 'active' or status = 'non_renewing') then 1 else null end) as total_paying
        , count(case when (status = 'in_trial') then 1 else null end) as total_free_trials
        from upfaithandfamily_subs
        group by 1,2 order by 1
      )
      , minno_totals as (
        select
        uploaded_at
        , brand
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
        , a.brand
        , a.customer_id
        , a.subscription_id as gaithertvplus_subscription_id
        , a.status as gaithertvplus_subscription_status
        , b.subscription_id as upfaithandfamily_subscription_id
        , b.status as upfaithandfamily_subscription_status
        from gaithertvplus_subs a
        inner join upfaithandfamily_subs b
        on a.uploaded_at = b.uploaded_at
        and a.customer_id = b.customer_id
        and a.started_at = b.started_at
        and a.created_at < b.created_at
      )
      , gaithertvplus_bundle_counts as (
        select uploaded_at
        , brand
        , CASE
          WHEN gaithertvplus_subscription_status IN ('in_trial', 'active', 'non_renewing') THEN
            CASE
              WHEN upfaithandfamily_subscription_status IN ('in_trial', 'active', 'non_renewing') THEN 'gaithertvplus_upfaithandfamily'
              ELSE 'gaithertvplus_only'
            END
          ELSE 'not_bundled'
        END AS bundle_type
        , count(case when gaithertvplus_subscription_status = 'in_trial' and (gaithertvplus_subscription_status = 'in_trial') then customer_id end) as total_bundled_gaithertvplus_trials
        , count(case when gaithertvplus_subscription_status in ('active', 'non_rewewing') and (gaithertvplus_subscription_status in ('active', 'non_rewewing')) then customer_id end) as total_bundled_gaithertvplus_subscribers
        , count(case when gaithertvplus_subscription_status = 'in_trial' and upfaithandfamily_subscription_status = 'in_trial' then customer_id end) as total_bundled_upfaithandfamily_trials
        , count(case when gaithertvplus_subscription_status in ('active', 'non_rewewing') and upfaithandfamily_subscription_status in ('active', 'non_rewewing') then customer_id end) as total_bundled_upfaithandfamily_subscribers
        , null::INT as total_bundled_minno_trials
        , null::INT as total_bundled_minno_subscribers
        , null::INT as total_upentertainment_bundle_trials
        , null::INT as total_upentertainment_bundle_subscribers
        from gaithertvplus_bundled_subs
        group by 1,2,3
        order by uploaded_at
      )
      , upfaithandfamily_bundled_subs as (
        select a.uploaded_at
        , a.brand
        , a.customer_id
        , a.subscription_id as upfaithandfamily_subscription_id
        , CASE
          WHEN a.status IN ('in_trial', 'active', 'non_renewing') THEN
            CASE
              WHEN b.status IN ('in_trial', 'active', 'non_renewing') AND c.status IN ('in_trial', 'active', 'non_renewing') THEN 'upfaithandfamily_gaithertvplus_minno'
              WHEN b.status IN ('in_trial', 'active', 'non_renewing') THEN 'upfaithandfamily_gaithertvplus'
              WHEN c.status IN ('in_trial', 'active', 'non_renewing') THEN 'upfaithandfamily_minno'
              ELSE 'upfaithandfamily_only'
            END
          ELSE 'not_bundled'
        END AS bundle_type
        , CASE
            WHEN a.status = 'in_trial' THEN
              CASE
                WHEN b.status = 'in_trial' and c.status = 'in_trial' then 'trial'
                when b.status = 'in_trial' then 'trial'
                when c.status = 'in_trial' then 'trial'
                ELSE 'not_active'
              END
            WHEN a.status IN ('active', 'non_renewing') THEN
              CASE
                WHEN b.status in ('active', 'non_renewing') and c.status in ('active', 'non_renewing') then 'active'
                when b.status in ('active', 'non_renewing') then 'active'
                when c.status in ('active', 'non_renewing') then 'active'
                ELSE 'not_active'
              END
            -- WHEN a.status not in ('trial', 'active', 'non_renewing') then
            --   case
            --     when b.status in ('active', 'non_renewing') and c.status in ('active', 'non_renewing') then 'active'
            --   end
            ELSE 'not_active'
          END AS bundle_status
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
        left join minno_subs c
        on a.uploaded_at = c.uploaded_at
        and a.customer_id = c.customer_id
        and a.started_at = c.started_at
        and a.created_at < c.created_at
      )
      , upfaithandfamily_bundle_counts as (
        select uploaded_at
        , brand
        , bundle_type
        , count(case when bundle_type not in ('upfaithandfamily_only', 'not_bundled') and bundle_status = 'trial' then customer_id end) as total_bundled_upfaithandfamily_trials
        , count(case when bundle_type not in ('upfaithandfamily_only', 'not_bundled') and bundle_status = 'active' then customer_id end) as total_bundled_upfaithandfamily_subscribers
        , count(case when bundle_type = 'upfaithandfamily_gaithertvplus' and bundle_status = 'trial' then customer_id end) as total_bundled_gaithertvplus_trials
        , count(case when bundle_type = 'upfaithandfamily_gaithertvplus' and bundle_status = 'active' then customer_id end) as total_bundled_gaithertvplus_subscribers
        , count(case when bundle_type = 'upfaithandfamily_minno' and bundle_status = 'trial' then customer_id end) as total_bundled_minno_trials
        , count(case when bundle_type = 'upfaithandfamily_minno' and bundle_status = 'active' then customer_id end) as total_bundled_minno_subscribers
        , count(case when bundle_type = 'upfaithandfamily_gaithertvplus_minno' and bundle_status = 'trial' then customer_id end) as total_upentertainment_bundle_trials
        , count(case when bundle_type = 'upfaithandfamily_gaithertvplus_minno' and bundle_status = 'active' then customer_id end) as total_upentertainment_bundle_subscribers
        from upfaithandfamily_bundled_subs
        group by 1,2,3
        order by uploaded_at
      )
      , all_brand_bundle_counts as (
      select
        uploaded_at,
        brand,
        bundle_type,
        sum(total_bundled_upfaithandfamily_trials) as total_bundled_upfaithandfamily_trials,
        sum(total_bundled_upfaithandfamily_subscribers) as total_bundled_upfaithandfamily_subscribers,
        sum(total_bundled_gaithertvplus_trials) as total_bundled_gaithertvplus_trials,
        sum(total_bundled_gaithertvplus_subscribers) as total_bundled_gaithertvplus_subscribers,
        sum(total_bundled_minno_trials) as total_bundled_minno_trials,
        sum(total_bundled_minno_subscribers) as total_bundled_minno_subscribers,
        sum(total_upentertainment_bundle_trials) as total_upentertainment_bundle_trials,
        sum(total_upentertainment_bundle_subscribers) as total_upentertainment_bundle_subscribers
      from (
        select * from upfaithandfamily_bundle_counts
        union all
        select * from gaithertvplus_bundle_counts
      )
      group by 1,2,3
      order by uploaded_at
      , bundle_type
      )
      , state_changes AS (
        SELECT
          current.uploaded_at,
          current.customer_id,
          current.bundle_type AS current_bundle_type,
          current.bundle_status AS current_status,
          prev.bundle_type AS previous_bundle_type,
          prev.bundle_status AS previous_status
        FROM upfaithandfamily_bundled_subs current
        LEFT JOIN upfaithandfamily_bundled_subs prev
          ON current.customer_id = prev.customer_id
          AND current.uploaded_at = prev.uploaded_at + INTERVAL '1 day'
      ),
      bundle_events AS (
        SELECT
          uploaded_at,
          customer_id,
          current_bundle_type,
          previous_bundle_type,
          current_status,
          previous_status,
          CASE
            WHEN (previous_bundle_type IS NULL OR previous_bundle_type = 'not_bundled' OR previous_bundle_type = 'upfaithandfamily_only')
                 AND current_bundle_type NOT IN ('not_bundled', 'upfaithandfamily_only')
                 AND current_status = 'trial' THEN 'bundle_trial_created'
            WHEN previous_bundle_type NOT IN ('not_bundled', 'upfaithandfamily_only')
                 AND current_bundle_type NOT IN ('not_bundled', 'upfaithandfamily_only')
                 AND previous_status = 'trial' AND current_status = 'active' THEN 'bundle_trial_converted'
            WHEN previous_bundle_type NOT IN ('not_bundled', 'upfaithandfamily_only')
                 AND previous_status = 'trial'
                 AND (current_bundle_type IN ('not_bundled', 'upfaithandfamily_only') OR current_status = 'not_active') THEN 'bundle_trial_expired'
            WHEN (previous_bundle_type IS NULL OR previous_bundle_type IN ('not_bundled', 'upfaithandfamily_only') OR previous_status = 'not_active')
                 AND current_bundle_type NOT IN ('not_bundled', 'upfaithandfamily_only')
                 AND current_status = 'active' THEN 'bundle_paying_created'
            WHEN previous_bundle_type NOT IN ('not_bundled', 'upfaithandfamily_only')
                 AND previous_status = 'active'
                 AND (current_bundle_type IN ('not_bundled', 'upfaithandfamily_only') OR current_status = 'not_active') THEN 'bundle_paying_churn'
            WHEN previous_bundle_type != current_bundle_type
                 AND previous_bundle_type NOT IN ('not_bundled', 'upfaithandfamily_only')
                 AND current_bundle_type NOT IN ('not_bundled', 'upfaithandfamily_only')
                 AND current_status IN ('trial', 'active') THEN 'bundle_change'
            ELSE NULL
          END AS event
        FROM state_changes
      )
      , bundle_analytics as (
        with p0 as (
          SELECT
            uploaded_at,
            coalesce(previous_bundle_type,current_bundle_type) AS bundle_type,
            COUNT(CASE WHEN event = 'bundle_trial_created' THEN 1 END) AS bundle_trial_created,
            COUNT(CASE WHEN event = 'bundle_trial_converted' THEN 1 END) AS bundle_trial_converted,
            COUNT(CASE WHEN event = 'bundle_trial_expired' THEN 1 END) AS bundle_trial_expired,
            COUNT(CASE WHEN event = 'bundle_paying_created' THEN 1 END) AS bundle_paying_created,
            COUNT(CASE WHEN event = 'bundle_paying_churn' THEN 1 END) AS bundle_paying_churn,
            COUNT(CASE WHEN event = 'bundle_change' THEN 1 END) AS bundle_changes,
            0 AS bundle_paused_created,  -- Assuming no pause functionality for bundles
            0 AS bundle_resumed  -- Assuming no pause functionality for bundles
          FROM bundle_events
          GROUP BY 1, 2
          ORDER BY 1, 2
        )
        select * from p0 where bundle_type not in ('upfaithandfamily_only', 'not_bundled')
      )
      , bundle_analytics_plus as (
        select
        a1.uploaded_at
        , a1.bundle_type
        , a1.bundle_paying_churn+sum(coalesce(a2.bundle_paying_churn,0)) as churn_30_days
        from bundle_analytics as a1
        left join bundle_analytics as a2
        on datediff(day,a2.uploaded_at,a1.uploaded_at)<=29 and datediff(day,a2.uploaded_at,a1.uploaded_at)>0
        group by a1.uploaded_at,a1.bundle_type,a1.bundle_paying_churn
      )
      , bundle_metrics as (
        select
        a.*
        , b.bundle_trial_created
        , b.bundle_trial_converted
        , b.bundle_trial_expired
        , b.bundle_paying_created
        , b.bundle_paying_churn
        , b.bundle_paused_created
        , b.bundle_resumed
        , b.bundle_changes
        , lag(b.bundle_trial_created, 7) over (partition by a.bundle_type order by a.uploaded_at) as bundle_trial_created_7_days_prior
        , lag(a.total_bundled_upfaithandfamily_trials, 30) over (partition by a.bundle_type order by a.uploaded_at) as total_upfaithandfamily_bundles_30_days_prior
        , c.free_trial_created
        , c.free_trial_converted
        , c.free_trial_churn
        , c.paying_created
        , c.paying_churn
        , c.paused_created
        , d.churn_30_days
        from all_brand_bundle_counts a
        left join bundle_analytics b
        on a.uploaded_at = b.uploaded_at and a.bundle_type = b.bundle_type
        left join upfaithandfamily_webhook_analytics c
        on a.uploaded_at = c.date and a.bundle_type = c.bundle_type
        left join bundle_analytics_plus d
        on a.uploaded_at = d.uploaded_at and a.bundle_type = d.bundle_type
      )
      select * from bundle_metrics
      order by uploaded_at desc, bundle_type desc
    ;;
    datagroup_trigger: upff_acquisition_reporting
    distribution_style: all
  }
#testing
  dimension: brand {
    type: string
    sql: ${TABLE}.brand ;;
  }

  dimension: bundle_type {
    type: string
    sql: ${TABLE}.bundle_type ;;
  }

  dimension_group: timestamp {
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

  measure: bundle_trial_created {
    type: sum
    sql: ${TABLE}.bundle_trial_created ;;
  }

  measure: bundle_trial_converted {
    type: sum
    sql: ${TABLE}.bundle_trial_converted ;;
  }

  measure: bundle_trial_expired {
    type: sum
    sql: ${TABLE}.bundle_trial_expired ;;
  }

  measure: bundle_paying_created {
    type: sum
    sql: ${TABLE}.bundle_paying_created ;;
  }

  measure: bundle_paying_churn {
    type: sum
    sql: ${TABLE}.bundle_paying_churn ;;
  }

  measure: bundle_changes {
    type: sum
    sql: ${TABLE}.bundle_changes ;;
  }

  measure: bundle_paused_created {
    type: sum
    sql: ${TABLE}.bundle_paused_created ;;
  }

  measure: bundle_resumed {
    type: sum
    sql: ${TABLE}.bundle_resumed ;;
  }

  measure: bundle_trial_created_7_days_prior{
    type: sum
    sql: ${TABLE}.bundle_trial_created_7_days_prior ;;
  }

  measure: bundle_trial_conversion_rate {
    type: number
    value_format: ".0#\%"
    sql: 100*${bundle_trial_converted}*1.0/NULLIF(${bundle_trial_created_7_days_prior}, 0) ;;
  }

  measure: free_trial_created {
    type: sum
    sql: ${TABLE}.free_trial_created ;;
  }

  measure: free_trial_converted {
    type: sum
    sql: ${TABLE}.free_trial_converted ;;
  }

  measure: free_trial_churn {
    type: sum
    sql: ${TABLE}.free_trial_churn ;;
  }

  measure: paying_created {
    type: sum
    sql: ${TABLE}.paying_created ;;
  }

  measure: paying_churn {
    type: sum
    sql: ${TABLE}.paying_churn ;;
  }

  measure: paused_created {
    type: sum
    sql: ${TABLE}.paused_created ;;
  }

  measure: total_upfaithandfamily_bundles_30_days_prior{
    type: sum
    sql: ${TABLE}.total_upfaithandfamily_bundles_30_days_prior ;;
  }

  measure: churn_30_days {
    type: sum
    sql: ${TABLE}.churn_30_days ;;
  }

  measure: bundle_churn {
    type: number
    value_format: ".0#\%"
    sql: ${total_upfaithandfamily_bundles_30_days_prior}*1.0/NULLIF(${churn_30_days}, 0) ;;
  }

}
