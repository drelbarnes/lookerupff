view: analytics_v2 {
  derived_table: {
    sql: with upff_events as (
        select * from ${chargebee_webhook_events.SQL_TABLE_NAME}
        WHERE plan LIKE '%UP-Faith-Family%'
      )
      , chargebee_webhook_analytics as (
        select
         date(DATEADD(HOUR, -4, timestamp)) as date
        , 'web' as platform
        ,count(case when (event = 'customer_product_free_trial_created') then 1 else null end) as free_trial_created
        , count(case when (event = 'customer_product_free_trial_converted') then 1 else null end) as free_trial_converted
        , count(case when (event = 'customer_product_free_trial_expired') then 1 else null end) as free_trial_churn
        , count(case when (event = 'customer_product_created') then 1 else null end) as paying_created
        , count(case when (event = 'customer_product_cancelled') then 1 else null end) as paying_churn
        , count(case when (event = 'customer_product_paused') then 1 else null end) as paused_created
        from upff_events
        group by 1,2 order by 1
      )
      , subs as (
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
          , row_number() over (partition by subscription_id, uploaded_at order by uploaded_at desc) as rn
          FROM http_api.chargebee_subscriptions
          WHERE subscription_subscription_items_0_item_price_id LIKE '%UP-Faith-Family%'
          )
          select
          *
          from p0
          where rn=1
        )
      , chargebee_totals as (
        select
        uploaded_at
        , count(case when (status = 'active' or status = 'non_renewing') then 1 else null end) as total_paying
        , count(case when (status = 'in_trial') then 1 else null end) as total_free_trials
        from subs
        group by 1 order by 1
      )
      , get_analytics_p0 as (
          with p0 as (
            select
            *
            , case when existing_free_trials is null AND existing_paying is null then 'v2'
                else 'v1'
              end as report_version
            from php.get_analytics
            where date(sent_at)=current_date -1
          )
          select
          analytics_timestamp as "timestamp"
          , existing_free_trials
          , existing_paying
          , free_trial_churn
          , free_trial_converted
          , free_trial_created
          , paused_created
          -- , paying_churn
          -- MIGRATION ADJUSTMENTS
          , case
            when date(analytics_timestamp) = '2024-04-20' then paying_churn-36455
            when date(analytics_timestamp) = '2024-04-18' then paying_churn-44853
            when date(analytics_timestamp) = '2024-04-17' then paying_churn-55194
            when date(analytics_timestamp) = '2024-04-16' then paying_churn-15351
            else paying_churn
            end as paying_churn
          -- , paying_created
          , case
            when date(analytics_timestamp) = '2024-04-20' then paying_created-36455
            when date(analytics_timestamp) = '2024-04-18' then paying_created-44853
            when date(analytics_timestamp) = '2024-04-17' then paying_created-55194
            when date(analytics_timestamp) = '2024-04-16' then paying_created-15351
            else paying_created
            end as paying_created
          , total_free_trials
          , total_paying
          , report_version
          , row_number() over (partition by analytics_timestamp, report_version order by sent_at desc) as n
          from p0
        )
        , apple_subs as (
          select
          a.report_date
          , a.ios+a.tvos as vimeo_ott_subscribers_total
          , a.roku + a.amazon_fire_tv + a.android + a.android_tv + b.ios + b.tvos as vimeo_total
          , b.ios + b.tvos as app_store_connect_subscribers_total
          from ${customer_file_subscriber_counts.SQL_TABLE_NAME} as a
          left join ${appstoreconnect_sub_counts.SQL_TABLE_NAME} as b
          -- from looker_scratch.lr$rm4oz1712233105228_customer_file_subscriber_counts as a
          -- left join looker_scratch.lr$rmre91712233831486_appstoreconnect_sub_counts as b
          on a.report_date = b.report_date
        )
        , get_analytics_p1_test as (
          select
          a.timestamp as timestamp
          , existing_free_trials
          , existing_paying
          , (a.free_trial_created + b.free_trial_created) as free_trial_created
          , (a.free_trial_converted + b.free_trial_converted) as free_trial_converted
          , a.free_trial_churn + b.free_trial_churn as free_trial_churn
          -- , NULL::INTEGER as paused_created
          , greatest(0, a.paying_created - b.free_trial_created - a.free_trial_converted) as paying_created
          , greatest(0, a.paying_churn - b.free_trial_churn) as paying_churn
          -- Need to figure chargebee trials out
          , a.total_free_trials + c.total_free_trials as total_free_trials
          , greatest(0, a.total_paying - c.total_free_trials) as total_paying
          from (select * from get_analytics_p0 where report_version = 2 and n=1) as a
          left join chargebee_webhook_analytics as b
          on date(a.timestamp) = b."date"
          left join chargebee_totals as c
          on date(a.timestamp) = date(c.uploaded_at)
        )
        , get_analytics_p1 as (
          select
          coalesce(a.timestamp, a2.timestamp) as timestamp
          , coalesce(a.existing_free_trials, a2.existing_free_trials) as existing_free_trials
          , coalesce(a.existing_paying, a2.existing_paying) as existing_paying
          , coalesce(coalesce(a.free_trial_churn, a2.free_trial_churn), 0) + coalesce(b.free_trial_churn, 0) AS free_trial_churn
          , coalesce(coalesce(a.free_trial_converted, a2.free_trial_converted), 0) + coalesce(b.free_trial_converted, 0) as free_trial_converted
          , coalesce(coalesce(a.free_trial_created, a2.free_trial_created), 0) + coalesce(b.free_trial_created, 0) as free_trial_created
          , coalesce(coalesce(a.paused_created, a2.paused_created), 0) + coalesce(b.paused_created, 0) as paused_created
          , greatest(0, coalesce(coalesce(a.paying_churn, a2.paying_churn), 0) - coalesce(b.free_trial_churn, 0)) as paying_churn
          , greatest(0, coalesce(coalesce(a.paying_created, a2.paying_created), 0) - coalesce(b.free_trial_created, 0)) as paying_created
          , coalesce(coalesce(a2.total_free_trials, a.total_free_trials), 0) + coalesce(c1.total_free_trials, 0) as total_free_trials
          , c.vimeo_total + c1.total_paying as total_paying
          , (-c.vimeo_ott_subscribers_total+c.app_store_connect_subscribers_total) as test2
          from (select * from get_analytics_p0 where report_version = 'v1' and n=1) as a
          left join (select * from get_analytics_p0 where report_version = 'v2' and n=1) as a2
          on date(a.timestamp) = date(a2.timestamp)
          left join chargebee_webhook_analytics as b
          on date(a.timestamp) = b."date"
          left join (
            select
            report_date
            , app_store_connect_subscribers_total
            , vimeo_ott_subscribers_total
            ,vimeo_total
            from apple_subs
          ) c
          on date(a.timestamp) = date(c.report_date)
          left join chargebee_totals as c1
          on date(a.timestamp) = date(c1.uploaded_at)

        )
        , get_analytics_p1_old as (
          select
          coalesce(a.timestamp, b.timestamp) as timestamp
          , coalesce(a.existing_free_trials, b.existing_free_trials) as existing_free_trials
          , coalesce(a.existing_paying, b.existing_paying) as existing_paying
          , coalesce(a.free_trial_churn, b.free_trial_churn) as free_trial_churn
          , coalesce(a.free_trial_converted, b.free_trial_converted) as free_trial_converted
          , coalesce(a.free_trial_created, b.free_trial_created) as free_trial_created
          , coalesce(a.paused_created, b.paused_created) as paused_created
          , coalesce(a.paying_churn, b.paying_churn) as paying_churn
          , coalesce(a.paying_created, b.paying_created) as paying_created
          , coalesce(b.total_free_trials, a.total_free_trials) as total_free_trials
          , coalesce(nullif(b.total_paying-c.vimeo_ott_subscribers_total+c.app_store_connect_subscribers_total,-c.vimeo_ott_subscribers_total+c.app_store_connect_subscribers_total)
            ,nullif(a.total_paying-c.vimeo_ott_subscribers_total+c.app_store_connect_subscribers_total,-c.vimeo_ott_subscribers_total+c.app_store_connect_subscribers_total), b.total_paying, a.total_paying) as total_paying
          , (null-c.vimeo_ott_subscribers_total+c.app_store_connect_subscribers_total) as test
          , (-c.vimeo_ott_subscribers_total+c.app_store_connect_subscribers_total) as text2
          from (select * from get_analytics_p0 where report_version = 'v1' and n=1) as a
          left join (select * from get_analytics_p0 where report_version = 'v2' and n=1) as b
          on date(a.timestamp) = date(b.timestamp)
          left join (
            select
            report_date
            , app_store_connect_subscribers_total
            , vimeo_ott_subscribers_total
            ,vimeo_total
            from apple_subs
          ) c
          on date(a.timestamp) = date(c.report_date)
        )
        -- BACK UP SOURCE INCASE OF API OUTAGE
        , distinct_events as (
          select distinct user_id
          , action
          , status
          , frequency
          , to_timestamp(event_created_at, 'YYYY-MM-DD HH24:MI:SS') as event_created_at
          , to_date(event_created_at, 'YYYY-MM-DD') as event_date
          , to_date(report_date, 'YYYY-MM-DD') as report_date
          from customers.all_customers
          where action = 'subscription'
        )
        , total_counts as (
          select report_date
          , count(distinct case when status = 'enabled' then user_id end) as total_paying
          , lag(total_paying, 1) over (order by report_date) as existing_paying
          , count(distinct case when status = 'free_trial' then user_id end) as total_free_trials
          , lag(total_free_trials, 1) over (order by report_date) as existing_free_trials
          from distinct_events
          group by 1
        )
        , customers_analytics as (
          with p0 as (
            select get_analytics_p1.timestamp,
            coalesce(get_analytics_p1.existing_free_trials, total_counts.existing_free_trials) as existing_free_trials,
            coalesce(get_analytics_p1.existing_paying, total_counts.existing_paying) as existing_paying,
            get_analytics_p1.free_trial_churn,
            get_analytics_p1.free_trial_converted,
            get_analytics_p1.free_trial_created,
            get_analytics_p1.paused_created,
            get_analytics_p1.paying_churn,
            get_analytics_p1.paying_created,
            coalesce(get_analytics_p1.total_free_trials, total_counts.total_free_trials) as total_free_trials,
            coalesce(get_analytics_p1.total_paying, total_counts.total_paying) as total_paying
            from get_analytics_p1
            full join total_counts
            on total_counts.report_date = trunc(get_analytics_p1.timestamp)
          )
          select * from p0 where date(timestamp) < current_date
        ),

      a as (select a.timestamp, ROW_NUMBER() OVER(ORDER BY a.timestamp desc) AS Row
      from customers_analytics as a),

      b as (select a.timestamp,total_paying,ROW_NUMBER() OVER(ORDER BY a.timestamp desc) AS Row
      from customers_analytics as a where a.timestamp < (DATEADD(day,-30, DATE_TRUNC('day',GETDATE()) ))),

      c as (select a.timestamp,total_paying as paying_30_days_prior from a inner join b on a.row=b.row),

      d as ((select a1.timestamp, a1.paying_churn+sum(coalesce(a2.paying_churn,0)) as churn_30_days, a1.paying_churn+sum(coalesce(a2.paying_created,0)) as winback_30_days
      from customers_analytics as a1
      left join customers_analytics as a2 on datediff(day,a2.timestamp,a1.timestamp)<=29 and datediff(day,a2.timestamp,a1.timestamp)>0
      group by a1.timestamp,a1.paying_churn)),

      e as (select c.timestamp, cast(paying_30_days_prior as decimal) as paying_30_days_prior,
      cast(churn_30_days as decimal) as churn_30_days,
      cast(paying_30_days_prior as decimal) / NULLIF(cast(churn_30_days as decimal), 0) as churn_30_day_percent,
      cast(winback_30_days as decimal) as winback_30_days
      from c inner join d on c.timestamp=d.timestamp),

      f as (select *, sum((49000-(total_paying))/nullif((365-day_of_year),0)) OVER (PARTITION by cast(datepart(month,date(timestamp)) as varchar) order by timestamp asc rows between unbounded preceding and current row) as Running_Free_Trial_Target
      from (select *, SUM(free_trial_created) OVER (PARTITION by cast(datepart(month,date(timestamp)) as varchar) order by timestamp asc rows between unbounded preceding and current row) AS Running_Free_Trials
      from (select distinct * from (select a.*,
      case when extract(YEAR from a.timestamp)='2018' then 795+((49000-795)*(cast(datepart(dayofyear,date(a.timestamp)) as integer)-1)/365)
      when extract(YEAR from a.timestamp)='2019' then 16680+((55000-16680)*(cast(datepart(dayofyear,date(a.timestamp)) as integer)-1)/365)
      when extract(YEAR from a.timestamp)='2020' then 64907+((125000-64907)*(cast(datepart(dayofyear,date(a.timestamp)) as integer)-1)/365)
      when extract(YEAR from a.timestamp)='2021' then 148678+((190000-148678)*(cast(datepart(dayofyear,date(a.timestamp)) as integer)-1)/365)
      when extract(YEAR from a.timestamp)='2022' then 229371+((339000-229371)*(cast(datepart(dayofyear,date(a.timestamp)) as integer)-1)/365) end as target,
      case when extract(YEAR from a.timestamp)='2018' then 3246+((49000-3246)*(cast(datepart(dayofyear,date(a.timestamp)) as integer)-1)/365)
      when extract(YEAR from a.timestamp)='2019' then 24268+((55000-24268)*(cast(datepart(dayofyear,date(a.timestamp)) as integer)-1)/365)
      when extract(YEAR from a.timestamp)='2020' then 70039+((125000-70039)*(cast(datepart(dayofyear,date(a.timestamp)) as integer)-1)/365)
      when extract(YEAR from a.timestamp)='2021' then 157586+((190000-157586)*(cast(datepart(dayofyear,date(a.timestamp)) as integer)-1)/365)
      when extract(YEAR from a.timestamp)='2022' then 243181+((339000-243181)*(cast(datepart(dayofyear,date(a.timestamp)) as integer)-1)/365) end as total_target, -- this baseline is paid subs + free trial
      229371+((339000-229371)*(cast(datepart(dayofyear,date(a.timestamp)) as integer)+14)/365) as target_14_days_future, -- this baseline matches total_target baseline
      cast(datepart(dayofyear,date(a.timestamp)) as integer)-1 as day_of_year,
      cast(datepart(dayofyear,date(a.timestamp)) as integer)+14 as day_of_year_14_days,
      case when extract(YEAR from a.timestamp)='2018' then 49000
      when extract(YEAR from a.timestamp)='2019' then 55000
      when extract(YEAR from a.timestamp)='2020' then 125000
      when extract(YEAR from a.timestamp)='2021' then 190000
      when extract(YEAR from a.timestamp)='2022' then 339000 end as annual_target,
      case when rownum=max(rownum) over(partition by Week) then existing_paying end as PriorWeekExistingSubs,
      case when rownum=max(rownum) over(partition by Month) then existing_paying end as PriorMonthExistingSubs,
      case when rownum=min(rownum) over(partition by Week||year) then total_paying end as CurrentWeekExistingSubs,
      case when rownum=min(rownum) over(partition by Month||year) then total_paying end as CurrentMonthExistingSubs,
      wait_content,
      save_money,
      vacation,
      high_price,
      other
      from
      (
        (
          select
          a.*
          ,cast(datepart(week,date(timestamp)) as varchar) as Week,
          cast(datepart(month,date(timestamp)) as varchar) as Month,
          cast(datepart(Quarter,date(timestamp)) as varchar) as Quarter,
          cast(datepart(Year,date(timestamp)) as varchar) as Year,
          case
            when timestamp < '2024-04-26' then b.new_trials_14_days_prior
            when timestamp >= '2024-04-26' and timestamp < '2024-05-04' then b.new_trials_14_days_prior + d.new_trials_7_days_prior
            when timestamp >= '2024-05-04' then b.new_trials_14_days_prior - e.new_trials_14_days_prior + d.new_trials_7_days_prior
            end as new_trials_14_days_prior
          /* Pre-trial length change period dimension
          b.new_trials_14_days_prior
          */
          /* Trial length changed on Web only period dimension
          case
            when timestamp < '2024-04-27' then b.new_trials_14_days_prior
            when timestamp >= '2024-04-27' and timestamp < '2024-05-04' then b.new_trials_14_days_prior + d.new_trials_7_days_prior
            when timestamp >= '2024-05-04' then b.new_trials_14_days_prior - e.new_trials_14_days_prior + d.new_trials_7_days_prior
            end as new_trials_14_days_prior
          */
          /* Trial length changed on all platforms period dimension
          case
            when timestamp < '2024-04-27' then b.new_trials_14_days_prior
            when timestamp >= '2024-04-27' and timestamp < '2024-05-04' then b.new_trials_14_days_prior + d.new_trials_7_days_prior
            when timestamp >= '2024-05-04' and timestamp < {PLATFORM_CHANGE_DATE} then b.new_trials_14_days_prior - e.new_trials_14_days_prior + d.new_trials_7_days_prior
            end as new_trials_14_days_prior
            when timestamp >= {PLATFORM_CHANGE_DATE} and timestamp < dateadd(day, 14, {PLATFORM_CHANGE_DATE}) then INSERT TRANSITION LOGIC HERE
            when timestamp >= dateadd(day, 14, {PLATFORM_CHANGE_DATE}) then c.new_trials_7_days_prior
          */
          from
          (
            select *, row_number() over(order by timestamp desc) as rownum from customers_analytics
          ) as a
          left join
          (
            select
              free_trial_created as new_trials_14_days_prior,
              row_number() over(order by timestamp desc) as rownum
            from customers_analytics
            where timestamp in (
              select
              dateadd(day, -14, timestamp) as timestamp
              from customers_analytics
            )
          ) as b
          on a.rownum=b.rownum
          left join
          (
            select
              free_trial_created as new_trials_7_days_prior,
              row_number() over(order by timestamp desc) as rownum
            from customers_analytics
            where timestamp in (
              select
              dateadd(day, -7, timestamp) as timestamp
              from customers_analytics
              )
          ) as c
          on a.rownum=c.rownum
          left join
          (
            select
              free_trial_created as new_trials_7_days_prior,
              row_number() over(order by date desc) as rownum
            from chargebee_webhook_analytics
            where date in (
              select
              date(dateadd(day, -7, timestamp)) as date
              from customers_analytics
              )
          ) as d
          on a.rownum=d.rownum
          left join
          (
            select
              free_trial_created as new_trials_14_days_prior,
              row_number() over(order by date desc) as rownum
            from chargebee_webhook_analytics
            where date in (
              select
              date(dateadd(day, -14, timestamp)) as date
              from customers_analytics
              )
          ) as e
          on a.rownum=e.rownum
        )
      ) as a
      left join customers.churn_reasons_aggregated as b on a.timestamp=b.timestamp)) as a))
      , outer_query as (
        select f.*,paying_30_days_prior,churn_30_days,churn_30_day_percent,winback_30_days from e inner join f on e.timestamp=f.timestamp
      )
      select * from outer_query
      ;;
    datagroup_trigger: upff_acquisition_reporting
    distribution_style: all
  }

  parameter: subscription_frequency {
    label: "Subscription Frequency"
    type: unquoted
    default_value: "distinct_events"
    allowed_value: {label: "All" value:"distinct_events"}
    allowed_value: {label: "Monthly" value:"distinct_events_monthly"}
    allowed_value: {label: "Yearly" value:"distinct_events_yearly"}
  }

  measure: winback_30_days {
    type: sum
    sql: ${TABLE}.winback_30_days ;;
  }

  dimension: paying_30_days_prior {
    type: number
    sql: ${TABLE}.paying_30_days_prior ;;
  }

  dimension: id {
    type: string
    primary_key: yes
    sql: ${TABLE}.id ;;
  }


  dimension: user_id {
    type: number
    tags: ["user_id"]
    sql: 1 ;;
  }

  measure: paying_30_days_prior_ {
    type: sum
    sql: ${paying_30_days_prior} ;;
  }

  dimension: churn_30_days {
    type: number
    sql: churn_30_days ;;
  }

  measure: churn_30_days_ {
    type: sum
    sql: ${churn_30_days} ;;
  }

  dimension: running_free_trials {
    type: number
    sql: ${TABLE}.Running_Free_Trials ;;
  }

  measure: running_free_trials_ {
    type: sum
    sql: ${running_free_trials} ;;
  }

  dimension: running_free_trial_target {
    type: number
    sql: ${TABLE}.running_free_trial_target*2 ;;
  }

  measure: running_free_trial_target_{
    type: sum
    sql: ${running_free_trial_target} ;;
  }

  dimension: target {
    type: number
    sql: ${TABLE}.target ;;
  }

  measure: targets {
    type: sum
    sql: ${target} ;;
  }

  dimension: total_target {
    type: number
    sql: ${TABLE}.total_target ;;
  }

  measure: total_target_ {
    type: sum
    sql: ${total_target} ;;
  }

  dimension: target_14_days_future {
    type: number
    sql: ${TABLE}.target_14_days_future ;;
  }

  measure: target_14_days_future_ {
    type: sum
    sql: ${target_14_days_future} ;;
  }

  dimension: annual_target {
    type: number
    sql: ${TABLE}.annual_target ;;
  }

  measure: annual_targets {
    type: sum
    sql: ${annual_target} ;;
  }

  dimension: day_of_year {
    type: number
    sql: ${TABLE}.day_of_year ;;
  }

  dimension: day_of_year_14 {
    type: number
    sql: ${TABLE}.day_of_year_14_days ;;
  }

  dimension: avg_target_subs_per_day {
    type:  number
    sql: (${annual_target}-(${TABLE}.total_paying))/(365-${TABLE}.day_of_year);;
  }

  measure: avg_targets_subs_per_day {
    type:  sum
    sql: ${avg_target_subs_per_day};;
  }

  measure: avg_targets_trials_per_day {
    type:  sum
    sql: ${avg_target_subs_per_day}*2;;
  }

  dimension: avg_target_subs_per_day_14_days {
    type:  number
    sql: (365-${TABLE}.day_of_year_14_days);;
  }

  measure: avg_targets_subs_per_day_14_days_ {
    type:  sum
    sql: ${avg_target_subs_per_day_14_days};;
  }

  measure: running_target {
    type: running_total
    sql: ${avg_target_subs_per_day_14_days} ;;
  }

  dimension: high_price {
    type: number
    sql: ${TABLE}.high_price ;;
  }

  dimension: other {
    type: number
    sql: ${TABLE}.other ;;
  }

  dimension: save_money {
    type: string
    sql: ${TABLE}.save_money ;;
  }

  measure: high_price_total {
    type: sum
    sql: ${TABLE}.high_price ;;
    drill_fields: [high_price,timestamp_date]
  }

  measure: other_total {
    type: sum
    sql: ${TABLE}.other ;;
    drill_fields: [other,timestamp_date]
  }

  measure: save_money_total {
    type: sum
    sql: ${TABLE}.save_money ;;
    drill_fields: [save_money,timestamp_date]
  }

  dimension: vacation {
    type: number
    sql: ${TABLE}.vacation ;;
  }

  dimension: wait_content {
    type: number
    sql: ${TABLE}.wait_content ;;
  }

  measure: vacation_total {
    type: sum
    sql: ${TABLE}.vacation ;;
    drill_fields: [vacation,timestamp_date]
  }

  measure: wait_content_total {
    type: sum
    sql: ${TABLE}.wait_content ;;
    drill_fields: [wait_content,timestamp_date]
  }

  dimension: new_trials_14_days_prior{
    type: number
    sql: ${TABLE}.new_trials_14_days_prior;;
  }

  dimension: conversion {
    type: number
    value_format: ".0#\%"
    sql: 100.0*${TABLE}.free_trial_converted/${TABLE}.new_trials_14_days_prior ;;
  }

  measure: total_new_trials_14_days_prior {
    type: sum
    sql: ${TABLE}.new_trials_14_days_prior;;
    drill_fields: [new_trials_14_days_prior,timestamp_date]
  }

  dimension: existing_free_trials {
    type: number
    sql: ${TABLE}.existing_free_trials ;;
  }

  measure: total_active_free_trials {
    type: sum
    sql:${existing_free_trials} ;;
  }

  dimension: existing_paying {
    type: number
    sql: ${TABLE}.existing_paying ;;
  }

  measure: total_active_paying {
    type: sum
    sql: ${existing_paying} ;;
  }

  measure: total_active_subs {
    type: number
    sql: ${existing_free_trials} + ${existing_paying} ;;
  }


  dimension: free_trial_churn {
    type: number
    sql: ${TABLE}.free_trial_churn ;;
  }

  measure: new_cancelled_trials {
    type: sum
    description: "Total number of cancelled trials during a time period."
    sql:  ${free_trial_churn} ;;
    drill_fields: [timestamp_date, free_trial_churn]
  }

  measure: cancelled_trials {
    type: sum
    description: "Total number of cancelled trials during a time period."
    sql:  ${free_trial_churn}*-1 ;;
    drill_fields: [timestamp_date, free_trial_churn]
  }

  measure: free_trials_count {
    type: sum
    description: "Total number of existing trials during a period of time"
    sql:  ${existing_free_trials} ;;
  }

  measure: paid_subs_count {
    type: sum
    description: "Total number of existing paid subs during a period of time"
    sql:  ${existing_paying} ;;
  }


  dimension: free_trial_converted {
    type: number
    sql: ${TABLE}.free_trial_converted ;;
  }

  measure: trial_to_paid {
    type: sum
    description: "Total number of trials to paid during a time period."
    sql:  ${free_trial_converted} ;;
    drill_fields: [free_trial_converted,timestamp_date]
  }

  measure: trial_to_paid_a {
    type: sum
    description: "Total number of trials to paid during a period a."
    sql:  ${free_trial_converted} ;;
    filters: {
      field: group_a
      value: "yes"
    }
  }

  measure: trial_to_paid_b {
    type: sum
    description: "Total number of trials to paid during a period b."
    sql:  ${free_trial_converted} ;;
    filters: {
      field: group_b
      value: "yes"
    }
  }

  dimension: free_trial_created {
    type: number
    sql: ${TABLE}.free_trial_created ;;
  }
  measure: new_trials {
    type: sum
    description: "Total number of new trials during a time period."
    sql:  ${free_trial_created} ;;
  }
  measure: new_trials_a {
    type: sum
    description: "Total number of new trials during period a."
    sql:  ${free_trial_created} ;;
    filters: {
      field: group_a
      value: "yes"
    }
  }
  measure: new_trials_b {
    type: sum
    description: "Total number of new trials during period b."
    sql:  ${free_trial_created} ;;
    filters: {
      field: group_b
      value: "yes"
    }
  }
  measure: new_paid_a {
    type: sum
    description: "Total number of new paids during period a."
    sql:  ${paying_created} ;;
    filters: {
      field: group_a
      value: "yes"
    }
  }

  measure: new_paid_b {
    type: sum
    description: "Total number of new paids during period b."
    sql:  ${paying_created} ;;
    filters: {
      field: group_b
      value: "yes"
    }
  }

  measure:  new_paid_total_a {
    type: number
    description: "Total number of new paid subs (reacquisitions) and free trial to paid during period a."
    sql: ${trial_to_paid_a}+${new_paid_a};;
  }

  measure:  new_paid_total_b {
    type: number
    description: "Total number of new paid subs (reacquisitions) and free trial to paid during period b."
    sql: ${trial_to_paid_b}+${new_paid_b};;
  }

  measure: net_new_a {
    type: number
    description: "Net new subscribers after trial conversions and paying churn during period A"
    sql: ${trial_to_paid_a}+${new_paid_a}-${paid_churn_a} ;;
  }

  measure: net_new_b {
    type: number
    description: "Net new subscribers after trial conversions and paying churn during period B"
    sql: ${trial_to_paid_b}+${new_paid_b}-${paid_churn_b} ;;
  }

  dimension: paused_created {
    type: number
    sql: ${TABLE}.paused_created ;;
  }

  dimension: paying_created {
    type: number
    sql: ${TABLE}.paying_created ;;
  }

  dimension: paying_churn {
    type: number
    sql: ${TABLE}.paying_churn ;;
  }

  measure: new_cancelled_paid {
    type: sum
    description: "Total number of cancelled paid subs during a time period."
    sql:  ${paying_churn} ;;
    drill_fields: [timestamp_date, paying_churn]
  }

  measure: total_cancelled {
    type: sum
    description: "Total number of cancelled free trials and paid subs during a time period."
    sql: ${paying_churn}+${free_trial_churn} ;;
  }
  measure: new_paid {
    type: sum
    description: "Total number of new paids during a time period."
    sql:  ${paying_created} ;;
    drill_fields: [paying_created,timestamp_date]
  }

  measure: new_total {
    type: sum
    description: "Total number of new free trials and paid subs during a time period."
    sql:  ${paying_created}+${free_trial_created}+${free_trial_converted};;
  }

  measure:  new_paid_total{
    type: sum
    description: "Total number of new paid subs (reacquisitions) and free trial to paid."
    sql: ${free_trial_converted}+${paying_created};;
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
    sql: ${TABLE}.timestamp ;;
  }
# Added for AGM Tues-Mon weekly reporting
  dimension_group: week_start_tuesday {
    type: time
    timeframes: [raw, date, day_of_week, week, month]
    sql: CASE
      WHEN ${timestamp_day_of_week} = 'Tuesday' THEN ${timestamp_date}
      WHEN ${timestamp_day_of_week} = 'Wednesday' THEN dateadd(days, -1, ${timestamp_date})
      WHEN ${timestamp_day_of_week} = 'Thursday' THEN dateadd(days, -2, ${timestamp_date})
      WHEN ${timestamp_day_of_week} = 'Friday' THEN dateadd(days, -3,  ${timestamp_date})
      WHEN ${timestamp_day_of_week} = 'Saturday' THEN dateadd(days, -4, ${timestamp_date})
      WHEN ${timestamp_day_of_week} = 'Sunday' THEN dateadd(days, -5, ${timestamp_date})
      WHEN ${timestamp_day_of_week} = 'Monday' THEN dateadd(days, -6, ${timestamp_date})
      END;;
    datatype: date
  }

# My original dimension group
  dimension_group: created {
    hidden: yes
    type: time
    timeframes: [date,time,week,month]
    sql: ${TABLE}.timestamp ;;
  }

# My customized timeframes, added under the group "Created"
  dimension: date_formatted {
    group_label: "Created" label: "Date"
    sql: ${created_date} ;;
    html: {{ rendered_value | date: "%b %d, %y" }};;
  }

  dimension: weekday_formatted {
    group_label: "Created" label: "WeekDay"
    sql: ${created_date} ;;
    html: {{ rendered_value | date: "%a" }};;
  }


  dimension: total_free_trials {
    type: number
    sql: ${TABLE}.total_free_trials ;;
  }

  dimension: total_paying {
    type: number
    sql: ${TABLE}.total_paying ;;
  }

  measure: paying_total {
    type: sum
    sql: ${TABLE}.total_paying ;;
  }

  measure: free_trials_total {
    type: sum
    sql: ${TABLE}.total_free_trials ;;
  }

  measure: total_count {
    type: sum
    description: "Total number of existing free trials and paid subs during a period of time"
    sql:  ${total_paying}+${total_free_trials} ;;
  }


  dimension: churn_rate {
    type: number
    value_format: ".0#\%"
    sql: 100.0*${TABLE}.paying_churn/${TABLE}.existing_paying ;;
  }

  dimension: rownum {
    type: number
    sql: {TABLE}.rownum ;;
  }

  measure: minrow {
    type: min
    sql: ${TABLE}.rownum ;;
  }

  measure: last_updated_date {
    type: date
    sql: MAX(${timestamp_raw});;
  }

  measure: end_of_prior_week_subs {
    type: sum
    sql: ${TABLE}.PriorWeekExistingSubs ;;
  }

  measure: end_of_prior_month_subs {
    type: sum
    sql: ${TABLE}.PriorMonthExistingSubs ;;
  }

  measure: end_of_week_subs {
    type: sum
    sql: ${TABLE}.CurrentWeekExistingSubs ;;
  }

  measure: end_of_month_subs {
    type: sum
    sql: ${TABLE}.CurrentMonthExistingSubs ;;
  }

  measure: weekly_churn {
    type: number
    value_format: ".0#\%"
    sql: 100.0*${new_cancelled_paid}/${end_of_prior_week_subs} ;;
  }

  measure: monthly_churn {
    type: number
    sql: ${new_cancelled_paid}/${end_of_prior_month_subs} ;;
  }

  measure: count {
    type: count
    drill_fields: []
  }

  measure: trial_to_paid_count {
    type: number
    description: "Total number of trials to paid during a time period."
    sql:  COUNT(${free_trial_converted}) ;;
    drill_fields: [free_trial_converted,timestamp_date]

  }

  measure: PaidTrialLost {
    type: sum
    sql: ${paying_created}-${paying_churn}  ;;

  }

  measure: Cancelled_Subs {
    type: sum
    sql: ${paying_churn}*-1 ;;
  }

  measure: total_new_trials_14_days_prior_a {
    type: sum
    sql: ${TABLE}.new_trials_14_days_prior;;
    filters: {
      field: group_a
      value: "yes"
    }
  }

  measure: total_new_trials_14_days_prior_b {
    type: sum
    sql: ${TABLE}.new_trials_14_days_prior;;
    filters: {
      field: group_b
      value: "yes"
    }
  }

  measure: conversion_rate_v2 {
    type: number
    value_format: ".0#\%"
    sql: 100.0*${trial_to_paid}/NULLIF(${total_new_trials_14_days_prior},0) ;;
  }

  measure: conversion_rate_a {
    type: number
    value_format: ".0#\%"
    sql: 100.0*${trial_to_paid_a}/NULLIF(${total_new_trials_14_days_prior_a},0) ;;
  }

  measure: conversion_rate_b {
    type: number
    value_format: ".0#\%"
    sql: 100.0*${trial_to_paid_b}/NULLIF(${total_new_trials_14_days_prior_b},0) ;;
  }

  measure: total_free_trial_change {
    type: number
    sql: (${free_trials_total}-${free_trials_count});;
  }

  measure: total_paid_sub_change {
    type: number
    sql: (${paying_total}-${paid_subs_count});;
  }

  measure: net_gained {
    type: number
    sql: (${new_trials}+${trial_to_paid}+${new_paid})+(${cancelled_trials}+${Cancelled_Subs}) ;;
  }

  measure: net_paid {
    type: number
    sql: (${trial_to_paid}+${new_paid})+(${Cancelled_Subs}) ;;
  }

  measure: net_trials {
    type: number
    sql: (${new_trials})+(${cancelled_trials}-${trial_to_paid}) ;;
  }

# ------
# Filters
# ------

## filter determining time range for all "A" measures
  filter: time_a {
    type: date_time
  }

## flag for "A" measures to only include appropriate time range
  dimension: group_a {
    hidden: yes
    type: yesno
    sql: {% condition time_a %} ${timestamp_raw} {% endcondition %}
      ;;
  }

  measure: free_trial_created_14_days_prior_a {
    type: sum
    sql:  ${free_trial_created} ;;
    filters: {
      field: group_a
      value: "yes"
    }
  }

  measure: free_trial_created_14_days_prior_b {
    type: sum
    sql:  ${free_trial_created} ;;
    filters: {
      field: group_b
      value: "yes"
    }
  }

  measure: new_trial_14_days_prior {
    type: sum
    sql:  ${free_trial_created}-14 ;;
  }

  measure: free_trial_converted_today {
    type: sum
    sql:  ${free_trial_converted} ;;
    filters: {
      field: group_b
      value: "yes"
    }
  }



## filter determining time range for all "B" measures
  filter: time_b {
    type: date_time
  }

## flag for "B" measures to only include appropriate time range
  dimension: group_b {
    hidden: yes
    type: yesno
    sql: {% condition time_b %} ${timestamp_raw} {% endcondition %}
      ;;
  }

  measure: count_b {
    type: sum
    sql:  ${free_trial_created} ;;
    filters: {
      field: group_b
      value: "yes"
    }
  }

  measure: paid_a {
    type: sum
    sql:  ${total_paying};;
    filters: {
      field: group_a
      value: "yes"
    }
  }

  measure: paid_b {
    type: sum
    sql:  ${total_paying};;
    filters: {
      field: group_b
      value: "yes"
    }
  }

  measure: trials_a {
    type: sum
    sql:  ${total_free_trials};;
    filters: {
      field: group_a
      value: "yes"
    }
  }

  measure: conversions_a {
    type: sum
    sql: ${free_trial_converted};;
    filters: {
      field: group_a
      value: "yes"
    }
  }

  measure: reacquisitions_a {
    type: sum
    sql: ${paying_created};;
    filters: {
      field: group_a
      value: "yes"
    }
  }

  measure: reacquisitions_b {
    type: sum
    sql: ${paying_created};;
    filters: {
      field: group_b
      value: "yes"
    }
  }

  measure: paid_churn_a {
    type: sum
    sql: ${paying_churn} ;;
    filters: {
      field: group_a
      value: "yes"
    }
  }

  measure: paid_churn_b {
    type: sum
    sql: ${paying_churn} ;;
    filters: {
      field: group_b
      value: "yes"
    }
  }

  measure: churn_30_day_percent {
    type: sum
    label: "Churn Rate"
    sql: ${churn_30_days} * 1.0 / NULLIF(${paying_30_days_prior}, 0);;
    value_format_name: percent_2
  }

  measure: churn_30_day_percent_a {
    type: sum
    sql: ${churn_30_days}/NULLIF(${paying_30_days_prior}, 0);;
    value_format_name: percent_1
    filters: {
      field: group_a
      value: "yes"
    }
  }


  measure: churn_30_day_percent_b {
    type: sum
    sql: ${churn_30_days}/NULLIF(${paying_30_days_prior}, 0);;
    value_format_name: percent_1
    filters: {
      field: group_b
      value: "yes"
    }
  }

  measure: trial_churn_a {
    type: sum
    sql: ${free_trial_churn} ;;
    filters: {
      field: group_a
      value: "yes"
    }
  }

  measure: trial_churn_b {
    type: sum
    sql: ${free_trial_churn} ;;
    filters: {
      field: group_b
      value: "yes"
    }
  }

  measure: trial_starts_a {
    type: sum
    sql: ${free_trial_created} ;;
    filters: {
      field: group_a
      value: "yes"
    }
  }


  measure: churn_percent_b {
    type: sum
    sql: ${TABLE}.churn_30_day_percent ;;
    filters: {
      field: group_b
      value: "yes"
    }
  }
  measure: avg_paid_b {
    type: average
    sql:  ${total_paying};;
    filters: {
      field: group_b
      value: "yes"
    }
  }

  measure: paid_change {
    type: number
    sql: (${paid_a}-${avg_paid_b}) ;;
  }

  measure: avg_trials_b {
    type: average
    sql:  ${total_free_trials};;
    filters: {
      field: group_b
      value: "yes"
    }
  }

  measure: trials_change {
    type: number
    sql: (${trials_a}-${avg_trials_b}) ;;
  }

  measure: trials_by_day {
    type: number
    sql: (${avg_trials_b}-${trials_a}) ;;
  }

  measure: avg_conversions_b {
    type: average
    sql:  ${free_trial_converted};;
    filters: {
      field: group_b
      value: "yes"
    }
  }

  measure: conversion_change {
    type: number
    sql: (${conversions_a}-${avg_conversions_b}) ;;
  }

  measure: avg_reacquisitions_b {
    type: average
    sql:  ${paying_created};;
    filters: {
      field: group_b
      value: "yes"
    }
  }

  measure: reacquisition_change {
    type: number
    sql: (${reacquisitions_a}-${avg_reacquisitions_b}) ;;
  }

  measure: avg_paid_churn_b {
    type: average
    sql:  ${paying_churn};;
    filters: {
      field: group_b
      value: "yes"
    }
  }

  measure: paid_churn_change {
    type: number
    sql: (${paid_churn_a}-${avg_paid_churn_b}) ;;
  }

  measure: avg_trial_churn_b {
    type: average
    sql:  ${free_trial_churn};;
    filters: {
      field: group_b
      value: "yes"
    }
  }

  measure: trial_churn_change {
    type: number
    sql: (${trial_churn_a}-${avg_trial_churn_b}) ;;
  }

  measure: avg_trial_starts_b {
    type: average
    sql:  ${free_trial_created};;
    filters: {
      field: group_b
      value: "yes"
    }
  }

  measure: trials_created_change {
    type: number
    sql: (${trial_starts_a}-${avg_trial_starts_b}) ;;
  }

## filter on comparison queries to avoid querying unnecessarily large date ranges.
  dimension: is_in_time_a_or_b {
    group_label: "Time Comparison Filters"
    type: yesno
    sql: {% condition time_a %} ${timestamp_raw} {% endcondition %}
          OR {% condition time_b %} ${timestamp_raw} {% endcondition %}
           ;;
  }

  dimension: is_in_time_a {
    group_label: "Group A Comparison Filter"
    type: yesno
    sql:{% condition time_a %} ${timestamp_raw} {% endcondition %};;
  }

  dimension: is_in_time_b {
    group_label: "Group B Comparison Filter"
    type: yesno
    sql:{% condition time_b %} ${timestamp_raw} {% endcondition %};;
  }

  parameter: date_granularity {
    type: string
    allowed_value: { value: "Day" }
    allowed_value: { value: "Week"}
    allowed_value: { value: "Month" }
    allowed_value: { value: "Quarter" }
    allowed_value: { value: "Year" }
  }

  dimension: date {
    label_from_parameter: date_granularity
    sql:
       CASE
         WHEN {% parameter date_granularity %} = 'Day' THEN
           ${timestamp_date}::VARCHAR
         WHEN {% parameter date_granularity %} = 'Week' THEN
           ${timestamp_week}::VARCHAR
         WHEN {% parameter date_granularity %} = 'Month' THEN
           ${timestamp_month}::VARCHAR
         WHEN {% parameter date_granularity %} = 'Quarter' THEN
           ${timestamp_quarter}::VARCHAR
         WHEN {% parameter date_granularity %} = 'Year' THEN
           ${timestamp_year}::VARCHAR
         ELSE
           NULL
       END ;;
  }

  ## PoP dimensions and measures created for H16 analysis

  ## ------------------ HIDDEN HELPER DIMENSIONS  ------------------ ##

  dimension: days_from_start_a {
    hidden: no
    group_label: "Time Comparison Filters"
    type: number
    sql: DATEDIFF('day',  {% date_start time_a %}, ${timestamp_date}) ;;
  }

  dimension: days_from_start_b {
    hidden: no
    group_label: "Time Comparison Filters"
    type: number
    sql: DATEDIFF('day',  {% date_start time_b %}, ${timestamp_date}) ;;
  }


  ## ------------------ DIMENSIONS TO PLOT ------------------ ##

  dimension: days_from_first_period {
    label: "Day of Period"
    description: "Select for Grouping (Rows)"
    group_label: "Time Comparison Filters"
    type: number
    sql:
            CASE
            WHEN ${days_from_start_b} >= 0
            THEN ${days_from_start_b}
            WHEN ${days_from_start_a} >= 0
            THEN ${days_from_start_a}
            END;;
  }


  dimension: period_selected {
    label: "Period"
    description: "Select for Comparison (Pivot)"
    group_label: "Time Comparison Filters"
    type: string
    sql:
            CASE
                WHEN {% condition time_a %}${timestamp_raw} {% endcondition %}
                THEN 'Period A'
                WHEN {% condition time_b %}${timestamp_raw} {% endcondition %}
                THEN 'Period B'
                END ;;
  }

}
