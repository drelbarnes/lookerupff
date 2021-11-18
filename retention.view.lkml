view: retention {
    derived_table: {
      sql: with

              paid_campaign as (
              select
                context_campaign_source,
                context_campaign_medium,
                anonymous_id,
                context_ip
              from javascript_upff_home.pages
              where context_campaign_source in ('fb','bing_ads','ig')
              ),

              order_completed as (
              select
                user_id,
                anonymous_id,
                context_ip
                from javascript.order_completed
              ),

              order_completed_by_campaign_p1 as (
              select oc.user_id
              from order_completed as oc
              left join paid_campaign as pc
              on oc.anonymous_id = pc.anonymous_id
              ),

              order_completed_by_campaign_p2 as (
              select oc.user_id
              from order_completed as oc
              left join paid_campaign as pc
              on oc.context_ip = pc.context_ip
              ),

              order_completed_by_campaign_full as (
              select * from order_completed_by_campaign_p1
              union all
              select * from order_completed_by_campaign_p2
              ),

              order_completed_by_campaign as (
              select
                distinct user_id,
                1 as paid_sub
              from order_completed_by_campaign_full
              ),

              purchase_event as ( /* a */
              select * from http_api.purchase_event
              ),

              purchase_event_analytic as ( /* b */
              select
                user_id,
                row_number() over (partition by user_id order by timestamp) as event_num,
                topic,
                platform,
                subscription_frequency,
                date(created_at) as create_dt,
                date(timestamp) as status_dt,
                date_diff(date(created_at), date(timestamp), day) as tenure,
                extract(dayofweek from date(created_at)) as create_day,
                case when topic in ('customer.product.free_trial_created') then 1 else 0 end as trialist
              from purchase_event
              where topic is not null
              order by user_id
              ),

              paid_media_users_for_churn as (
              select pe.*, oc.paid_sub
              from purchase_event_analytic as pe
              left join order_completed_by_campaign as oc
              on pe.user_id = oc.user_id
              ),

              free_trial_date as (
              select
                user_id,
                case when topic = 'customer.product.free_trial_created' then status_dt else null end as trial_dt
              from purchase_event_analytic
              ),

              free_trial_date_lookup as (
              select
                user_id,
                min(trial_dt) as min_trial_dt
              from free_trial_date
              where trial_dt is not null
              group by user_id
              ),

              free_trial_subs_aggregate as (
              select
                user_id,
                trialist
                from purchase_event_analytic
                group by 1,2
              ),

              free_trial_subs_lookup as ( /*** creates lookup table for trialists ***/
              select user_id, trialist from free_trial_subs_aggregate where trialist = 1
              ),

              last_event as ( /* c */
              select
                user_id,
                max(event_num) as max_event,
                max(status_dt) as max_status
              from purchase_event_analytic
              group by 1
              order by 1
              ),

              analytic_table as ( /* d */
              select b.*, c.max_event, c.max_status, f.min_trial_dt
              from paid_media_users_for_churn as b
              inner join last_event as c
              on b.user_id = c.user_id and b.event_num = c.max_event
              inner join free_trial_date_lookup as f
              on b.user_id = f.user_id
              where b.user_id in (select user_id from free_trial_subs_lookup)
              order by b.user_id
              ),

              pre_flags as (
              select *, date_diff(max_status, date(min_trial_dt), day) as tenure2
              from analytic_table
              ),

              flags as ( /*** e ***/
              select *,
                case
                  when topic in ('customer.deleted', 'customer.product.free_trial_expired', 'customer.product.cancelled', 'customer.product.expired') then 1 else 0 end as churn_status,
                case
                  when tenure2 between 0 and 30 then 1
                  when tenure2 between 30 and 60 then 2
                  when tenure2 between 60 and 90 then 3
                  when tenure2 between 90 and 120 then 4
                  when tenure2 between 120 and 150 then 5
                  when tenure2 between 150 and 180 then 6
                  when tenure2 between 180 and 210 then 7
                  when tenure2 between 210 and 240 then 8
                  when tenure2 between 240 and 270 then 9
                  when tenure2 between 270 and 300 then 10
                  when tenure2 between 300 and 330 then 11
                  when tenure2 between 330 and 360 then 12
                  when tenure2 between 360 and 390 then 13
                  when tenure2 between 390 and 420 then 14
                  when tenure2 between 420 and 450 then 15
                  when tenure2 between 450 and 480 then 16
                  when tenure2 between 480 and 510 then 17
                  when tenure2 between 510 and 540 then 18
                  when tenure2 between 540 and 570 then 19
                  when tenure2 between 570 and 600 then 20
                  when tenure2 between 600 and 630 then 21
                  when tenure2 between 630 and 660 then 22
                  when tenure2 between 660 and 690 then 23
                  when tenure2 between 690 and 720 then 24
                  when tenure2 between 720 and 750 then 25
                  when tenure2 between 750 and 780 then 26
                  when tenure2 between 780 and 810 then 27
                  when tenure2 between 810 and 840 then 28
                  when tenure2 between 840 and 870 then 29
                  when tenure2 between 870 and 900 then 30
                  when tenure2 between 900 and 930 then 31
                  when tenure2 between 930 and 960 then 32
                  when tenure2 between 960 and 990 then 33
                  when tenure2 between 990 and 1020 then 34
                  when tenure2 between 1020 and 1050 then 35
                  else 36 end as num_months,
                extract(dayofweek from min_trial_dt) as trial_day,
                case
                  when date(min_trial_dt) between '2019-01-01' and '2019-03-31' then '2019-Q1'
                  when date(min_trial_dt) between '2019-04-01' and '2019-06-30' then '2019-Q2'
                  when date(min_trial_dt) between '2019-07-01' and '2019-09-30' then '2019-Q3'
                  when date(min_trial_dt) between '2019-10-01' and '2019-12-31' then '2019-Q4'
                  when date(min_trial_dt) between '2020-01-01' and '2020-03-31' then '2020-Q1'
                  when date(min_trial_dt) between '2020-04-01' and '2020-06-30' then '2020-Q2'
                  when date(min_trial_dt) between '2020-07-01' and '2020-09-30' then '2020-Q3'
                  when date(min_trial_dt) between '2020-10-01' and '2020-12-31' then '2020-Q4'
                  when date(min_trial_dt) between '2021-01-01' and '2021-03-31' then '2021-Q1'
                  when date(min_trial_dt) between '2021-04-01' and '2021-06-30' then '2021-Q2'
                  when date(min_trial_dt) between '2021-07-01' and '2021-09-30' then '2021-Q3'
                else 'Missing' end as quarter,
                case
                  when date(min_trial_dt) between '2019-01-01' and '2019-01-31' then '2019-01'
                  when date(min_trial_dt) between '2019-02-01' and '2019-02-28' then '2019-02'
                  when date(min_trial_dt) between '2019-03-01' and '2019-03-31' then '2019-03'
                  when date(min_trial_dt) between '2019-04-01' and '2019-04-30' then '2019-04'
                  when date(min_trial_dt) between '2019-05-01' and '2019-05-31' then '2019-05'
                  when date(min_trial_dt) between '2019-06-01' and '2019-06-30' then '2019-06'
                  when date(min_trial_dt) between '2019-07-01' and '2019-07-31' then '2019-07'
                  when date(min_trial_dt) between '2019-08-01' and '2019-08-31' then '2019-08'
                  when date(min_trial_dt) between '2019-09-01' and '2019-09-30' then '2019-09'
                  when date(min_trial_dt) between '2019-10-01' and '2019-10-31' then '2019-10'
                  when date(min_trial_dt) between '2019-11-01' and '2019-11-30' then '2019-11'
                  when date(min_trial_dt) between '2019-12-01' and '2019-12-31' then '2019-12'
                  when date(min_trial_dt) between '2020-01-01' and '2020-01-31' then '2020-01'
                  when date(min_trial_dt) between '2020-02-01' and '2020-02-29' then '2020-02'
                  when date(min_trial_dt) between '2020-03-01' and '2020-03-31' then '2020-03'
                  when date(min_trial_dt) between '2020-04-01' and '2020-04-30' then '2020-04'
                  when date(min_trial_dt) between '2020-05-01' and '2020-05-31' then '2020-05'
                  when date(min_trial_dt) between '2020-06-01' and '2020-06-30' then '2020-06'
                  when date(min_trial_dt) between '2020-07-01' and '2020-07-31' then '2020-07'
                  when date(min_trial_dt) between '2020-08-01' and '2020-08-31' then '2020-08'
                  when date(min_trial_dt) between '2020-09-01' and '2020-09-30' then '2020-09'
                  when date(min_trial_dt) between '2020-10-01' and '2020-10-31' then '2020-10'
                  when date(min_trial_dt) between '2020-11-01' and '2020-11-30' then '2020-11'
                  when date(min_trial_dt) between '2020-12-01' and '2020-12-31' then '2020-12'
                  when date(min_trial_dt) between '2021-01-01' and '2021-01-31' then '2021-01'
                  when date(min_trial_dt) between '2021-02-01' and '2021-02-28' then '2021-02'
                  when date(min_trial_dt) between '2021-03-01' and '2021-03-31' then '2021-03'
                  when date(min_trial_dt) between '2021-04-01' and '2021-04-30' then '2021-04'
                  when date(min_trial_dt) between '2021-05-01' and '2021-05-31' then '2021-05'
                  when date(min_trial_dt) between '2021-06-01' and '2021-06-30' then '2021-06'
                  when date(min_trial_dt) between '2021-07-01' and '2021-07-31' then '2021-07'
                  when date(min_trial_dt) between '2021-08-01' and '2021-08-31' then '2021-08'
                  when date(min_trial_dt) between '2021-09-01' and '2021-09-30' then '2021-09'
                else 'Missing' end as month
              from pre_flags
              ),

              lkup as (
              select user_id, min_trial_dt from flags where month in (
              '2021-01',
              '2021-02',
              '2021-03',
              '2021-04',
              '2021-05',
              '2021-06',
              '2021-07',
              '2021-08')
              ),

              lkup_firstplay as (
              select
                cast(user_id as string) as user_id,
                seriesname_watched_1st as series,
                case
                  when seriesname_watched_1st = 'Heartland' then 'Heartland'
                  when seriesname_watched_1st = 'Bringing Up Bates' then 'Bates'
                  when seriesname_watched_1st not in ('Heartland','Bringing Up Bates') then 'Other Serials'
                  when seriesname_watched_1st is null then 'Movie'
                else 'Null' end as first_play
                from ad_hoc.first_watched
              ),

              missing_user as (
              select
                user_id from lkup where user_id not in (select user_id from lkup_firstplay)
              ),

              post_flags as (
              select a.*, b.*
              from flags as a
              left join lkup_firstplay as b
              on a.user_id = b.user_id
              ),

              aggregate as (
              select *
              from post_flags
              where create_day is not null
              ),

              counts as (
              select 'paid_campaign' as table_name, count(*) as size from paid_campaign
              union all
              select 'order_completed' as table_name, count(*) as size  from order_completed as order_completed_counts
              union all
              select 'order_completed_by_campaign_p1' as table_name, count(*) as size  from order_completed_by_campaign_p1 as order_completed_by_campaign_p1_counts
              union all
              select 'order_completed_by_campaign_p2' as table_name, count(*) as size  from order_completed_by_campaign_p2 as order_completed_by_campaign_p2_counts
              union all
              select 'order_completed_by_campaign_full' as table_name, count(*) as size  from order_completed_by_campaign_full as order_completed_by_campaign_full_counts
              union all
              select 'order_completed_by_campaign' as table_name, count(*) as size  from order_completed_by_campaign as order_completed_by_campaign_counts
              union all
              select 'purchase_event' as table_name, count(*) as size  from purchase_event as purchase_event_counts
              union all
              select 'purchase_event_analytic' as table_name, count(*) as size  from purchase_event_analytic as purchase_event_analytic_counts
              union all
              select 'paid_media_users_for_churn' as table_name, count(*) as size  from paid_media_users_for_churn as paid_media_users_for_churn_counts
              union all
              select 'free_trial_date' as table_name, count(*) as size  from free_trial_date as free_trial_date_counts
              union all
              select 'free_trial_date_lookup' as table_name, count(*) as size  from free_trial_date_lookup as free_trial_date_lookup_counts
              union all
              select 'free_trial_subs_aggregate' as table_name, count(*) as size  from free_trial_subs_aggregate as free_trial_subs_aggregate_counts
              union all
              select 'free_trial_subs_lookup' as table_name, count(*) as size  from free_trial_subs_lookup as free_trial_subs_lookup_counts
              union all
              select 'last_event_' as table_name, count(*) as size  from last_event as last_event_counts
              union all
              select 'analytic_table' as table_name, count(*) as size from analytic_table as analytic_table_counts
              union all
              select 'flags' as table_name, count(*) as size  from flags as flags_counts
              union all
              select 'aggregate' as table_name, count(*) as size  from aggregate as aggregate_counts
              ),

              tenure_mismatch as (
              select * from flags where month = '2021-09' and tenure > 30
              )

              select * from aggregate where month in
              (
              '2021-01',
              '2021-02',
              '2021-03',
              '2021-04',
              '2021-05',
              '2021-06',
              '2021-07',
              '2021-08')

              /*
              select count(*) as n from flags
              union all
              select count(*) as n from post_flags
              */

              /*
              select
                count(*) as n,
                heartland_flg,
                bates_flg,
                other_flg,
                movie_flg
              from lkup_firstplay
              group by 2,3,4,5
              */

              /*
              select count(*) as n, heartland_flg, bates_flg, other_flg, null_flg from lkup_firstplay group by 2,3,4,5 order by 2
              */

              /*
              select * from ad_hoc.first_watched
              */

              /*
              select * from aggregate where month in
              (
              '2021-01',
              '2021-02',
              '2021-03',
              '2021-04',
              '2021-05',
              '2021-06',
              '2021-07',
              '2021-08')
              */

              /*
              select month, tenure2, count(*) as n from flags where month in
              (
              '2020-04',
              '2020-05',
              '2020-06',
              '2020-07',
              '2020-08')
              group by 1,2
              order by 1,2
              */
               ;;
    }

    measure: count {
      type: count
      drill_fields: [detail*]
    }

    dimension: trial_day {
      type: number
      sql: ${TABLE}.trial_day ;;
    }

    dimension: num_months {
      type: number
      sql: ${TABLE}.num_months ;;
    }

    dimension: paid_sub_indicator {
      type: number
      sql: ${TABLE}.paid_sub_indicator ;;
    }

    dimension: platform {
      type: string
      sql: ${TABLE}.platform ;;
    }

    dimension: subscription_frequency {
      type: string
      sql: ${TABLE}.subscription_frequency ;;
    }

    dimension: quarter {
      type: string
      sql: ${TABLE}.quarter ;;
    }

    dimension: month {
      type: string
      sql: ${TABLE}.month ;;
    }

    dimension: topic {
      type: string
      sql: ${TABLE}.topic ;;
    }

    dimension: first_play {
      type: string
      sql: ${TABLE}.first_play ;;
    }

  measure: num_churned {
    type: sum
    sql: ${TABLE}.churn_status ;;
  }


    set: detail {
      fields: [
        trial_day,
        num_months,
        paid_sub_indicator,
        platform,
        subscription_frequency,
        quarter,
        month,
        topic,
        first_play
      ]
    }
  }
