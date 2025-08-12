view: free_trials {
  derived_table: {
    sql:
    with p0 as (
            select
            *
            , case when existing_free_trials is null AND existing_paying is null then 'v2'
                else 'v1'
              end as report_version
            from php.get_analytics
            where date(sent_at)=current_date
          ),
      p1 as (
          select
          analytics_timestamp as "timestamp",total_free_trials,report_version,
          row_number() over (partition by analytics_timestamp, report_version order by sent_at desc) as n
          from p0),
          vimeo as(

          select total_free_trials ,date(timestamp) as report_date from p1
          where report_version = 'v2' and n=1),

chargebee0 as (
 SELECT
          uploaded_at
          , subscription_status as status
          , row_number() over (partition by subscription_id, uploaded_at order by uploaded_at desc) as rn
          FROM http_api.chargebee_subscriptions
          WHERE subscription_subscription_items_0_item_price_id LIKE '%UP-Faith-Family%'
          ),
chargebee1 as (
          select
          *
          from chargebee0
          where rn=1
        ),
chargebee2 as (
select
uploaded_at as report_date,
count(case when (status = 'in_trial') then 1 else null end) as total_free_trials
        from chargebee1
        group by 1)

select
a.total_free_trials + b.total_free_trials as total_free_trials
,b.report_date
FROM chargebee2 a
LEFT JOIN vimeo b
ON date(a.report_date )= b.report_date;;
  }
  dimension: date {
    type: date
    primary_key: yes
    sql:  ${TABLE}.report_date ;;
  }
  dimension_group: report_date {
    type: time

    timeframes: [date, week]
    sql: ${TABLE}.report_date ;;
    convert_tz: yes  # Adjust for timezone conversion if needed
  }

  dimension: total_free_trials {
    type: number
    sql: ${TABLE}.total_free_trials ;;
  }

}
