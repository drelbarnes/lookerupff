view: ltv_cpa{
  derived_table: {
    sql: with get_analytics_p0 as (
        select
        analytics_timestamp as timestamp
        , existing_free_trials
        , existing_paying
        , free_trial_churn
        , free_trial_converted
        , free_trial_created
        , paused_created
        , paying_churn
        , paying_created
        , total_free_trials
        , total_paying
        , row_number() over (partition by analytics_timestamp order by sent_at) as n
        from php.get_analytics
        where date(sent_at)=current_date
      )
      , apple_subs as (
        select
        report_date
        , app_store_connect_subscribers_total
        , vimeo_ott_subscribers_total
        from
        looker.get_app_store_connect_subs
        where date(sent_at)=current_date
      )
      , get_analytics_p1 as (
        select
        a.timestamp
        , a.existing_free_trials
        , a.existing_paying
        , a.free_trial_churn
        , a.free_trial_converted
        , a.free_trial_created
        , a.paused_created
        , a.paying_churn
        , a.paying_created
        , b.total_free_trials
        -- , b.total_paying
        , (b.total_paying-c.vimeo_ott_subscribers_total+c.app_store_connect_subscribers_total) as total_paying
        from (select * from get_analytics_p0 where n=1) as a
        inner join (select * from get_analytics_p0 where n=2) as b
        on date(a.timestamp) = date(b.timestamp)
        inner join (
          select
          report_date
          , app_store_connect_subscribers_total
          , vimeo_ott_subscribers_total
          from
          looker.get_app_store_connect_subs
          where date(sent_at)=current_date
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

      apple_perf as (
        select start_date as date_start,
        sum(total_local_spend_amount) as spend
        from php.get_apple_search_ads_campaigns
        group by 1
      ),

      /*Pull FB and Google Spend*/
      fb_perf as (
        select i.date_start,
        sum(i.spend) as spend
        from facebook_ads.insights as i
        group by 1
      ),

      google_perf as (
        select apr.date_start,
        sum(campaigncost) as spend
        from (
          select apr.date_start,
          sum((apr.cost/1000000)) as campaigncost
          from adwords.campaign_performance_reports as apr
          group by 1
        ) as apr
        inner join (
          select date_start,
          sum(COALESCE((cost/1000000),0 )) as spend
          from adwords.ad_performance_reports
          group by date_start
        ) as b
        on apr.date_start=b.date_start
        group by 1
      ),

      /* Adding other marketing spend provided by Ribbow */
      others_perf as (
        with p0 as (
          select date_start
          , channel
          , original_timestamp
          , spend
          , row_number() over (partition by date_start, channel order by original_timestamp desc) as rn
          , count(*) as n
          FROM looker.get_other_marketing_spend
          group by 1,2,3,4 order by 1 desc,2,3 desc,4
        )
        select
        date_start
        , sum(spend) as spend
        from p0
        where rn = 1
        and date_start is not null
        group by 1
        order by date_start desc
      ),

      /*Input manual spend for earlier dates*/
      t1 as (
        select date_start,
        case
          when TO_CHAR(DATE_TRUNC('month', date_start), 'YYYY-MM') = '2018-07' then spend+(1440/31)
          when TO_CHAR(DATE_TRUNC('month', date_start), 'YYYY-MM') = '2018-06' then spend+(19000/30)
          when TO_CHAR(DATE_TRUNC('month', date_start ), 'YYYY-MM') = '2018-05' then spend+(10000/31)
          when TO_CHAR(DATE_TRUNC('month', date_start ), 'YYYY-MM') = '2018-04' then spend+(0/30)
          when TO_CHAR(DATE_TRUNC('month', date_start ), 'YYYY-MM') = '2018-03' then spend+(22018/31)
          when TO_CHAR(DATE_TRUNC('month', date_start ), 'YYYY-MM') = '2018-02' then spend+(21565/28)
          when TO_CHAR(DATE_TRUNC('month', date_start ), 'YYYY-MM') = '2018-01' then spend+(21570/31)
          when date(date_start) between timestamp '2018-08-11' and timestamp '2018-09-08' then spend+((288.37+87.27)/28)
          when date(date_start)between timestamp '2018-10-10' and timestamp '2019-04-30' then spend+(total_paying/30)
          when date(date_start)>'2019-04-30' then spend + 657.03 + (1.5*(free_trial_converted+paying_created))
          else spend
        end as spend
        from google_perf
        inner join
        customers_analytics
        on date(google_perf.date_start)=date(customers_analytics.timestamp)
        union all
        select date_start,
        spend
        from fb_perf
        union all
        select date_start,
        spend
        from apple_perf
        union all
        select date_start,
        spend
        from others_perf
      ),

      /*Aggregate spend by date*/
      t2 as (
        select date_start as timestamp,
        sum(spend) as spend
        from t1 group by date_start
      ),

      /*Create rolling 30 day spend*/
      t3 as (
        select a1.timestamp,
        a1.spend+sum(coalesce(a2.spend,0)) as spend_30_days
        from t2 as a1
        left join t2 as a2
        on datediff(day,a2.timestamp,a1.timestamp)<=30 and datediff(day,a2.timestamp,a1.timestamp)>0
        group by a1.timestamp,a1.spend
      ),

      /* add row numbers with a 14 day offset for CPA calculation */
      t4 as (
        select *,
        ROW_NUMBER() OVER(ORDER BY t3.timestamp desc) AS Row
        from t3
        where (t3.timestamp  < (DATEADD(day,-14, DATE_TRUNC('day',GETDATE()) )))
      ),

      /* trial to paid conversions over last 30 days */
      t5 as (
        select a1.timestamp,
        ROW_NUMBER() OVER(ORDER BY a1.timestamp desc) AS Row,
        a1.free_trial_converted+sum(coalesce(a2.free_trial_converted,0)) as conversions_30_days
        from customers_analytics as a1
        left join customers_analytics as a2
        on datediff(day,a2.timestamp,a1.timestamp)<=30 and datediff(day,a2.timestamp,a1.timestamp)>0
        group by a1.timestamp,a1.free_trial_converted
      ),

      /*Calculate CPA as rolling 30 day spend divided by trial to paid conversions over last 30 days*/
      t6 as (
        select t5.timestamp,
        spend_30_days,
        conversions_30_days,
        cast(spend_30_days as decimal)/cast(conversions_30_days as decimal) as CPA
        from t4 inner join t5 on t4.row=t5.row
      ),

      /*Calculate LTV as ration between gross margin and churn %. Be sure to update manually as new gross margin arises*/
      t7 as (
        select a.*,
        prior_31_days_subs,
        case
          when date(a.timestamp)>'2020-08-18' then 4.1/(cast(churn_30_days as decimal)/cast(prior_31_days_subs as decimal))
          else 3.69/(cast(churn_30_days as decimal)/cast(prior_31_days_subs as decimal))
        end as LTV
        from (
          select a1.timestamp,
          a1.paying_churn+sum(coalesce(a2.paying_churn,0)) as churn_30_days
          from customers_analytics as a1
          left join customers_analytics as a2
          on datediff(day,a2.timestamp,a1.timestamp)<=29 and datediff(day,a2.timestamp,a1.timestamp)>0
          group by a1.timestamp,a1.paying_churn
        ) as a
        inner join (
          select a.timestamp,total_paying as prior_31_days_subs
          from (
            select a.timestamp
            , ROW_NUMBER() OVER(ORDER BY a.timestamp desc) AS Row
            from customers_analytics as a
          ) as a
          inner join (
            select a.timestamp,
            total_paying,
            ROW_NUMBER() OVER(ORDER BY a.timestamp desc) AS Row
            from customers_analytics as a
            where (a.timestamp  < (DATEADD(day,-32, DATE_TRUNC('day',GETDATE()) )))
          ) as b
          on a.row=b.row
        ) as b
        on a.timestamp=b.timestamp
      ),

      /*Manually update the LTV/CPA ratio target*/
      t8 as (
        select t6.timestamp,
        CPA,
        LTV,
        cast(LTV as decimal)/cast(CPA as decimal) as LTV_CPA_Ratio,
        2.3 as LTV_CPA_Ratio_Target,
        ROW_NUMBER() OVER(ORDER BY t6.timestamp desc) AS Row
        from t6
        inner join t7
        on t6.timestamp=t7.timestamp
      ),

      t9 as (
        select a1.timestamp,
        avg(coalesce(a2.ltv,0)) as ltv_4_week_avg,
        avg(coalesce(a2.cpa,0)) as cpa_4_week_avg
        from t8 as a1
        left join t8 as a2
        on datediff(day,a2.timestamp,a1.timestamp)<=28 and datediff(day,a2.timestamp,a1.timestamp)>=0
        group by a1.timestamp
      )

      , outer_query as (
        select t8.*,
        cpa_4_week_avg,
        ltv_4_week_avg,
        ltv_4_week_avg/cpa_4_week_avg as ltv_cpa_ratio_4_week_avg
        from t8
        inner join t9
        on t8.timestamp=t9.timestamp
      )
      select * from outer_query
      ;;
    datagroup_trigger: upff_acquisition_reporting
    distribution_style: all
  }

  measure: target_ratio {
  type: sum
  sql: 2.3 ;;
  value_format_name: percent_0
  }

  dimension_group: timestamp {
    type: time
    timeframes: [
      raw,
      time,
      date,
      week,
      month,
      quarter,
      year
    ]
    sql: ${TABLE}.timestamp ;;}

  dimension: CPA {
    type: number
    sql: ${TABLE}.CPA ;;
    value_format_name: usd
  }


  dimension: user_id{
    type: number
    tags: ["user_id"]
    sql:1;;
  }

  dimension: CPA_4_week_avg{
    type: number
    sql: ${TABLE}.CPA_4_week_avg ;;
    value_format_name: usd

  }

  dimension: CPA_4_week_avg_diff {
    type: number
    sql: ${CPA}-${CPA_4_week_avg} ;;
    value_format_name: usd
  }

          measure: CPA_ {
            type: sum
            sql: ${CPA} ;;
            value_format_name: usd
            }

  measure: CPA_4_week_avg_ {
    type: sum
    sql: ${CPA_4_week_avg} ;;
    value_format_name: usd
  }

          dimension: LTV {
            type: number
            sql: ${TABLE}.LTV ;;
            value_format_name: usd
          }

  dimension: LTV_4_week_avg {
    type: number
    sql: ${TABLE}.LTV_4_week_avg ;;
    value_format_name: usd
  }

  dimension: LTV_4_week_avg_diff {
    type: number
    sql: ${LTV}-${LTV_4_week_avg} ;;
    value_format_name: usd
  }

          measure: LTV_ {
            type: sum
            sql: ${LTV} ;;
            value_format_name: usd
          }

  measure: LTV_4_week_avg_ {
    type: sum
    sql: ${LTV_4_week_avg} ;;
    value_format_name: usd
  }

          measure: LTV_CPA_Ratio {
            type: sum
            sql: ${LTV}/${CPA} ;;
            value_format_name: percent_0
          }

  measure: LTV_CPA_Ratio_4_Week_Avg {
    type: sum
    sql: ${LTV_4_week_avg}/${CPA_4_week_avg} ;;
    value_format_name: percent_0
  }

          measure: LTV_CPA_Ratio_Target {
            type: number
            sql: 1.4 ;;
            value_format_name: percent_0
          }

          dimension: Goal{
            type: number
            sql: ${TABLE}.Goal ;;
          }

          dimension: LTV_Goal {
            type: number
            sql: (${CPA}*2.3)-${LTV};;
            value_format_name: usd
          }

          dimension: CPA_Goal{
            type: number
            sql: (${LTV}/2.3)-${CPA} ;;
            value_format_name: usd
          }
          }
