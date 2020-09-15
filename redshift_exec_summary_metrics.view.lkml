view: redshift_exec_summary_metrics  {
  derived_table: {
    sql: -- raw sql results do not include filled-in values for 'analytics_v2.timestamp_date'
      with a AS (

      WITH analytics_v2 AS (with customers_analytics as (select analytics_timestamp as timestamp,
             existing_free_trials,
             existing_paying,
             free_trial_churn,
             free_trial_converted,
             free_trial_created,
             paused_created,
             paying_churn,
             paying_created,
             total_free_trials,
             total_paying
      from php.get_analytics
      where date(sent_at)=current_date),

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
                                     cast(paying_30_days_prior as decimal)/cast(churn_30_days as decimal) as churn_30_day_percent,
                                     cast(winback_30_days as decimal) as winback_30_days
                 from c inner join d on c.timestamp=d.timestamp),

           f as (select *, sum((49000-(total_paying))/(365-day_of_year)) OVER (PARTITION by cast(datepart(month,date(timestamp)) as varchar) order by timestamp asc
                       rows between unbounded preceding and current row) as Running_Free_Trial_Target
               from (select *, SUM(free_trial_created) OVER (PARTITION by cast(datepart(month,date(timestamp)) as varchar) order by timestamp asc rows between unbounded preceding and current row) AS Running_Free_Trials
               from (select distinct * from (select a.*,
                      case when extract(YEAR from a.timestamp)='2018' then 795+((49000-795)*(cast(datepart(dayofyear,date(a.timestamp)) as integer)-1)/365)
                           when extract(YEAR from a.timestamp)='2019' then 16680+((55000-16680)*(cast(datepart(dayofyear,date(a.timestamp)) as integer)-1)/365)
                           when extract(YEAR from a.timestamp)='2020' then 64907+((125000-64907)*(cast(datepart(dayofyear,date(a.timestamp)) as integer)-1)/365)end as target,
                      case when extract(YEAR from a.timestamp)='2018' then 3246+((49000-3246)*(cast(datepart(dayofyear,date(a.timestamp)) as integer)-1)/365)
                           when extract(YEAR from a.timestamp)='2019' then 24268+((55000-24268)*(cast(datepart(dayofyear,date(a.timestamp)) as integer)-1)/365)
                           when extract(YEAR from a.timestamp)='2020' then 70039+((125000-70039)*(cast(datepart(dayofyear,date(a.timestamp)) as integer)-1)/365) end as total_target,
                      70039+((125000-70039)*(cast(datepart(dayofyear,date(a.timestamp)) as integer)+14)/365) as target_14_days_future,
                      cast(datepart(dayofyear,date(a.timestamp)) as integer)-1 as day_of_year,
                      cast(datepart(dayofyear,date(a.timestamp)) as integer)+14 as day_of_year_14_days,
                      case when extract(YEAR from a.timestamp)='2018' then 49000
                           when extract(YEAR from a.timestamp)='2019' then 55000
                           when extract(YEAR from a.timestamp)='2020' then 125000 end  as annual_target,
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
            ((select a.*,cast(datepart(week,date(timestamp)) as varchar) as Week,
            cast(datepart(month,date(timestamp)) as varchar) as Month,
            cast(datepart(Quarter,date(timestamp)) as varchar) as Quarter,
            cast(datepart(Year,date(timestamp)) as varchar) as Year,
            new_trials_14_days_prior from
            (select *, row_number() over(order by timestamp desc) as rownum from customers_analytics) as a
            left join
            (select free_trial_created as new_trials_14_days_prior, row_number() over(order by timestamp desc) as rownum from customers_analytics
            where timestamp in
                            (select dateadd(day,-14,timestamp) as timestamp from customers_analytics )) as b on a.rownum=b.rownum)) as a
            left join customers.churn_reasons_aggregated as b on a.timestamp=b.timestamp)) as a))

            select f.*,paying_30_days_prior,churn_30_days,churn_30_day_percent,winback_30_days from e inner join f on e.timestamp=f.timestamp )
      SELECT
        DATE(analytics_v2.timestamp ) AS "ingestion_date",
        CAST(COALESCE(SUM(churn_30_days ), 0)/COALESCE(SUM(analytics_v2.paying_30_days_prior ), 0) AS VARCHAR) AS "metric",
        'churn_rate' As metric_type
      FROM analytics_v2

      GROUP BY 1),

      b AS (

      WITH analytics_v2 AS (with customers_analytics as (select analytics_timestamp as timestamp,
             existing_free_trials,
             existing_paying,
             free_trial_churn,
             free_trial_converted,
             free_trial_created,
             paused_created,
             paying_churn,
             paying_created,
             total_free_trials,
             total_paying
      from php.get_analytics
      where date(sent_at)=current_date),

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
                                     cast(paying_30_days_prior as decimal)/cast(churn_30_days as decimal) as churn_30_day_percent,
                                     cast(winback_30_days as decimal) as winback_30_days
                 from c inner join d on c.timestamp=d.timestamp),

           f as (select *, sum((49000-(total_paying))/(365-day_of_year)) OVER (PARTITION by cast(datepart(month,date(timestamp)) as varchar) order by timestamp asc
                       rows between unbounded preceding and current row) as Running_Free_Trial_Target
               from (select *, SUM(free_trial_created) OVER (PARTITION by cast(datepart(month,date(timestamp)) as varchar) order by timestamp asc rows between unbounded preceding and current row) AS Running_Free_Trials
               from (select distinct * from (select a.*,
                      case when extract(YEAR from a.timestamp)='2018' then 795+((49000-795)*(cast(datepart(dayofyear,date(a.timestamp)) as integer)-1)/365)
                           when extract(YEAR from a.timestamp)='2019' then 16680+((55000-16680)*(cast(datepart(dayofyear,date(a.timestamp)) as integer)-1)/365)
                           when extract(YEAR from a.timestamp)='2020' then 64907+((125000-64907)*(cast(datepart(dayofyear,date(a.timestamp)) as integer)-1)/365)end as target,
                      case when extract(YEAR from a.timestamp)='2018' then 3246+((49000-3246)*(cast(datepart(dayofyear,date(a.timestamp)) as integer)-1)/365)
                           when extract(YEAR from a.timestamp)='2019' then 24268+((55000-24268)*(cast(datepart(dayofyear,date(a.timestamp)) as integer)-1)/365)
                           when extract(YEAR from a.timestamp)='2020' then 70039+((125000-70039)*(cast(datepart(dayofyear,date(a.timestamp)) as integer)-1)/365) end as total_target,
                      70039+((125000-70039)*(cast(datepart(dayofyear,date(a.timestamp)) as integer)+14)/365) as target_14_days_future,
                      cast(datepart(dayofyear,date(a.timestamp)) as integer)-1 as day_of_year,
                      cast(datepart(dayofyear,date(a.timestamp)) as integer)+14 as day_of_year_14_days,
                      case when extract(YEAR from a.timestamp)='2018' then 49000
                           when extract(YEAR from a.timestamp)='2019' then 55000
                           when extract(YEAR from a.timestamp)='2020' then 125000 end  as annual_target,
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
            ((select a.*,cast(datepart(week,date(timestamp)) as varchar) as Week,
            cast(datepart(month,date(timestamp)) as varchar) as Month,
            cast(datepart(Quarter,date(timestamp)) as varchar) as Quarter,
            cast(datepart(Year,date(timestamp)) as varchar) as Year,
            new_trials_14_days_prior from
            (select *, row_number() over(order by timestamp desc) as rownum from customers_analytics) as a
            left join
            (select free_trial_created as new_trials_14_days_prior, row_number() over(order by timestamp desc) as rownum from customers_analytics
            where timestamp in
                            (select dateadd(day,-14,timestamp) as timestamp from customers_analytics )) as b on a.rownum=b.rownum)) as a
            left join customers.churn_reasons_aggregated as b on a.timestamp=b.timestamp)) as a))

            select f.*,paying_30_days_prior,churn_30_days,churn_30_day_percent,winback_30_days from e inner join f on e.timestamp=f.timestamp )
      SELECT
        DATE(analytics_v2.timestamp ) AS "ingestion_date",
        CAST(100.0*(COALESCE(SUM(analytics_v2.free_trial_converted ), 0))/(COALESCE(SUM(analytics_v2.new_trials_14_days_prior), 0)) AS VARCHAR)  AS "metric",
        'paid_conversion_rate' As metric_type
      FROM analytics_v2

      GROUP BY 1
      HAVING
        (100.0*(COALESCE(SUM(analytics_v2.free_trial_converted ), 0))/(COALESCE(SUM(analytics_v2.new_trials_14_days_prior), 0))  > 0)
      ORDER BY 1 DESC
      ),

      c as (WITH ltv_cpa AS (with customers_analytics as (select analytics_timestamp as timestamp,
             existing_free_trials,
             existing_paying,
             free_trial_churn,
             free_trial_converted,
             free_trial_created,
             paused_created,
             paying_churn,
             paying_created,
             total_free_trials,
             total_paying
      from php.get_analytics
      where date(sent_at)=current_date),

            fb_perf as (select
                      i.date_start,
                      sum(i.spend) as spend
                from  facebook_ads.insights as i
            group by  1
            ),
            google_perf as (
              select  apr.date_start,
                      sum(campaigncost) as spend
                from  (select  apr.date_start,
                      sum((apr.cost/1000000)) as campaigncost
                from  adwords.campaign_performance_reports as apr
                group by  1) as apr
                inner join
                (select date_start,
        sum(COALESCE((cost/1000000),0 )) as spend from adwords.ad_performance_reports
        group by date_Start) as b on apr.date_start=b.date_start
                group by  1
            ),
              t1 as (select date_start,
      case when TO_CHAR(DATE_TRUNC('month', date_start), 'YYYY-MM') = '2018-07' then spend+(1440/31)
           when TO_CHAR(DATE_TRUNC('month', date_start), 'YYYY-MM') = '2018-06' then spend+(19000/30)
           when TO_CHAR(DATE_TRUNC('month', date_start ), 'YYYY-MM') = '2018-05' then spend+(10000/31)
           when TO_CHAR(DATE_TRUNC('month', date_start ), 'YYYY-MM') = '2018-04' then spend+(0/30)
           when TO_CHAR(DATE_TRUNC('month', date_start ), 'YYYY-MM') = '2018-03' then spend+(22018/31)
           when TO_CHAR(DATE_TRUNC('month', date_start ), 'YYYY-MM') = '2018-02' then spend+(21565/28)
           when TO_CHAR(DATE_TRUNC('month', date_start ), 'YYYY-MM') = '2018-01' then spend+(21570/31)
           when date(date_start) between timestamp '2018-08-11' and timestamp '2018-09-08' then spend+((288.37+87.27)/28)
           when date(date_start)between timestamp'2018-10-10' and timestamp '2019-04-30' then spend+(total_paying/30)
           when date(date_start)>'2019-04-30' then spend + 657.03 + (1.5*(free_trial_converted+paying_created))
           else spend end as spend
                      from google_perf inner join customers_analytics on date(date_start)=date(timestamp)
            union all
              select  date_start,
                      spend
              from fb_perf),

             t2 as (select date_start as timestamp, sum(spend) as spend from t1 group by date_start),

             t3 as (select a1.timestamp, a1.spend+sum(coalesce(a2.spend,0)) as spend_30_days
      from t2 as a1
      left join t2 as a2 on datediff(day,a2.timestamp,a1.timestamp)<=30 and datediff(day,a2.timestamp,a1.timestamp)>0
      group by a1.timestamp,a1.spend),

      t4 as (select *,ROW_NUMBER() OVER(ORDER BY t3.timestamp desc) AS Row
      from t3
      where (t3.timestamp  < (DATEADD(day,-14, DATE_TRUNC('day',GETDATE()) )))),

      t5 as (select a1.timestamp,ROW_NUMBER() OVER(ORDER BY a1.timestamp desc) AS Row, a1.free_trial_converted+sum(coalesce(a2.free_trial_converted,0)) as conversions_30_days
      from customers_analytics as a1
      left join customers_analytics as a2 on datediff(day,a2.timestamp,a1.timestamp)<=30 and datediff(day,a2.timestamp,a1.timestamp)>0
      group by a1.timestamp,a1.free_trial_converted),

      t6 as (select t5.timestamp,
      spend_30_days, conversions_30_days,cast(spend_30_days as decimal)/cast(conversions_30_days as decimal) as CPA
      from t4 inner join t5 on t4.row=t5.row),

      t7 as (select a.*,prior_31_days_subs, case when date(a.timestamp)>'2020-08-18' then 4.1/(cast(churn_30_days as decimal)/cast(prior_31_days_subs as decimal)) else 3.69/(cast(churn_30_days as decimal)/cast(prior_31_days_subs as decimal)) end as LTV
      from
      (select a1.timestamp, a1.paying_churn+sum(coalesce(a2.paying_churn,0)) as churn_30_days
      from customers_analytics as a1
      left join customers_analytics as a2 on datediff(day,a2.timestamp,a1.timestamp)<=29 and datediff(day,a2.timestamp,a1.timestamp)>0
      group by a1.timestamp,a1.paying_churn) as a
      inner join
      (select a.timestamp,total_paying as prior_31_days_subs
      from
      (select a.timestamp, ROW_NUMBER() OVER(ORDER BY a.timestamp desc) AS Row from customers_analytics as a) as a
      inner join
      (select a.timestamp,total_paying, ROW_NUMBER() OVER(ORDER BY a.timestamp desc) AS Row from customers_analytics as a where (a.timestamp  < (DATEADD(day,-32, DATE_TRUNC('day',GETDATE()) )))) as b
      on a.row=b.row) as b
      on a.timestamp=b.timestamp),

      t8 as (select t6.timestamp, CPA, LTV, cast(LTV as decimal)/cast(CPA as decimal) as LTV_CPA_Ratio, 1.8 as LTV_CPA_Ratio_Target,  ROW_NUMBER() OVER(ORDER BY t6.timestamp desc) AS Row
      from t6 inner join t7 on t6.timestamp=t7.timestamp),

      t9 as (select a1.timestamp,
                    avg(coalesce(a2.ltv,0)) as ltv_4_week_avg,
                    avg(coalesce(a2.cpa,0)) as cpa_4_week_avg
             from t8 as a1
                  left join t8 as a2 on datediff(day,a2.timestamp,a1.timestamp)<=28 and datediff(day,a2.timestamp,a1.timestamp)>=0
             group by a1.timestamp)

      select t8.*,
             cpa_4_week_avg,
             ltv_4_week_avg,
             ltv_4_week_avg/cpa_4_week_avg as ltv_cpa_ratio_4_week_avg
      from t8 inner join t9 on t8.timestamp=t9.timestamp
      )
      SELECT
        DATE(ltv_cpa.timestamp ) AS "ingestion_date",
          CAST(ltv_cpa.CPA AS VARCHAR)  AS "metric",
        'cpa' AS metric_type
      FROM ltv_cpa

      GROUP BY 1,2,3
      ORDER BY 3 DESC),

      d AS (
      WITH ltv_cpa AS (with customers_analytics as (select analytics_timestamp as timestamp,
             existing_free_trials,
             existing_paying,
             free_trial_churn,
             free_trial_converted,
             free_trial_created,
             paused_created,
             paying_churn,
             paying_created,
             total_free_trials,
             total_paying
      from php.get_analytics
      where date(sent_at)=current_date),

            fb_perf as (select
                      i.date_start,
                      sum(i.spend) as spend
                from  facebook_ads.insights as i
            group by  1
            ),
            google_perf as (
              select  apr.date_start,
                      sum(campaigncost) as spend
                from  (select  apr.date_start,
                      sum((apr.cost/1000000)) as campaigncost
                from  adwords.campaign_performance_reports as apr
                group by  1) as apr
                inner join
                (select date_start,
        sum(COALESCE((cost/1000000),0 )) as spend from adwords.ad_performance_reports
        group by date_Start) as b on apr.date_start=b.date_start
                group by  1
            ),
              t1 as (select date_start,
      case when TO_CHAR(DATE_TRUNC('month', date_start), 'YYYY-MM') = '2018-07' then spend+(1440/31)
           when TO_CHAR(DATE_TRUNC('month', date_start), 'YYYY-MM') = '2018-06' then spend+(19000/30)
           when TO_CHAR(DATE_TRUNC('month', date_start ), 'YYYY-MM') = '2018-05' then spend+(10000/31)
           when TO_CHAR(DATE_TRUNC('month', date_start ), 'YYYY-MM') = '2018-04' then spend+(0/30)
           when TO_CHAR(DATE_TRUNC('month', date_start ), 'YYYY-MM') = '2018-03' then spend+(22018/31)
           when TO_CHAR(DATE_TRUNC('month', date_start ), 'YYYY-MM') = '2018-02' then spend+(21565/28)
           when TO_CHAR(DATE_TRUNC('month', date_start ), 'YYYY-MM') = '2018-01' then spend+(21570/31)
           when date(date_start) between timestamp '2018-08-11' and timestamp '2018-09-08' then spend+((288.37+87.27)/28)
           when date(date_start)between timestamp'2018-10-10' and timestamp '2019-04-30' then spend+(total_paying/30)
           when date(date_start)>'2019-04-30' then spend + 657.03 + (1.5*(free_trial_converted+paying_created))
           else spend end as spend
                      from google_perf inner join customers_analytics on date(date_start)=date(timestamp)
            union all
              select  date_start,
                      spend
              from fb_perf),

             t2 as (select date_start as timestamp, sum(spend) as spend from t1 group by date_start),

             t3 as (select a1.timestamp, a1.spend+sum(coalesce(a2.spend,0)) as spend_30_days
      from t2 as a1
      left join t2 as a2 on datediff(day,a2.timestamp,a1.timestamp)<=30 and datediff(day,a2.timestamp,a1.timestamp)>0
      group by a1.timestamp,a1.spend),

      t4 as (select *,ROW_NUMBER() OVER(ORDER BY t3.timestamp desc) AS Row
      from t3
      where (t3.timestamp  < (DATEADD(day,-14, DATE_TRUNC('day',GETDATE()) )))),

      t5 as (select a1.timestamp,ROW_NUMBER() OVER(ORDER BY a1.timestamp desc) AS Row, a1.free_trial_converted+sum(coalesce(a2.free_trial_converted,0)) as conversions_30_days
      from customers_analytics as a1
      left join customers_analytics as a2 on datediff(day,a2.timestamp,a1.timestamp)<=30 and datediff(day,a2.timestamp,a1.timestamp)>0
      group by a1.timestamp,a1.free_trial_converted),

      t6 as (select t5.timestamp,
      spend_30_days, conversions_30_days,cast(spend_30_days as decimal)/cast(conversions_30_days as decimal) as CPA
      from t4 inner join t5 on t4.row=t5.row),

      t7 as (select a.*,prior_31_days_subs, case when date(a.timestamp)>'2020-08-18' then 4.1/(cast(churn_30_days as decimal)/cast(prior_31_days_subs as decimal)) else 3.69/(cast(churn_30_days as decimal)/cast(prior_31_days_subs as decimal)) end as LTV
      from
      (select a1.timestamp, a1.paying_churn+sum(coalesce(a2.paying_churn,0)) as churn_30_days
      from customers_analytics as a1
      left join customers_analytics as a2 on datediff(day,a2.timestamp,a1.timestamp)<=29 and datediff(day,a2.timestamp,a1.timestamp)>0
      group by a1.timestamp,a1.paying_churn) as a
      inner join
      (select a.timestamp,total_paying as prior_31_days_subs
      from
      (select a.timestamp, ROW_NUMBER() OVER(ORDER BY a.timestamp desc) AS Row from customers_analytics as a) as a
      inner join
      (select a.timestamp,total_paying, ROW_NUMBER() OVER(ORDER BY a.timestamp desc) AS Row from customers_analytics as a where (a.timestamp  < (DATEADD(day,-32, DATE_TRUNC('day',GETDATE()) )))) as b
      on a.row=b.row) as b
      on a.timestamp=b.timestamp),

      t8 as (select t6.timestamp, CPA, LTV, cast(LTV as decimal)/cast(CPA as decimal) as LTV_CPA_Ratio, 1.8 as LTV_CPA_Ratio_Target,  ROW_NUMBER() OVER(ORDER BY t6.timestamp desc) AS Row
      from t6 inner join t7 on t6.timestamp=t7.timestamp),

      t9 as (select a1.timestamp,
                    avg(coalesce(a2.ltv,0)) as ltv_4_week_avg,
                    avg(coalesce(a2.cpa,0)) as cpa_4_week_avg
             from t8 as a1
                  left join t8 as a2 on datediff(day,a2.timestamp,a1.timestamp)<=28 and datediff(day,a2.timestamp,a1.timestamp)>=0
             group by a1.timestamp)

      select t8.*,
             cpa_4_week_avg,
             ltv_4_week_avg,
             ltv_4_week_avg/cpa_4_week_avg as ltv_cpa_ratio_4_week_avg
      from t8 inner join t9 on t8.timestamp=t9.timestamp
      )
      SELECT
        DATE(ltv_cpa.timestamp ) AS "ingestion_date",
        CAST(ltv_cpa.LTV AS VARCHAR)  AS "metric",
        'ltv' AS metric_type
      FROM ltv_cpa

      GROUP BY 1,2
      ORDER BY 1 DESC
      ),

      e AS (
      WITH analytics_v2 AS (with customers_analytics as (select analytics_timestamp as timestamp,
             existing_free_trials,
             existing_paying,
             free_trial_churn,
             free_trial_converted,
             free_trial_created,
             paused_created,
             paying_churn,
             paying_created,
             total_free_trials,
             total_paying
      from php.get_analytics
      where date(sent_at)=current_date),

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
                                     cast(paying_30_days_prior as decimal)/cast(churn_30_days as decimal) as churn_30_day_percent,
                                     cast(winback_30_days as decimal) as winback_30_days
                 from c inner join d on c.timestamp=d.timestamp),

           f as (select *, sum((49000-(total_paying))/(365-day_of_year)) OVER (PARTITION by cast(datepart(month,date(timestamp)) as varchar) order by timestamp asc
                       rows between unbounded preceding and current row) as Running_Free_Trial_Target
               from (select *, SUM(free_trial_created) OVER (PARTITION by cast(datepart(month,date(timestamp)) as varchar) order by timestamp asc rows between unbounded preceding and current row) AS Running_Free_Trials
               from (select distinct * from (select a.*,
                      case when extract(YEAR from a.timestamp)='2018' then 795+((49000-795)*(cast(datepart(dayofyear,date(a.timestamp)) as integer)-1)/365)
                           when extract(YEAR from a.timestamp)='2019' then 16680+((55000-16680)*(cast(datepart(dayofyear,date(a.timestamp)) as integer)-1)/365)
                           when extract(YEAR from a.timestamp)='2020' then 64907+((125000-64907)*(cast(datepart(dayofyear,date(a.timestamp)) as integer)-1)/365)end as target,
                      case when extract(YEAR from a.timestamp)='2018' then 3246+((49000-3246)*(cast(datepart(dayofyear,date(a.timestamp)) as integer)-1)/365)
                           when extract(YEAR from a.timestamp)='2019' then 24268+((55000-24268)*(cast(datepart(dayofyear,date(a.timestamp)) as integer)-1)/365)
                           when extract(YEAR from a.timestamp)='2020' then 70039+((125000-70039)*(cast(datepart(dayofyear,date(a.timestamp)) as integer)-1)/365) end as total_target,
                      70039+((125000-70039)*(cast(datepart(dayofyear,date(a.timestamp)) as integer)+14)/365) as target_14_days_future,
                      cast(datepart(dayofyear,date(a.timestamp)) as integer)-1 as day_of_year,
                      cast(datepart(dayofyear,date(a.timestamp)) as integer)+14 as day_of_year_14_days,
                      case when extract(YEAR from a.timestamp)='2018' then 49000
                           when extract(YEAR from a.timestamp)='2019' then 55000
                           when extract(YEAR from a.timestamp)='2020' then 125000 end  as annual_target,
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
            ((select a.*,cast(datepart(week,date(timestamp)) as varchar) as Week,
            cast(datepart(month,date(timestamp)) as varchar) as Month,
            cast(datepart(Quarter,date(timestamp)) as varchar) as Quarter,
            cast(datepart(Year,date(timestamp)) as varchar) as Year,
            new_trials_14_days_prior from
            (select *, row_number() over(order by timestamp desc) as rownum from customers_analytics) as a
            left join
            (select free_trial_created as new_trials_14_days_prior, row_number() over(order by timestamp desc) as rownum from customers_analytics
            where timestamp in
                            (select dateadd(day,-14,timestamp) as timestamp from customers_analytics )) as b on a.rownum=b.rownum)) as a
            left join customers.churn_reasons_aggregated as b on a.timestamp=b.timestamp)) as a))

            select f.*,paying_30_days_prior,churn_30_days,churn_30_day_percent,winback_30_days from e inner join f on e.timestamp=f.timestamp )
      SELECT
        DATE(analytics_v2.timestamp ) AS "ingestion_date",
        CAST(analytics_v2.existing_free_trials AS VARCHAR)  AS "metric",
        'free_trial_subs' as metric_type
      FROM analytics_v2

      GROUP BY 1,2
      ORDER BY 1 DESC
      ),

      f as (
      WITH analytics_v2 AS (with customers_analytics as (select analytics_timestamp as timestamp,
             existing_free_trials,
             existing_paying,
             free_trial_churn,
             free_trial_converted,
             free_trial_created,
             paused_created,
             paying_churn,
             paying_created,
             total_free_trials,
             total_paying
      from php.get_analytics
      where date(sent_at)=current_date),

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
                                     cast(paying_30_days_prior as decimal)/cast(churn_30_days as decimal) as churn_30_day_percent,
                                     cast(winback_30_days as decimal) as winback_30_days
                 from c inner join d on c.timestamp=d.timestamp),

           f as (select *, sum((49000-(total_paying))/(365-day_of_year)) OVER (PARTITION by cast(datepart(month,date(timestamp)) as varchar) order by timestamp asc
                       rows between unbounded preceding and current row) as Running_Free_Trial_Target
               from (select *, SUM(free_trial_created) OVER (PARTITION by cast(datepart(month,date(timestamp)) as varchar) order by timestamp asc rows between unbounded preceding and current row) AS Running_Free_Trials
               from (select distinct * from (select a.*,
                      case when extract(YEAR from a.timestamp)='2018' then 795+((49000-795)*(cast(datepart(dayofyear,date(a.timestamp)) as integer)-1)/365)
                           when extract(YEAR from a.timestamp)='2019' then 16680+((55000-16680)*(cast(datepart(dayofyear,date(a.timestamp)) as integer)-1)/365)
                           when extract(YEAR from a.timestamp)='2020' then 64907+((125000-64907)*(cast(datepart(dayofyear,date(a.timestamp)) as integer)-1)/365)end as target,
                      case when extract(YEAR from a.timestamp)='2018' then 3246+((49000-3246)*(cast(datepart(dayofyear,date(a.timestamp)) as integer)-1)/365)
                           when extract(YEAR from a.timestamp)='2019' then 24268+((55000-24268)*(cast(datepart(dayofyear,date(a.timestamp)) as integer)-1)/365)
                           when extract(YEAR from a.timestamp)='2020' then 70039+((125000-70039)*(cast(datepart(dayofyear,date(a.timestamp)) as integer)-1)/365) end as total_target,
                      70039+((125000-70039)*(cast(datepart(dayofyear,date(a.timestamp)) as integer)+14)/365) as target_14_days_future,
                      cast(datepart(dayofyear,date(a.timestamp)) as integer)-1 as day_of_year,
                      cast(datepart(dayofyear,date(a.timestamp)) as integer)+14 as day_of_year_14_days,
                      case when extract(YEAR from a.timestamp)='2018' then 49000
                           when extract(YEAR from a.timestamp)='2019' then 55000
                           when extract(YEAR from a.timestamp)='2020' then 125000 end  as annual_target,
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
            ((select a.*,cast(datepart(week,date(timestamp)) as varchar) as Week,
            cast(datepart(month,date(timestamp)) as varchar) as Month,
            cast(datepart(Quarter,date(timestamp)) as varchar) as Quarter,
            cast(datepart(Year,date(timestamp)) as varchar) as Year,
            new_trials_14_days_prior from
            (select *, row_number() over(order by timestamp desc) as rownum from customers_analytics) as a
            left join
            (select free_trial_created as new_trials_14_days_prior, row_number() over(order by timestamp desc) as rownum from customers_analytics
            where timestamp in
                            (select dateadd(day,-14,timestamp) as timestamp from customers_analytics )) as b on a.rownum=b.rownum)) as a
            left join customers.churn_reasons_aggregated as b on a.timestamp=b.timestamp)) as a))

            select f.*,paying_30_days_prior,churn_30_days,churn_30_day_percent,winback_30_days from e inner join f on e.timestamp=f.timestamp )
      SELECT
        DATE(analytics_v2.timestamp ) AS "ingestion_date",
        CAST(COALESCE(SUM(CASE WHEN 1=1 -- no filter on 'analytics_v2.time_a'
             THEN analytics_v2.total_paying ELSE NULL END), 0) AS VARCHAR) AS "metric",
             'paid_subs' AS metric_type
      FROM analytics_v2

      GROUP BY 1
      ORDER BY 1 DESC
      )

      select * from a

      UNION ALL

      select * from b

      UNION ALL

      select * from c

      UNION ALL

      select * from d

      UNION ALL

      select * from e

      UNION ALL

      select * from f
       ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: user_id {
    type: number
    tags: ["user_id"]
    sql: 1;;
  }

  dimension: ingestion_date {
    type: date
    sql: ${TABLE}.ingestion_date ;;
  }

  dimension: metric {
    type: string
    sql: cast(${TABLE}.metric as VARCHAR ) ;;
  }

  dimension: metric_type {
    type: string
    sql: ${TABLE}.metric_type ;;
  }

  set: detail {
    fields: [ingestion_date, metric, metric_type]
  }
}
