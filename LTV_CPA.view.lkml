view: ltv_cpa{
  derived_table: {
    sql: with customers_analytics as (
        select
        "timestamp",
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
        from ${analytics_v2.SQL_TABLE_NAME}
      ),

      /*Aggregate spend by date*/
      t2 as (
        select date_start as timestamp,
        spend
        from ${daily_spend.SQL_TABLE_NAME}
        group by 1,2
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
