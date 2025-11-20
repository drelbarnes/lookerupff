view: UPFF_analytics_Vw_v2 {
  derived_table: {
    sql:
     , cfg AS (  -- renamed from "cfg"
  SELECT report_date
  FROM ${configg.SQL_TABLE_NAME}
),

      chargebee_subscriptions as (
      select * from http_api.chargebee_subscriptions where  CAST(uploaded_at AS DATE) >= (
      SELECT MAX(report_date)
      FROM cfg)),

      vimeo_subscriptions as(
      -- select * from customers.all_customers where report_date = TO_CHAR(CURRENT_DATE, 'YYYY-MM-DD') --
      select * from customers.all_customers where report_date >= (SELECT max(report_date) FROM cfg)),

      ------  Chargebee ------
      -- get daily status of each user
      chargebee_raw as(
      SELECT
      date(uploaded_at)::DATE as report_date
      ,subscription_id as user_id
      ,Case
      WHEN subscription_status = 'non_renewing' THEN 'active'
      ELSE subscription_status
      END AS status
      ,'Chargebee' as platform
      ,CASE
      WHEN subscription_billing_period_unit = 'month' THEN 'monthly'
      ELSE 'yearly'
      END AS billing_period
      ,ROW_NUMBER() OVER (PARTITION BY subscription_id, uploaded_at ORDER BY uploaded_at DESC) AS rn
      FROM chargebee_subscriptions
      WHERE subscription_subscription_items_0_item_price_id LIKE '%UP%'

      ),
      chargebee_subs as(
      select
      *
      from chargebee_raw
      where rn=1.  -- select the report with most recent date for each day
      ),




      -- unmark subs where it was marked as cancelled, but its actually trial not converted

      ------ Vimeo OTT ------


      vimeo_raw as (
      select
      CAST(user_id AS VARCHAR(255))
      ,CASE
        WHEN status = 'free_trial' THEN 'in_trial'
        WHEN status = 'expired' THEN 'cancelled'
        WHEN status = 'enabled' THEN 'active'
        ELSE status
      END AS status
      ,platform
      ,CASE
        WHEN frequency = 'custom' THEN 'monthly'
        ELSE frequency
      END as billing_period
      ,report_date::DATE as report_date
      from vimeo_subscriptions
      where action = 'subscription' and platform not in('api','web')
      ),


      final_join as (
      select
      report_date
      ,user_id
      ,status
      ,platform
      ,billing_period
      from chargebee_subs
      UNION ALL
      select
      report_date
      ,user_id
      ,status
      ,platform
      ,billing_period
      from vimeo_raw )
      select *
      from final_join
      ;;


    # Option 1: Time-based rebuild
    #persist_for: "2 hours"

    # Option 2 (Redshift-friendly): Rebuild based on table update timestamp
    sql_trigger_value: SELECT TO_CHAR(DATEADD(minute, -555, GETDATE()), 'YYYY-MM-DD');;
    #sql_trigger_value:  SELECT TO_CHAR(DATE_TRUNC('day', CURRENT_TIMESTAMP) + INTERVAL '9 hours 45 minutes', 'YYYY-MM-DD');;
    distribution: "user_id"
    sortkeys: ["user_id"]

  }


  dimension: date {
    type: date
    sql:  ${TABLE}.report_date ;;
  }
  dimension_group: report_date {
    type: time
    timeframes: [date, week]
    sql: ${TABLE}.report_date ;;
    convert_tz: yes  # Adjust for timezone conversion if needed
  }

  dimension: billing_period {
    type: string
    sql: ${TABLE}.billing_period ;;
  }


  dimension: user_id {
    type: string
    sql:  ${TABLE}.user_id ;;
  }

  dimension: status {
    type:  string
    sql: ${TABLE}.status ;;
  }

  dimension: platform {
    type: string
    sql: ${TABLE}.platform ;;
  }

  measure: total_paying {
    type: count_distinct
    # for Chargebee : active,non_rewing
    # for Vimeo : enabled
    filters: [status: "active,non_renewing,enabled"]
    sql:${TABLE}.user_id   ;;
  }




}
