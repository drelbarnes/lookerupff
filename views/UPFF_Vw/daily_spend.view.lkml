view: daily_spend {
    derived_table: {
      sql:
          ,cfg AS (
    SELECT report_date
    FROM ${configg.SQL_TABLE_NAME}
),

          c as (

        select id
        , name
        , received_at
        , status
        from adwords.campaigns

        ),

        g as (

        select
        groups.id as ad_group_id
        , campaign_id
        , groups.received_at
        , c.name
        , c.received_at
        , c.status
        from adwords.ad_groups as groups
        INNER JOIN c
        ON groups.campaign_id = c.id


        ),

        ad as (

        select
        g.campaign_id
        , g.name
        , ads.ad_group_id
        , g.status
        , ads.received_at
        , ads.cost
        , ads.date_start
        from adwords.ad_performance_reports as ads
        INNER JOIN g ON ads.ad_group_id = g.ad_group_id


        ),
        free_trial as (with chargebee as (
        SELECT
        content_subscription_id as user_id
        ,CASE
        WHEN content_subscription_billing_period_unit ='month' THEN 'monthly'
        ELSE 'yearly'
        END AS billing_period
        ,'web' as platform
        ,date(DATEADD(HOUR, -4, received_at)) as report_date
        FROM chargebee_webhook_events.subscription_created
        WHERE report_date >= (SELECT MAX(report_date) FROM cfg)
        AND content_subscription_subscription_items like '%UP%'
        ),

        vimeo as (
        SELECT distinct
        email
        ,date(event_occurred_at) as report_date
        ,subscription_frequency as billing_period
        FROM customers.new_customers
        WHERE event_type = 'New Free Trial' and report_date >= (SELECT MAX(report_date) FROM cfg)
        )

        SELECT
          report_date
          ,email as user_id
        from vimeo

        UNION ALL
        SELECT
          report_date
          ,user_id
        from chargebee),

        customers_analytics as (
        select
        report_date,
        count(user_id) as free_trial_created
        from free_trial
        group by 1
        ),

        fb_campaign as (
        SELECT
          a.id
          ,b.name as campaign_name
        FROM facebook_ads.ads a
        INNER JOIN facebook_ads.campaigns b
        ON a.campaign_id = b.id
        ),

        fb_data as (
        select
        a.date_start,
        a.spend,
        a.impressions,
        a.clicks,
        null::integer as installs,
        COALESCE(a.actions_1d_view_app_custom_event_fb_mobile_purchase, 0) +
        COALESCE(a.actions_1d_view_offsite_conversion_fb_pixel_purchase, 0) +
        COALESCE(a.actions_28d_click_app_custom_event_fb_mobile_purchase, 0) +
        COALESCE(a.actions_28d_click_offsite_conversion_fb_pixel_purchase, 0) AS purchases_28d
        ,b.campaign_name
        from facebook_ads.insights a
        LEFT JOIN fb_campaign b
        ON a.ad_id = b.id
        ),

        fb_perf as (
        select i.date_start,
        'Facebook' as channel,
        i.campaign_name,
        sum(i.spend) as spend,
        sum(i.impressions) as impressions,
        sum(i.clicks) as clicks,
        sum(i.installs) as installs,
        sum(i.purchases_28d) as conversions
        from fb_data i
        group by 1,2,3
        ),


        google_perf as (
        select apr.date_start,
        b.name as channel,
        b.spend as spend,
        sum(impressions) as impressions,
        sum(clicks) as clicks,
        sum(installs) as installs,
        sum(conversions) as conversions
        from (
        select  apr.date_start,
        sum((apr.cost/1000000)) as campaigncost,
        sum(impressions) as impressions,
        sum(clicks) as clicks,
        sum(null::integer) as installs,
        sum(conversions) as conversions
        from adwords.campaign_performance_reports as apr
        group by 1
        ) as apr
        inner join (
        select date_start
        , sum(COALESCE((cost/1000000),0 )) as spend
        ,name
        from ad
        group by date_start,name
        ) as b
        on apr.date_start=b.date_start
        group by apr.date_start, b.spend,b.name
        ),
        others_perf as (
        with p0 as (
        select
        other_marketing_spend_date as date_start
        ,other_marketing_spend_channel as channel
        , original_timestamp
        , other_marketing_spend_spend  as spend
        , 0 as impressions
        , 0 as clicks
        , 0 as installs
        ,0 as conversions
        , row_number() over (partition by other_marketing_spend_date , channel order by original_timestamp desc) as rn
        , count(*) as n

        FROM looker.get_channel_spend
        group by 1,2,3,4,5,6,7,8 order by 1 desc,2,3 desc,4
        )
        select
        date_start
        , channel
        , sum(spend) as spend
        , sum(impressions) as impressions
        , sum(clicks) as clicks
        , sum(installs) as installs
        , sum(conversions) as conversions
        from p0
        where rn = 1
        and date_start is not null
        group by 1,2
        order by date_start desc
        ),

        t1 as (
        -- manually adding spend to historical google adwords record
        select date_start,
        /*
        case
        when TO_CHAR(DATE_TRUNC('month', date_start), 'YYYY-MM') = '2018-07' then spend+(1440/31)
        when TO_CHAR(DATE_TRUNC('month', date_start), 'YYYY-MM') = '2018-06' then spend+(19000/30)
        when TO_CHAR(DATE_TRUNC('month', date_start ), 'YYYY-MM') = '2018-05' then spend+(10000/31)
        when TO_CHAR(DATE_TRUNC('month', date_start ), 'YYYY-MM') = '2018-04' then spend+(0/30)
        when TO_CHAR(DATE_TRUNC('month', date_start ), 'YYYY-MM') = '2018-03' then spend+(22018/31)
        when TO_CHAR(DATE_TRUNC('month', date_start ), 'YYYY-MM') = '2018-02' then spend+(21565/28)
        when TO_CHAR(DATE_TRUNC('month', date_start ), 'YYYY-MM') = '2018-01' then spend+(21570/31)
        when date(date_start) between timestamp '2018-08-11' and timestamp '2018-09-08' then spend+((288.37+87.27)/28)
        else spend
        end as */spend,
        impressions,
        clicks,
        installs,
        conversions,
        channel,
        null as campaign_name
        from google_perf
        union all
        select date_start,
        spend,
        impressions,
        clicks,
        installs,
        conversions,
        channel
        ,campaign_name
        from fb_perf
        union all
        select date_start,
        spend,
        impressions,
        clicks,
        installs,
        conversions,
        channel,
        null as campaign_name
        from others_perf

        )





        -- NOTE: we discontinued use of the spend forecasting logic.
        , outer_query as (
        select
        date_start,
        free_trial_created,
        channel,
        campaign_name,
        spend
        from t1
        inner join customers_analytics
        on date(date_start) =report_date
        group by 1,2,3,4,5

        )
        select * from outer_query   ;;

        sql_trigger_value: SELECT TO_CHAR( DATEADD(minute, -610, GETDATE()), 'YYYY-MM-DD');;
      #sql_trigger_value:  SELECT TO_CHAR(DATE_TRUNC('day', CURRENT_TIMESTAMP) + INTERVAL '9 hours 45 minutes', 'YYYY-MM-DD');;
      distribution: "date_start"
      sortkeys: ["date_start"]
    }





    dimension_group: timestamp {
      type: time
      timeframes: [raw, time, date, day_of_week, week, month]
      sql: ${TABLE}.date_start ;;
    }

    dimension: campaign_name {
      type: string
      sql: ${TABLE}.campaign_name ;;
    }

  dimension: channel {
    type: string
    sql: ${TABLE}.channel ;;
  }


    measure: spend {
      type: sum
      sql_distinct_key: ${timestamp_date} ;;
      sql: ${TABLE}.spend ;;
      value_format_name: usd
    }


    measure: free_trial_created {
      type: sum_distinct
      sql_distinct_key: ${timestamp_date} ;;
      sql: ${TABLE}.free_trial_created ;;
    }


  }
