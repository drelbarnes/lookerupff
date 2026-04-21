view: marketing_attribution {
  derived_table: {
    sql:
    with marketing_page as(
      SELECT
        date(received_at) as report_date
        ,context_ip
        ,anonymous_id
        ,context_campaign_source as campaign_source
        ,context_campaign_name as campaign_name
        ,context_campaign_medium as campaign_medium
        ,context_campaign_content as campaign_content
      FROM javascript_upff_home.pages
      WHERE 1=1
        --and context_campaign_name LIKE '%blue_skies%'
        AND DATE(received_at) between TIMESTAMP '2026-01-01' and '2026-03-30'
    ),

  trial_created as (
  SELECT
  distinct
    user_id
    ,context_ip
    ,anonymous_id
    ,date(timestamp) as report_date
  FROM javaScript_upentertainment_checkout.order_completed
  WHERE brand = 'upfaithandfamily'

    AND report_date between TIMESTAMP '2026-01-01' and '2026-03-30'

  ),

  reacquisition as (

  FROM javascript_upentertainment_checkout.order_resubscribed
  ),

  converted as (
  SELECT
    date(received_at) as report_date
    ,user_id
  FROM chargebee_webhook_events.subscription_activated
  WHERE content_subscription_subscription_items like '%UP%'

    AND DATE(received_at) between TIMESTAMP '2026-01-01' and '2026-03-30'

  ),

  not_converted as (
  SELECT
      user_id
      ,DATE("timestamp") AS report_date
      FROM chargebee_webhook_events.subscription_cancelled
      WHERE (content_subscription_cancelled_at - content_subscription_trial_end) < 10000
      AND content_subscription_subscription_items LIKE '%UP%'

    AND DATE(timestamp) between TIMESTAMP '2026-01-01' and '2026-03-30'

  ),

  join_data as (
  SELECT
  distinct
    a.user_id
    ,a.context_ip
    ,a.anonymous_id
    ,a.report_date as trial_start_date
    ,CASE
      WHEN b.report_date is NOT NULL THEN 'Yes'
      WHEN c.report_date is NOT NULL THEN 'No'
      ELSE 'in trial'
    END AS has_converted
    FROM trial_created a
    LEFT JOIN converted b
    ON a.user_id = b.user_id
    LEFT JOIN not_converted c
    ON a.user_id = c.user_id
  ),

result as (
  SELECT
    jd.*
    ,mp.campaign_source
    ,mp.campaign_name
    ,mp.report_date as click_date
    ,campaign_medium
    ,campaign_content
  FROM join_data jd
  LEFT JOIN marketing_page mp
  ON mp.context_ip = jd.context_ip or mp.anonymous_id = jd.anonymous_id
  and mp.report_date <= jd.trial_start_date
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY jd.user_id, jd.trial_start_date
    ORDER BY
      CASE WHEN mp.campaign_name IS NULL THEN 1 ELSE 0 END,
      mp.report_date DESC
  ) = 1
  )

  SELECT
    user_id
    ,context_ip
    ,anonymous_id
    ,campaign_source
    ,campaign_name
    ,campaign_medium
    ,campaign_content
    ,trial_start_date
  FROM result;;





  }
  }
