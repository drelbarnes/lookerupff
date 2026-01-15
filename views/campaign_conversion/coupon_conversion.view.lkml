view: coupon_conversion {
  derived_table: {
    sql:
  WITH marketing_page AS (
  SELECT
    DATE(received_at) AS report_date,
    context_ip,
    CASE
      WHEN context_campaign_source LIKE '%fb%' THEN 'Facebook'
      ELSE context_campaign_source
    END AS campaign_source,
    context_campaign_name AS campaign_name,
    context_campaign_medium AS campaign_medium
  FROM javascript_upff_home.pages
  WHERE 1=1
    {% if start_date._parameter_value != "NULL" %}
    AND DATE(received_at) >= {% parameter start_date %}
    {% endif %}

    {% if end_date._parameter_value != "NULL" %}
    AND DATE(received_at) <= {% parameter end_date %}
    {% endif %}
),

trial_created0 AS (
  SELECT DISTINCT
    user_id,
    context_ip,
    DATE("timestamp") AS report_date
  FROM javascript_upentertainment_checkout.order_completed
  WHERE brand = 'upfaithandfamily'
   {% if start_date._parameter_value != "NULL" %}
    AND DATE(received_at) >= {% parameter start_date %}
    {% endif %}
),

trial_created AS (
  SELECT
    a.user_id,
    a.context_ip,
    a.report_date,
    b.content_subscription_coupon
  FROM trial_created0 a
  LEFT JOIN chargebee_webhook_events.subscription_created b
    ON a.user_id = b.content_customer_id
  -- removed: WHERE content_subscription_coupon = 'COZY2025'
),

converted AS (
  SELECT DISTINCT
    DATE(received_at) AS report_date,
    user_id
  FROM chargebee_webhook_events.subscription_activated
  WHERE content_subscription_subscription_items LIKE '%UP%'
),

not_converted AS (
  SELECT DISTINCT
    user_id,
    DATE("timestamp") AS report_date
  FROM chargebee_webhook_events.subscription_cancelled
  WHERE (content_subscription_cancelled_at - content_subscription_trial_end) < 10000
    AND content_subscription_subscription_items LIKE '%UP%'
),

join_data AS (
  SELECT
    a.user_id,
    a.context_ip,
    a.report_date AS trial_start_date,
    a.content_subscription_coupon,
    CASE
      WHEN b.report_date IS NOT NULL THEN 'Yes'
      WHEN c.report_date IS NOT NULL THEN 'No'
      ELSE 'in trial'
    END AS has_converted
  FROM trial_created a
  LEFT JOIN converted b
    ON a.user_id = b.user_id
  LEFT JOIN not_converted c
    ON a.user_id = c.user_id
  -- removed: WHERE content_subscription_coupon = 'COZY2025'
),

result AS (
  SELECT
    jd.*,
    mp.campaign_source,
    mp.campaign_name,
    mp.report_date AS click_date,
    mp.campaign_medium
  FROM join_data jd
  LEFT JOIN marketing_page mp
    ON mp.context_ip = jd.context_ip
   AND mp.report_date <= jd.trial_start_date
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY jd.context_ip, jd.user_id, jd.trial_start_date, jd.content_subscription_coupon
    ORDER BY
      CASE WHEN mp.campaign_name IS NULL THEN 1 ELSE 0 END,
      mp.report_date DESC
  ) = 1
),

visit_count AS (
  SELECT
    COUNT(DISTINCT context_ip) AS visit_count,
    campaign_name,
    campaign_source,
    campaign_medium
  FROM marketing_page
  GROUP BY 2,3,4
),

trial_count AS (
  SELECT
    COUNT(DISTINCT user_id) AS trial_started_count,
    content_subscription_coupon,
    campaign_source,
    campaign_name,
    campaign_medium
  FROM result
  GROUP BY 2,3,4,5
),

in_trial_count AS (
  SELECT
    COUNT(DISTINCT user_id) AS in_trial_count,
    content_subscription_coupon,
    campaign_source,
    campaign_name,
    campaign_medium
  FROM result
  WHERE has_converted = 'in trial'
  GROUP BY 2,3,4,5
),

converted_count AS (
  SELECT
    COUNT(DISTINCT user_id) AS converted_count,
    content_subscription_coupon,
    campaign_source,
    campaign_name,
    campaign_medium
  FROM result
  WHERE has_converted = 'Yes'
  GROUP BY 2,3,4,5
),

resubscribed_count AS (
  SELECT
    COUNT(DISTINCT user_id) AS resubscribed_count,
    content_subscription_coupon,
    campaign_source,
    campaign_name,
    campaign_medium
  FROM result
  WHERE has_converted = 'Resubscribed'
  GROUP BY 2,3,4,5
),

result2 AS (
  SELECT
    vc.visit_count,
    tc.trial_started_count,
    itc.in_trial_count,
    cc.converted_count,
    rc.resubscribed_count,
    tc.content_subscription_coupon,         -- coupon dimension comes from trial side
    vc.campaign_source,
    vc.campaign_name,
    vc.campaign_medium
  FROM visit_count vc
  LEFT JOIN trial_count tc
    ON vc.campaign_source = tc.campaign_source
   AND vc.campaign_name   = tc.campaign_name
   AND COALESCE(vc.campaign_medium,'(none)') = COALESCE(tc.campaign_medium,'(none)')

  LEFT JOIN in_trial_count itc
    ON tc.content_subscription_coupon = itc.content_subscription_coupon
   AND vc.campaign_source = itc.campaign_source
   AND vc.campaign_name   = itc.campaign_name
   AND COALESCE(vc.campaign_medium,'(none)') = COALESCE(itc.campaign_medium,'(none)')

  LEFT JOIN converted_count cc
    ON tc.content_subscription_coupon = cc.content_subscription_coupon
   AND vc.campaign_source = cc.campaign_source
   AND vc.campaign_name   = cc.campaign_name
   AND COALESCE(vc.campaign_medium,'(none)') = COALESCE(cc.campaign_medium,'(none)')

  LEFT JOIN resubscribed_count rc
    ON tc.content_subscription_coupon = rc.content_subscription_coupon
   AND vc.campaign_source = rc.campaign_source
   AND vc.campaign_name   = rc.campaign_name
   AND COALESCE(vc.campaign_medium,'(none)') = COALESCE(rc.campaign_medium,'(none)') -- FIXED (you had rc=rc)
)

SELECT
  *,
  CASE
    WHEN campaign_name LIKE '%Instant%Nanny%' THEN 'Instant Nanny'
    WHEN campaign_name LIKE '%Hudson%Rex%' OR campaign_name LIKE '%hudson%rex%' THEN 'Hudson and Rex'
    ELSE campaign_name
  END AS campaign_name_grouped
FROM result2

  ;;
  }

  parameter: start_date {
    type: date
    default_value: "30 days ago"
  }

  parameter: end_date {
    type: date
  }

  dimension: campaign_source {
    type: string
    sql: ${TABLE}.campaign_source ;;
  }

  dimension: campaign_name {
    type: string
    sql: ${TABLE}.campaign_name ;;
  }

  dimension: campaign_name_grouped {
    type: string
    sql: ${TABLE}.campaign_name_grouped ;;
  }


  dimension: campaign_medium {
    type: string
    sql: ${TABLE}.campaign_medium ;;
  }

  dimension: content_subscription_coupon {
    type: string
    label: "coupon"
    sql: ${TABLE}.content_subscription_coupon ;;
  }

  dimension: trial_started_count {
    type: number
    sql: ${TABLE}.trial_started_count ;;
  }

  dimension: in_trial_count {
    type: number
    sql: ${TABLE}.in_trial_count ;;
  }

  dimension: converted_count {
    type: number
    sql: ${TABLE}.converted_count ;;
  }

}
