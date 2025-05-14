view: nps_trial {
 derived_table:{
   sql: with raiting as (
SELECT
    (DATE("redshift_php_get_trialist_survey"."received_at")) AS "redshift_php_get_trialist_survey.received_date",
    "http_api_purchase_event"."email" AS "http_api_purchase_event.email",
    "redshift_php_get_trialist_survey"."user_id" AS "redshift_php_get_trialist_survey.user_id",
    "http_api_purchase_event"."fname" AS "http_api_purchase_event.fname",
    "http_api_purchase_event"."region" AS "http_api_purchase_event.region",
    "http_api_purchase_event"."platform" AS "http_api_purchase_event.platform",
    CASE
WHEN CAST(redshift_php_get_trialist_survey.rating AS INT) = 9 OR
          CAST(redshift_php_get_trialist_survey.rating AS INT) = 10  THEN '0'
WHEN CAST(redshift_php_get_trialist_survey.rating AS INT)  = 7 OR
          CAST(redshift_php_get_trialist_survey.rating AS INT)  = 8  THEN '1'
WHEN CAST(redshift_php_get_trialist_survey.rating AS INT) <= 6  THEN '2'
ELSE '3'
END AS "redshift_php_get_trialist_survey.nps_cat__sort_",
    CASE
WHEN CAST(redshift_php_get_trialist_survey.rating AS INT) = 9 OR
          CAST(redshift_php_get_trialist_survey.rating AS INT) = 10  THEN 'Promoters'
WHEN CAST(redshift_php_get_trialist_survey.rating AS INT)  = 7 OR
          CAST(redshift_php_get_trialist_survey.rating AS INT)  = 8  THEN 'Passives'
WHEN CAST(redshift_php_get_trialist_survey.rating AS INT) <= 6  THEN 'Detractors'
ELSE 'Nevers'
END AS "redshift_php_get_trialist_survey.nps_cat",
    cast(redshift_php_get_trialist_survey.rating as INT)  AS "redshift_php_get_trialist_survey.total_rating",
    "redshift_php_get_trialist_survey"."reason" AS "redshift_php_get_trialist_survey.reason",
    "redshift_php_get_trialist_survey"."comment" AS "redshift_php_get_trialist_survey.comment",
    "redshift_php_get_trialist_survey"."programming" AS "redshift_php_get_trialist_survey.programming"
FROM
    "http_api"."purchase_event" AS "http_api_purchase_event"
    LEFT JOIN "php"."get_trialist_survey" AS "redshift_php_get_trialist_survey" ON "http_api_purchase_event"."user_id" = "redshift_php_get_trialist_survey"."user_id"
WHERE ("http_api_purchase_event"."email" NOT LIKE '%uptv.com%' AND "http_api_purchase_event"."email" NOT LIKE '%drebarnes.com%' OR "http_api_purchase_event"."email" IS NULL) AND "http_api_purchase_event"."topic" = 'customer.created'
GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8,
    9,
    10,
    11,
    12
),
result as (
SELECT
  r.*,
  CASE
    WHEN r."http_api_purchase_event.email" IN (
      SELECT customer_email
      FROM http_api.chargebee_subscriptions
      WHERE DATE(uploaded_at) = CURRENT_DATE - INTERVAL '1 day'
        AND subscription_status IN ('active', 'non_renewing') and  subscription_subscription_items_0_item_price_id LIKE '%UP%'
    ) THEN 'Chargebee'

    WHEN r."http_api_purchase_event.email" IN (
      SELECT email
      FROM customers.all_customers
      WHERE status = ''
        AND report_date = CURRENT_DATE - INTERVAL '1 day'
    ) THEN 'Vimeo'


  END AS active_platform
  ,CASE
    WHEN r."http_api_purchase_event.email" IN (SELECT customer_email
      FROM http_api.chargebee_subscriptions WHERE subscription_subscription_items_0_item_price_id LIKE '%UP%')
      THEN 'Chargebee'
    ELSE 'Vimeo'
  END AS account_platform

FROM raiting r)
select * from result
where account_platform = 'Chargebee'
and "http_api_purchase_event.email" in (SELECT customer_email
      FROM http_api.chargebee_subscriptions
      WHERE DATE(uploaded_at) = CURRENT_DATE - INTERVAL '1 day'
        AND subscription_status IN ('cancelled', 'paused'));;
 }

  dimension: received_date {
    type: date
    sql: ${TABLE}."redshift_php_get_trialist_survey.received_date" ;;
  }

  dimension: email {
    type: string
    sql: ${TABLE}."http_api_purchase_event.email" ;;
  }

  dimension: user_id {
    type: string
    sql: ${TABLE}."redshift_php_get_trialist_survey.user_id" ;;
  }

  dimension: fname {
    type: string
    sql: ${TABLE}."http_api_purchase_event.fname" ;;
  }

  dimension: region {
    type: string
    sql: ${TABLE}."http_api_purchase_event.region";;
  }

  dimension: platform {
    type: string
    sql: ${TABLE}."http_api_purchase_event.platform";;
  }

  dimension: total_rating {
    type: number
    sql: ${TABLE}."redshift_php_get_trialist_survey.total_rating" ;;
  }

  dimension: reason {
    type: string
    sql: ${TABLE}."redshift_php_get_trialist_survey.reason" ;;
  }

  dimension: comment {
    type: string
    sql: ${TABLE}."redshift_php_get_trialist_survey.comment" ;;
  }

  dimension: programming {
    type: string
    sql: ${TABLE}."redshift_php_get_trialist_survey.programming" ;;
  }

  dimension: account_platform {
    type: string
    sql:
    CASE
      WHEN ${TABLE}.active_platform is NULL THEN ${TABLE}.account_platform
      ELSE NULL
    END
  ;;
  }



}
