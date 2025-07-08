view: utm_lookup {
  derived_table: {
    sql:
  WITH marketing AS (
    SELECT
      anonymous_id
    FROM javascript_upff_home.pages
    WHERE search LIKE '%utm_source=guidepost%sugarcreekamish%'
  ),

  order_completed AS (
    SELECT
      DISTINCT anonymous_id
      ,user_email
      ,timestamp
    FROM javascript_upentertainment_checkout.order_completed
    WHERE anonymous_id IN (SELECT anonymous_id FROM marketing)
)

  select distinct * from order_completed where date(timestamp)>='2025-06-09'  ;;
  }

  dimension: time_period {
    type: date
    sql:${TABLE}.day;;
  }
}
