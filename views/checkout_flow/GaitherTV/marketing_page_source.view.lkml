view: marketing_page_source {
  derived_table: {
    sql:
    with result as(
    select
    url
    ,referrer
    ,anonymous_id
    ,CASE
      WHEN referrer LIKE '%upfaithandfamily%' or referrer LIKE '%upfaith.family%' THEN 'Upfaithandfamily'
      WHEN referrer LIKE '%search.yahoo%' THEN 'Yahoo'
      WHEN referrer LIKE '%uptv%' THEN 'UPTV'
      WHEN referrer LIKE '%bing%' THEN 'Bing'
      --WHEN referrer LIKE '%googlesyndication%' THEN 'googlesyndication'
      WHEN referrer LIKE '%google%' THEN 'Google'
      WHEN referrer LIKE '%facebook%' THEN 'Facebook'
      WHEN referrer LIKE '%instagram%' THEN 'Instagram'
      WHEN referrer LIKE '%youtube%' THEN 'YouTube'
      WHEN referrer is NOT NULL THEN 'OTHER'

      ELSE 'NONE'
      END as referrer_cleaned
      ,timestamp
      from javascript_gaither_tv.pages
      where url LIKE '%gaither%')
      select * from result;;

  }


  dimension: date {
    type: date
    sql: ${TABLE}.timestamp ;;
  }

  dimension: url {
    type: string
    sql: ${TABLE}.url ;;
  }

  dimension: referrer {
    type: string
    sql: ${TABLE}.referrer_cleaned ;;
  }
  dimension: referrer_original_value {
    type: string
    sql: ${TABLE}.referrer ;;
  }
  measure: Total_referrer_count {
    type: count_distinct
    sql: ${TABLE}.anonymous_id ;;
    label: "Total Referrer Count"
  }
  measure: upfaithandfamily_referrer_count {
    type: count_distinct
    sql: ${TABLE}.anonymous_id ;;
    label: "Upfaithandfamily Referrer Count"
    filters: [referrer: "Upfaithandfamily"]
  }

  measure: yahoo_referrer_count {
    type: count_distinct
    sql: ${TABLE}.anonymous_id ;;
    label: "Yahoo Referrer Count"
    filters: [referrer: "Yahoo"]
  }

  measure: uptv_referrer_count {
    type: count_distinct
    sql: ${TABLE}.anonymous_id ;;
    label: "UPTV Referrer Count"
    filters: [referrer: "UPTV"]
  }

  measure: bing_referrer_count {
    type: count_distinct
    sql: ${TABLE}.anonymous_id ;;
    label: "Bing Referrer Count"
    filters: [referrer: "Bing"]
  }

  measure: google_referrer_count {
    type: count_distinct
    sql: ${TABLE}.anonymous_id ;;
    label: "Google Referrer Count"
    filters: [referrer: "Google"]
  }

  measure: facebook_referrer_count {
    type: count_distinct
    sql: ${TABLE}.anonymous_id ;;
    label: "Facebook Referrer Count"
    filters: [referrer: "Facebook"]
  }

  measure: instagram_referrer_count {
    type: count_distinct
    sql: ${TABLE}.anonymous_id ;;
    label: "Instagram Referrer Count"
    filters: [referrer: "Instagram"]
  }

  measure: youtube_referrer_count {
    type: count_distinct
    sql: ${TABLE}.anonymous_id ;;
    label: "YouTube Referrer Count"
    filters: [referrer: "YouTube"]
  }

  measure: other_referrer_count {
    type: count_distinct
    sql: ${TABLE}.anonymous_id ;;
    label: "Other Referrer Count"
    filters: [referrer: "OTHER"]
  }

  measure: none_referrer_count {
    type: count_distinct
    sql: ${TABLE}.anonymous_id ;;
    label: "No Referrer Count"
    filters: [referrer: "NONE"]
  }




}
