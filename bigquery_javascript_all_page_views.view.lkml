view: bigquery_javascript_all_page_views {
  derived_table: {
    sql: select anonymous_id,
       context_page_referrer,
       timestamp
from javascript.view
union all
select anonymous_id,
       context_campaign_referrer,
       timestamp
from javascript_upff_home.pages
 ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: anonymous_id {
    type: string
    sql: ${TABLE}.anonymous_id ;;
  }

  dimension: context_page_referrer {
    type: string
    sql: ${TABLE}.context_page_referrer ;;
  }


  dimension_group: timestamp {
    type: time
    sql: ${TABLE}.timestamp ;;
  }

  dimension: visit_type {
    type: string
    sql: case when lower(${context_page_referrer}) like '%heartland%'
           or lower(${context_page_referrer}) like '%somebodychild%'
           or lower(${context_page_referrer}) like '%potential%'
           or lower(${context_page_referrer}) like '%wayhomewicketmvp%'
           or lower(${context_page_referrer}) like '%heartforgives%'
           or lower(${context_page_referrer}) like '%23blast%'
           or lower(${context_page_referrer}) like '%lfyivwicketmvp%'
           or lower(${context_page_referrer}) like '%scarlett%'
           or lower(${context_page_referrer}) like '%loveslastresort%'
           or lower(${context_page_referrer}) like '%touchedbygracewicketmvp%'
           or lower(${context_page_referrer}) like '%amancalledjon%'
           or lower(${context_page_referrer}) like '%remembergoal%'
           or lower(${context_page_referrer}) like '%wayhomewicketmvp%'
           or lower(${context_page_referrer}) like '%dancer%' then 'paid' else 'organic' end ;;
  }

  dimension: campaign {
    type: string
    sql: case when lower(${context_page_referrer}) like '%heartland%' then 'heartland'
              when lower(${context_page_referrer}) like '%somebodychild%' then 'somebodychild'
              when lower(${context_page_referrer}) like '%potential%' then 'potential'
              when lower(${context_page_referrer}) like '%wayhomewicketmvp%' then 'wayhomewicketmvp'
              when lower(${context_page_referrer}) like '%heartforgives%' then 'heartforgives'
              when lower(${context_page_referrer}) like '%23blast%' then '23blast'
              when lower(${context_page_referrer}) like '%lfyivwicketmvp%' then 'lfyivwicketmvp'
              when lower(${context_page_referrer}) like '%scarlett%' then 'scarlett'
              when lower(${context_page_referrer}) like '%loveslastresort%' then 'loveslastresort'
              when lower(${context_page_referrer}) like '%touchedbygracewicketmvp%' then 'touchedbygracewicketmvp'
              when lower(${context_page_referrer}) like '%amancalledjon%' then 'amancalledjon'
              when lower(${context_page_referrer}) like '%remembergoal%' then 'remembergoal'
              when lower(${context_page_referrer}) like '%wayhomewicketmvp%' then 'wayhomewicketmvp'
              when lower(${context_page_referrer}) like '%dancer%' then 'dancer' else 'organic' end ;;
  }

  measure: visitor_count {
    type: count_distinct
    sql: ${anonymous_id} ;;
  }

  set: detail {
    fields: [anonymous_id, context_page_referrer, timestamp_time]
  }
}
