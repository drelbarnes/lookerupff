view: bigquery_javascript_all_page_views {
  derived_table: {
    sql:
with a as
(select anonymous_id,
       context_campaign_source,
       context_campaign_medium,
       context_campaign_name,
       context_campaign_content,
       context_campaign_term,
       context_page_referrer,
       replace(split(split(context_page_referrer, 'utm_campaign=')[safe_ordinal(2)],'&')[safe_ordinal(1)],'+',' ') as campaign_name,
       timestamp
from javascript.view
where date(timestamp)>'2019-09-15'
union all
select anonymous_id,
       context_campaign_source,
       context_campaign_medium,
       context_campaign_name,
       context_campaign_content,
       context_campaign_term,
       context_page_referrer,
       replace(split(split(context_page_referrer, 'utm_campaign=')[safe_ordinal(2)],'&')[safe_ordinal(1)],'+',' ') as campaign_name,
       timestamp
from javascript_upff_home.pages
where date(timestamp)>'2019-09-15')

(select a.*,
       case when name is null then 'organic' else 'paid' end as type
from a left join facebook_ads.campaigns as b on upper(a.campaign_name)=upper(name))
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

  dimension: context_campaign_source {
    type: string
    sql:  ${TABLE}.context_campaign_source;;
  }

  dimension: context_campaign_medium {
    type: string
    sql:  ${TABLE}.context_campaign_medium;;
  }

  dimension: context_campaign_name {
    type: string
    sql:  ${TABLE}.context_campaign_name;;
  }

  dimension: context_campaign_term {
    type: string
    sql:  ${TABLE}.context_campaign_term;;
  }

  dimension: context_campaign_content {
    type: string
    sql:  ${TABLE}.context_campaign_content;;
  }

  dimension_group: timestamp {
    type: time
    sql: ${TABLE}.timestamp ;;
  }

  dimension: visit_type {
    type: string
    sql: ${TABLE}.type ;;
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
