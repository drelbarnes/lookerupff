view: bigquery_clickthroughs {
  derived_table: {
    sql: select anonymous_id,
       received_at,
       context_campaign_name,
       context_campaign_content,
       context_campaign_source,
       context_campaign_medium,
       os,
       event
from android.branch_install
union all
select anonymous_id,
       received_at,
       context_campaign_name,
       context_campaign_content,
       context_campaign_source,
       context_campaign_medium,
       os,
       event
from android.branch_reinstall
union all
select anonymous_id,
       received_at,
       context_campaign_name,
       context_campaign_content,
       context_campaign_source,
       context_campaign_medium,
       os,
       event
from ios.branch_install
union all
select anonymous_id,
       received_at,
       context_campaign_name,
       context_campaign_content,
       context_campaign_source,
       context_campaign_medium,
       os,
       event
from ios.branch_reinstall
union all
select anonymous_id,
       received_at,
       context_campaign_name,
       context_campaign_content,
       context_campaign_source,
       context_campaign_medium,
       "Web" as os,
       "UPFF Pages" as event
from javascript.pages
union all
select anonymous_id,
       received_at,
       context_campaign_name,
       context_campaign_content,
       context_campaign_source,
       context_campaign_medium,
       "Web" as os,
       "UPtv Pages" as event
from javascript_up_tv.pages
union all
select anonymous_id,
       received_at,
       context_campaign_name,
       context_campaign_content,
       context_campaign_source,
       context_campaign_medium,
       "Web" as os,
       "UPFF Home" as event
from javascript_upff_home.pages
 ;;
  }


  dimension: anonymous_id {
    type: string
    sql: ${TABLE}.anonymous_id ;;
  }

  measure: count {
    type: count_distinct
    sql: ${anonymous_id} ;;
  }

  dimension_group: received_at {
    type: time
    sql: ${TABLE}.received_at ;;
  }

  dimension: context_campaign_name {
    type: string
    sql: lower(${TABLE}.context_campaign_name) ;;
  }

  dimension: context_campaign_content {
    type: string
    sql: ${TABLE}.context_campaign_content ;;
  }

  dimension: context_campaign_source {
    type: string
    sql: ${TABLE}.context_campaign_source ;;
  }

  dimension: context_campaign_medium {
    type: string
    sql: ${TABLE}.context_campaign_medium ;;
  }

  dimension: os {
    type: string
    sql: ${TABLE}.os ;;
  }

  dimension: event {
    type: string
    sql: ${TABLE}.event ;;
  }

  set: detail {
    fields: [
      anonymous_id,
      received_at_time,
      context_campaign_name,
      context_campaign_content,
      context_campaign_source,
      context_campaign_medium,
      os,
      event
    ]
  }
}
