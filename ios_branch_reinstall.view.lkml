view: ios_branch_reinstall {
  sql_table_name: ios.branch_reinstall ;;

  dimension: id {
    primary_key: yes
    type: string
    sql: ${TABLE}.id ;;
  }

  dimension: _id {
    type: string
    sql: ${TABLE}._id ;;
  }

  dimension: alias {
    type: string
    sql: ${TABLE}.alias ;;
  }

  dimension: anonymous_id {
    type: string
    sql: ${TABLE}.anonymous_id ;;
  }

  dimension: branch_match_id {
    type: string
    sql: ${TABLE}.branch_match_id ;;
  }

  dimension: campaign {
    type: string
    sql: ${TABLE}.campaign ;;
  }

  dimension: canonical_url {
    type: string
    sql: ${TABLE}.canonical_url ;;
  }

  dimension: click_timestamp {
    type: string
    sql: ${TABLE}.click_timestamp ;;
  }

  dimension: collection {
    type: string
    sql: ${TABLE}.collection ;;
  }

  dimension: context_campaign_content {
    type: string
    sql: ${TABLE}.context_campaign_content ;;
  }

  dimension: context_campaign_medium {
    type: string
    sql: ${TABLE}.context_campaign_medium ;;
  }

  dimension: context_campaign_name {
    type: string
    sql: ${TABLE}.context_campaign_name ;;
  }

  dimension: context_campaign_source {
    type: string
    sql: ${TABLE}.context_campaign_source ;;
  }

  dimension: context_device_id {
    type: string
    sql: ${TABLE}.context_device_id ;;
  }

  dimension: context_idfa {
    type: string
    sql: ${TABLE}.context_idfa ;;
  }

  dimension: context_idfv {
    type: string
    sql: ${TABLE}.context_idfv ;;
  }

  dimension: context_ip {
    type: string
    sql: ${TABLE}.context_ip ;;
  }

  dimension: context_library_name {
    type: string
    sql: ${TABLE}.context_library_name ;;
  }

  dimension: context_library_version {
    type: string
    sql: ${TABLE}.context_library_version ;;
  }

  dimension: context_os {
    type: string
    sql: ${TABLE}.context_os ;;
  }

  dimension: context_user_agent {
    type: string
    sql: ${TABLE}.context_user_agent ;;
  }

  dimension: creation_source {
    type: string
    sql: ${TABLE}.creation_source ;;
  }

  dimension: desktop_url {
    type: string
    sql: ${TABLE}.desktop_url ;;
  }

  dimension: domain {
    type: string
    sql: ${TABLE}.domain ;;
  }

  dimension: event {
    type: string
    sql: ${TABLE}.event ;;
  }

  dimension: event_text {
    type: string
    sql: ${TABLE}.event_text ;;
  }

  dimension: ios_passive_deepview {
    type: string
    sql: ${TABLE}.ios_passive_deepview ;;
  }

  dimension: ip {
    type: string
    sql: ${TABLE}.ip ;;
  }

  dimension: journey_id {
    type: string
    sql: ${TABLE}.journey_id ;;
  }

  dimension: journey_name {
    type: string
    sql: ${TABLE}.journey_name ;;
  }

  dimension: marketing {
    type: string
    sql: ${TABLE}.marketing ;;
  }

  dimension: marketing_title {
    type: string
    sql: ${TABLE}.marketing_title ;;
  }

  dimension: mc_cid {
    type: string
    sql: ${TABLE}.mc_cid ;;
  }

  dimension: mc_eid {
    type: string
    sql: ${TABLE}.mc_eid ;;
  }

  dimension: og_app_id {
    type: string
    sql: ${TABLE}.og_app_id ;;
  }

  dimension: og_description {
    type: string
    sql: ${TABLE}.og_description ;;
  }

  dimension: og_image_url {
    type: string
    sql: ${TABLE}.og_image_url ;;
  }

  dimension: og_title {
    type: string
    sql: ${TABLE}.og_title ;;
  }

  dimension: og_type {
    type: string
    sql: ${TABLE}.og_type ;;
  }

  dimension: one_time_use {
    type: string
    sql: ${TABLE}.one_time_use ;;
  }

  dimension: original_timestamp {
    type: string
    sql: ${TABLE}.original_timestamp ;;
  }

  dimension: os {
    type: string
    sql: ${TABLE}.os ;;
  }

  dimension: platform {
    type: string
    sql: ${TABLE}.platform ;;
  }

  dimension_group: received {
    type: time
    timeframes: [
      raw,
      time,
      date,
      week,
      month,
      quarter,
      year
    ]
    sql: ${TABLE}.received_at ;;
  }

  dimension: referrer {
    type: string
    sql: ${TABLE}.referrer ;;
  }

  dimension: referring_domain {
    type: string
    sql: ${TABLE}.referring_domain ;;
  }

  dimension_group: sent {
    type: time
    timeframes: [
      raw,
      time,
      date,
      week,
      month,
      quarter,
      year
    ]
    sql: ${TABLE}.sent_at ;;
  }

  dimension_group: timestamp {
    type: time
    timeframes: [
      raw,
      time,
      date,
      week,
      month,
      quarter,
      year
    ]
    sql: ${TABLE}.timestamp ;;
  }

  dimension: uri_redirect_mode {
    type: string
    sql: ${TABLE}.uri_redirect_mode ;;
  }

  dimension: url {
    type: string
    sql: ${TABLE}.url ;;
  }

  dimension: user_agent {
    type: string
    sql: ${TABLE}.user_agent ;;
  }

  dimension: uuid {
    type: number
    value_format_name: id
    sql: ${TABLE}.uuid ;;
  }

  dimension_group: uuid_ts {
    type: time
    timeframes: [
      raw,
      time,
      date,
      week,
      month,
      quarter,
      year
    ]
    sql: ${TABLE}.uuid_ts ;;
  }

  dimension: via_features {
    type: string
    sql: ${TABLE}.via_features ;;
  }

  dimension: video {
    type: string
    sql: ${TABLE}.video ;;
  }

  dimension: view_id {
    type: string
    sql: ${TABLE}.view_id ;;
  }

  dimension: view_name {
    type: string
    sql: ${TABLE}.view_name ;;
  }

  measure: count {
    type: count
    drill_fields: [id, context_library_name, context_campaign_name, view_name, journey_name]
  }
}
