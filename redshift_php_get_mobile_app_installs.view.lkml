view: redshift_php_get_mobile_app_installs {
  sql_table_name: php.get_mobile_app_installs ;;

  dimension: id {
    primary_key: yes
    type: string
    sql: ${TABLE}.id ;;
  }

  dimension: _id {
    type: string
    sql: ${TABLE}._id ;;
  }

  dimension: ad_id {
    type: string
    sql: ${TABLE}.ad_id ;;
  }

  dimension: ad_name {
    type: string
    sql: ${TABLE}.ad_name ;;
  }

  dimension: ad_set_id {
    type: string
    sql: ${TABLE}.ad_set_id ;;
  }

  dimension: ad_set_name {
    type: string
    sql: ${TABLE}.ad_set_name ;;
  }

  dimension: advertising_partner_name {
    type: string
    sql: ${TABLE}.advertising_partner_name ;;
  }

  dimension: anonymous_id {
    type: string
    sql: ${TABLE}.anonymous_id ;;
  }

  dimension: campaign {
    type: string
    sql: ${TABLE}.campaign ;;
  }

  dimension: campaign_id {
    type: string
    sql: ${TABLE}.campaign_id ;;
  }

  dimension: context_library_consumer {
    type: string
    sql: ${TABLE}.context_library_consumer ;;
  }

  dimension: context_library_name {
    type: string
    sql: ${TABLE}.context_library_name ;;
  }

  dimension: context_library_version {
    type: string
    sql: ${TABLE}.context_library_version ;;
  }

  dimension: creative_id {
    type: string
    sql: ${TABLE}.creative_id ;;
  }

  dimension: creative_name {
    type: string
    sql: ${TABLE}.creative_name ;;
  }

  dimension: event {
    type: string
    sql: ${TABLE}.event ;;
  }

  dimension: event_text {
    type: string
    sql: ${TABLE}.event_text ;;
  }

  dimension: feature {
    type: string
    sql: ${TABLE}.feature ;;
  }

  dimension: last_attributed_touch_data_tilde_ad_id {
    type: string
    sql: ${TABLE}.last_attributed_touch_data_tilde_ad_id ;;
  }

  dimension: last_attributed_touch_data_tilde_ad_name {
    type: string
    sql: ${TABLE}.last_attributed_touch_data_tilde_ad_name ;;
  }

  dimension: last_attributed_touch_data_tilde_ad_set_id {
    type: string
    sql: ${TABLE}.last_attributed_touch_data_tilde_ad_set_id ;;
  }

  dimension: last_attributed_touch_data_tilde_ad_set_name {
    type: string
    sql: ${TABLE}.last_attributed_touch_data_tilde_ad_set_name ;;
  }

  dimension: last_attributed_touch_data_tilde_advertising_partner_name {
    type: string
    sql: ${TABLE}.last_attributed_touch_data_tilde_advertising_partner_name ;;
  }

  dimension: last_attributed_touch_data_tilde_campaign {
    type: string
    sql: ${TABLE}.last_attributed_touch_data_tilde_campaign ;;
  }

  dimension: last_attributed_touch_data_tilde_campaign_id {
    type: string
    sql: ${TABLE}.last_attributed_touch_data_tilde_campaign_id ;;
  }

  dimension: last_attributed_touch_data_tilde_creative_id {
    type: string
    sql: ${TABLE}.last_attributed_touch_data_tilde_creative_id ;;
  }

  dimension: last_attributed_touch_data_tilde_creative_name {
    type: string
    sql: ${TABLE}.last_attributed_touch_data_tilde_creative_name ;;
  }

  dimension: last_attributed_touch_data_tilde_feature {
    type: string
    sql: ${TABLE}.last_attributed_touch_data_tilde_feature ;;
  }

  dimension_group: original_timestamp {
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
    sql: ${TABLE}.original_timestamp ;;
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

  dimension: timestamp_iso {
    type: string
    sql: ${TABLE}.timestamp_iso ;;
  }

  dimension: upff {
    type: string
    sql: ${TABLE}.upff ;;
  }

  dimension: user_id {
    type: string
    sql: ${TABLE}.user_id ;;
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

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  # ----- Sets of fields for drilling ------
  set: detail {
    fields: [
      id,
      last_attributed_touch_data_tilde_ad_name,
      ad_name,
      last_attributed_touch_data_tilde_creative_name,
      context_library_name,
      last_attributed_touch_data_tilde_ad_set_name,
      last_attributed_touch_data_tilde_advertising_partner_name,
      advertising_partner_name,
      ad_set_name,
      creative_name
    ]
  }
}
