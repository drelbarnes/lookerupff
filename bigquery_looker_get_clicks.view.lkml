view: bigquery_looker_get_clicks {
  sql_table_name: php.get_clicks ;;

  dimension: id {
    primary_key: yes
    type: string
    sql: ${TABLE}.id ;;
  }

  dimension: _id {
    type: number
    sql: ${TABLE}._id ;;
  }

  dimension: advertising_partner_name {
    type: string
    sql: ${TABLE}.advertising_partner_name ;;
  }

  dimension: brand {
    type: string
    sql: ${TABLE}.brand ;;
  }

  dimension: browser {
    type: string
    sql: ${TABLE}.browser ;;
  }

  dimension: campaign {
    type: string
    sql: ${TABLE}.campaign ;;
  }

  dimension: canonical_url {
    type: string
    sql: ${TABLE}.canonical_url ;;
  }

  dimension: channel {
    type: string
    sql: ${TABLE}.channel ;;
  }

  dimension: click_timestamp {
    type: number
    sql: ${TABLE}.click_timestamp ;;
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

  dimension: creation_source {
    type: number
    sql: ${TABLE}.creation_source ;;
  }

  dimension: current_feature {
    type: string
    sql: ${TABLE}.current_feature ;;
  }

  dimension: desktop_url {
    type: string
    sql: ${TABLE}.desktop_url ;;
  }

  dimension: domain {
    type: string
    sql: ${TABLE}.domain ;;
  }

  dimension: environment {
    type: string
    sql: ${TABLE}.environment ;;
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

  dimension: geo_city_code {
    type: number
    sql: ${TABLE}.geo_city_code ;;
  }

  dimension: geo_country_code {
    type: string
    sql: ${TABLE}.geo_country_code ;;
  }

  dimension: geo_dma_code {
    type: number
    sql: ${TABLE}.geo_dma_code ;;
  }

  dimension: geo_lat {
    type: number
    sql: ${TABLE}.geo_lat ;;
  }

  dimension: geo_lon {
    type: number
    sql: ${TABLE}.geo_lon ;;
  }


  dimension: geo_region_code {
    type: string
    sql: ${TABLE}.geo_region_code ;;
  }

  dimension: geo_region_en {
    type: string
    sql: ${TABLE}.geo_region_en ;;
  }

  dimension: ip {
    type: string
    sql: ${TABLE}.ip ;;
  }

  dimension: last_attributed_touch_timestamp {
    type: number
    sql: ${TABLE}.last_attributed_touch_timestamp ;;
  }

  dimension_group: loaded {
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
    sql: ${TABLE}.loaded_at ;;
  }

  dimension: marketing {
    type: yesno
    sql: ${TABLE}.marketing ;;
  }

  dimension: marketing_title {
    type: string
    sql: ${TABLE}.marketing_title ;;
  }

  dimension: model {
    type: string
    sql: ${TABLE}.model ;;
  }

  dimension: name {
    type: string
    sql: ${TABLE}.name ;;
  }

  dimension: og_app_id {
    type: string
    sql: ${TABLE}.og_app_id ;;
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
    type: yesno
    sql: ${TABLE}.one_time_use ;;
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

  dimension: os {
    type: string
    sql: ${TABLE}.os ;;
  }

  dimension: os_version {
    type: string
    sql: ${TABLE}.os_version ;;
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

  dimension: secondary_ad_format {
    type: string
    sql: ${TABLE}.secondary_ad_format ;;
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

  dimension: tags {
    type: string
    sql: ${TABLE}.tags ;;
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

  dimension: upff {
    type: string
    sql: ${TABLE}.upff ;;
  }

  dimension: url {
    type: string
    sql: ${TABLE}.url ;;
  }

  dimension: user_agent {
    type: string
    sql: ${TABLE}.user_agent ;;
  }

  dimension: user_id {
    type: string
    sql: ${TABLE}.user_id ;;
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
    drill_fields: [id, advertising_partner_name, context_library_name, name]
  }
}
