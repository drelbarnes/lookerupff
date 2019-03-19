view: bigquery_looker_get_app_installs {
  sql_table_name: php.get_app_installs ;;

  dimension: id {
    primary_key: yes
    type: string
    sql: ${TABLE}.id ;;
  }

  dimension: aaid {
    type: string
    sql: ${TABLE}.aaid ;;
  }

  dimension: ad_id {
    type: string
    sql: ${TABLE}.ad_id ;;
  }

  dimension: ad_name {
    type: string
    sql: ${TABLE}.ad_name ;;
  }

  dimension: ad_objective_name {
    type: string
    sql: ${TABLE}.ad_objective_name ;;
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

  dimension: brand {
    type: string
    sql: ${TABLE}.brand ;;
  }

  dimension: campaign {
    type: string
    sql: ${TABLE}.campaign ;;
  }

  dimension: campaign_id {
    type: string
    sql: ${TABLE}.campaign_id ;;
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

  dimension: creative_id {
    type: string
    sql: ${TABLE}.creative_id ;;
  }

  dimension: creative_name {
    type: string
    sql: ${TABLE}.creative_name ;;
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

  dimension: location {
    type: location
    sql_latitude: ${geo_lat};;
    sql_longitude: ${geo_lon};;

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

  dimension: last_attributed_touch_type {
    type: string
    sql: ${TABLE}.last_attributed_touch_type ;;
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

  dimension: model {
    type: string
    sql: ${TABLE}.model ;;
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

  dimension: upff {
    type: string
    sql: ${TABLE}.upff ;;
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
    drill_fields: [detail*]
  }

  measure: count_anonymous_id {
    type: count_distinct
    sql: ${anonymous_id} ;;
    drill_fields: [detail*]
  }

  # ----- Sets of fields for drilling ------
  set: detail {
    fields: [
      id,
      creative_name,
      advertising_partner_name,
      ad_objective_name,
      context_library_name,
      ad_name,
      ad_set_name
    ]
  }
}
