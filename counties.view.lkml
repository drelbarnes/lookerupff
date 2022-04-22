view: counties {
  derived_table: {
    sql: SELECT * FROM `up-faith-and-family-216419.http_api.counties`
      ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: county {
    type: string
    sql: ${TABLE}.county ;;
  }

  dimension: county_ascii {
    type: string
    sql: ${TABLE}.county_ascii ;;
  }

  dimension: county_full {
    type: string
    sql: ${TABLE}.county_full ;;
  }

  dimension: county_fips {
    type: number
    sql: ${TABLE}.county_fips ;;
  }

  dimension: state_id {
    type: string
    sql: ${TABLE}.state_id ;;
  }

  dimension: state_name {
    type: string
    sql: ${TABLE}.state_name ;;
  }

  dimension: lat {
    type: number
    sql: ${TABLE}.lat ;;
  }

  dimension: lng {
    type: number
    sql: ${TABLE}.lng ;;
  }

  dimension: population {
    type: number
    sql: ${TABLE}.population ;;
  }

  dimension: location {
    type: location
    sql_latitude: ${TABLE}.lat ;;
    sql_longitude: ${TABLE}.lng ;;
  }

  set: detail {
    fields: [
      county,
      county_ascii,
      county_full,
      county_fips,
      state_id,
      state_name,
      lat,
      lng,
      population,
      location
    ]
  }
}
