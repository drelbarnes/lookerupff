view: op_uplift_registrations {
  derived_table: {
    sql:select distinct * from svod_titles.op_uplift_registrations;;}

  dimension: email {
    type: string
    sql: ${TABLE}.Email ;;
  }

  dimension_group: entry {
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
    sql: ${TABLE}.Entry ;;
  }

  dimension: source_url {
    type: string
    sql: ${TABLE}.Source_Url ;;
  }

  dimension: source {
    type: string
    sql: case when source_url not like '%medium=%' then 'organic' else substr(substr(source_url,strpos(source_url,'medium=')+7),0,strpos(substr(source_url,strpos(source_url,'medium=')+7),'&utm_campaign')-1) end ;;
  }

  measure: sign_ups {
    type: count_distinct
    sql: ${email} ;;
  }


  measure: count {
    type: count
    drill_fields: []
  }
}
