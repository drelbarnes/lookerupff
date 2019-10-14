explore: titles_id_mapping {}
view: titles_id_mapping {
  derived_table: {
    sql:select a.*
from svod_titles.titles_id_mapping as a inner join svod_titles.content as b on a.id=b.id;;}

  dimension: id {
    primary_key: yes
    type: number
    sql: ${TABLE}.id ;;
  }

dimension: collection {
  type: string
  sql: ${TABLE}.collection ;;
}

dimension: collection_id {
  type: string
  sql: ${TABLE}.collection_id ;;
}

dimension: type {
  type: string
  sql:  case when ${series} is null and upper(${collection})=upper(${title}) then 'Movies'
                     when ${series} is not null then 'Series' else 'Movies' end  ;;
}

  dimension_group: date {
    type: time
    timeframes: [
      raw,
      date,
      week,
      month,
      quarter,
      year
    ]
    convert_tz: no
    datatype: date
    sql: ${TABLE}.date ;;
  }

  dimension: episode {
    type: number
    sql: ${TABLE}.episode ;;
  }

  dimension: season {
    type: string
    sql: ${TABLE}.season ;;
  }

  dimension: series {
    type: string
    sql: ${TABLE}.series ;;
  }

  dimension: title {
    type: string
    sql: ${TABLE}.title ;;
  }

  dimension: duration {
    type: number
    sql: ${TABLE}.duration ;;
  }

  dimension_group: promotion_date {
    type: time
    timeframes: [
      raw,
      date,
      week,
      month,
      quarter,
      year
    ]
    convert_tz: no
    datatype: date
    sql: ${TABLE}.promotion ;;
  }

  measure: count {
    type: count
    drill_fields: [id]
  }
}
