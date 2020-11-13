view: promos {
  derived_table: {
    sql: select * from svod_titles.promos3q20
      ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: collection {
    type: string
    sql: ${TABLE}.collection ;;
  }

  dimension: title {
    type: string
    sql: ${TABLE}.title ;;
  }

  dimension: video_id {
    type: number
    sql: ${TABLE}.video_id ;;
  }

  dimension: promo {
    type: date
    datatype: date
    sql: ${TABLE}.promo ;;
  }

  set: detail {
    fields: [collection, title, video_id, promo]
  }
}
