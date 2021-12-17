view: svod_titles_general {
    derived_table: {
      sql: select * from svod_titles.titles
        ;;
    }

    measure: count {
      type: count
      drill_fields: [detail*]
    }

    dimension: month {
      type: number
      sql: ${TABLE}.month ;;
    }

    dimension: year {
      type: number
      sql: ${TABLE}.year ;;
    }

    dimension: platform {
      type: string
      sql: ${TABLE}.platform ;;
    }

    dimension: up_title {
      type: string
      sql: ${TABLE}.up_title ;;
    }

    dimension: studio {
      type: string
      sql: ${TABLE}.studio ;;
    }

    dimension: views {
      type: number
      sql: ${TABLE}.views ;;
    }

    dimension: type {
      type: string
      sql: ${TABLE}.type ;;
    }

    dimension: category {
      type: string
      sql: ${TABLE}.category ;;
    }

    dimension: franchise {
      type: string
      sql: ${TABLE}.franchise ;;
    }

    dimension: season {
      type: number
      sql: ${TABLE}.season ;;
    }

    dimension: lf_sf {
      type: string
      sql: ${TABLE}.lf_sf ;;
    }

    dimension: content_type {
      type: string
      sql: ${TABLE}.content_type ;;
    }

    dimension: datetime {
      type: date
      datatype: date
      sql: ${TABLE}.datetime ;;
    }

    set: detail {
      fields: [
        month,
        year,
        platform,
        up_title,
        studio,
        views,
        type,
        category,
        franchise,
        season,
        lf_sf,
        content_type,
        datetime
      ]
    }
  }
