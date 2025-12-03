view: csat_tickets {
    derived_table: {
      sql: with

              csat_tickets AS
              (
              SELECT * FROM ad_hoc.csat_tickets
              )

              select * from csat_tickets ;;
    }

    measure: count {
      type: count
      drill_fields: [detail*]
    }

    dimension: year {
      type: number
      sql: ${TABLE}.year ;;
    }

    dimension: week_number {
      type: number
      sql: ${TABLE}.week_number ;;
    }

    dimension: week_date {
      type: date
      datatype: date
      sql: ${TABLE}.week_date ;;
    }

  measure: num_tickets {
    type: sum
    sql: ${TABLE}.num_tickets ;;
  }

  measure: num_responses {
    type: sum
    sql: ${TABLE}.num_responses ;;
  }

  measure: response_rate {
    type: average
    sql: ${TABLE}.response_rate ;;
    value_format_name: "percent_0"
  }

    set: detail {
      fields: [
        year,
        week_number,
        week_date,
        num_tickets,
        num_responses,
        response_rate
      ]
    }
  }
