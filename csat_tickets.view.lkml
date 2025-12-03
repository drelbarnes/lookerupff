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

    dimension: num_tickets {
      type: number
      sql: ${TABLE}.num_tickets ;;
    }

    dimension: num_responses {
      type: number
      sql: ${TABLE}.num_responses ;;
    }

    dimension: response_rate {
      type: number
      sql: ${TABLE}.response_rate ;;
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
