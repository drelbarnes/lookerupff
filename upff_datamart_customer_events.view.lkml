view: upff_datamart_customer_events {
    derived_table: {
      sql:

    SELECT
      user_id
      ,vimeo_id
      ,first_name
      ,last_name
      ,report_date as received_at
      ,state
      ,email
      ,subscription_status as status
      ,topic as event
    FROM ${subscriber_data.SQL_TABLE_NAME}
    ;;
    }

    measure: count {
      type: count
      drill_fields: [detail*]
    }

    dimension: vimeo_id {
      type: number
      sql: ${TABLE}.vimeo_id ;;
    }

  dimension: user_id {
    type: number
    sql: ${TABLE}.user_id ;;
    tags: ["user_id"]
  }


    dimension_group: received_at {
      type: time
      sql: ${TABLE}.received_at ;;
    }

    dimension: event {
      type: string
      sql: ${TABLE}.event ;;
    }

    dimension: status {
      type: string
      sql: ${TABLE}.status ;;
    }

    dimension: first_name {
      type: string
      sql: ${TABLE}.first_name ;;
    }

    dimension: last_name {
      type: string
      sql: ${TABLE}.last_name ;;
    }

    dimension: email {
      type: string
      sql: ${TABLE}.email ;;
    }

    set: detail {
      fields: [
        vimeo_id,
        user_id,
        received_at_time,
        event,
        status,
        first_name,
        last_name,
        email
      ]
    }
  }
