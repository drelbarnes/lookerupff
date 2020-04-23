view: mysql_email_campaigns {
  sql_table_name: admin_roku.email_campaigns ;;
  drill_fields: [id]

  dimension: id {
    primary_key: yes
    type: number
    sql: ${TABLE}.id ;;
  }

  dimension: user_id {
    type: number
    tags: ["user_id"]
    sql: 1 ;;
  }

  dimension: action {
    type: string
    sql: ${TABLE}.action ;;
  }

  dimension: campaign_id {
    type: string
    sql: ${TABLE}.campaign_id ;;
  }

  dimension: email {
    type: string
    tags: ["email"]
    sql: ${TABLE}.email ;;
  }

  dimension: ip {
    type: string
    sql: ${TABLE}.ip ;;
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


  dimension: url {
    type: string
    sql: ${TABLE}.url ;;
  }

  measure: count {
    type: count
    drill_fields: [id]
  }

  measure: open_count {
    type: count
    filters: {
      field: action
      value: "'open'"
    }
  }


  measure: open_count_distinct {
    type: count_distinct
    sql: ${email} ;;
    filters: {
      field: action
      value: "open"
    }
  }

  measure: click_count {
    type: count
    filters: {
      field: action
      value: "'click'"
    }
  }


  measure: click_count_distinct {
    type: count_distinct
    sql: ${email} ;;
    filters: {
      field: action
      value: "click"
    }
  }


}
