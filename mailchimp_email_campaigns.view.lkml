view: mailchimp_email_campaigns {
  sql_table_name: customers.email_campaigns ;;

  dimension_group: campaign {
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
    sql: ${TABLE}.campaign_date ;;
  }

  dimension: campaign_id {
    type: number
    sql: ${TABLE}.campaign_id ;;
  }

  dimension: clicked {
    type: string
    sql: ${TABLE}.clicked ;;
  }

  dimension: email_address {
    type: string
    sql: ${TABLE}.email_address ;;
  }

  dimension: opened {
    type: string
    sql: ${TABLE}.opened ;;
  }

  dimension: userid {
    primary_key: yes
    type: number
    sql: ${TABLE}.userid ;;
  }

  measure: has_opened_email{
    type: count_distinct
    sql: ${userid} ;;
    filters: {
      field: opened
      value: "Yes"
    }
  }

  measure: count {
    type: count
    drill_fields: [userid, email_address,campaign_id,campaign_date]
  }

  measure: count_users {
    type: count_distinct
    sql: ${userid} ;;
    drill_fields: [userid, email_address,campaign_id,campaign_date]
  }

}
