view: uptv_gilmore {
  derived_table: {
    sql: SELECT * FROM gilmore_the_merrier.gtm_users
      ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: id {
    type: number
    primary_key: yes
    sql: ${TABLE}.id ;;
  }

  dimension: fname {
    type: string
    sql: ${TABLE}.fname ;;
  }

  dimension: lname {
    type: string
    sql: ${TABLE}.lname ;;
  }

  dimension: email {
    type: string
    sql: ${TABLE}.email ;;
  }

  dimension: password {
    type: string
    sql: ${TABLE}.password ;;
  }

  dimension: hashed_password {
    type: string
    sql: ${TABLE}.hashed_password ;;
  }

  dimension: activation {
    type: string
    sql: ${TABLE}.activation ;;
  }

  dimension_group: date {
    type: time
    sql: ${TABLE}.date ;;
  }

  dimension: birthday {
    type: string
    sql: ${TABLE}.birthday ;;
  }

  dimension_group: last_time_logged_in {
    type: time
    sql: ${TABLE}.last_time_logged_in ;;
  }

  dimension: twitter {
    type: string
    sql: ${TABLE}.twitter ;;
  }

  dimension: twitter_id {
    type: string
    sql: ${TABLE}.twitter_id ;;
  }

  dimension: ip {
    type: string
    sql: ${TABLE}.ip ;;
  }

  dimension: access {
    type: string
    sql: ${TABLE}.access ;;
  }

  dimension: receive_email {
    type: string
    sql: ${TABLE}.receive_email ;;
  }

  dimension: received_dm {
    type: string
    sql: ${TABLE}.received_dm ;;
  }

  set: detail {
    fields: [
      id,
      fname,
      lname,
      email,
      password,
      hashed_password,
      activation,
      date_time,
      birthday,
      last_time_logged_in_time,
      twitter,
      twitter_id,
      ip,
      access,
      receive_email,
      received_dm
    ]
  }
}
