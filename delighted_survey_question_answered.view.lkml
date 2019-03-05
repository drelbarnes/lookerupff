view: delighted_survey_question_answered {
  sql_table_name: delighted.survey_question_answered ;;

  dimension: id {
    primary_key: yes
    type: string
    sql: ${TABLE}.id ;;
  }

  dimension: anonymous_id {
    type: string
    sql: ${TABLE}.anonymous_id ;;
  }

  dimension: context_integration_name {
    type: string
    sql: ${TABLE}.context_integration_name ;;
  }

  dimension: context_integration_version {
    type: string
    sql: ${TABLE}.context_integration_version ;;
  }

  dimension: context_library_name {
    type: string
    sql: ${TABLE}.context_library_name ;;
  }

  dimension: context_library_version {
    type: string
    sql: ${TABLE}.context_library_version ;;
  }

  dimension: context_traits_delighted_person_id {
    type: string
    sql: ${TABLE}.context_traits_delighted_person_id ;;
  }

  dimension: context_traits_email {
    type: string
    tags: ["email"]
    sql: ${TABLE}.context_traits_email ;;
  }

  dimension: event {
    type: string
    sql: ${TABLE}.event ;;
  }

  dimension: event_text {
    type: string
    sql: ${TABLE}.event_text ;;
  }

  dimension: original_timestamp {
    type: string
    sql: ${TABLE}.original_timestamp ;;
  }

  dimension_group: received {
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
    sql: ${TABLE}.received_at ;;
  }

  dimension_group: sent {
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
    sql: ${TABLE}.sent_at ;;
  }

  dimension: survey_id {
    type: string
    sql: ${TABLE}.survey_id ;;
  }

  dimension: survey_medium {
    type: string
    sql: ${TABLE}.survey_medium ;;
  }

  dimension: survey_name {
    type: string
    sql: ${TABLE}.survey_name ;;
  }

  dimension: survey_notes {
    type: string
    sql: ${TABLE}.survey_notes ;;
  }

  dimension: survey_permalink {
    type: string
    sql: ${TABLE}.survey_permalink ;;
  }

  dimension: survey_properties_delighted_browser {
    type: string
    sql: ${TABLE}.survey_properties_delighted_browser ;;
  }

  dimension: survey_properties_delighted_device_type {
    type: string
    sql: ${TABLE}.survey_properties_delighted_device_type ;;
  }

  dimension: survey_properties_delighted_operating_system {
    type: string
    sql: ${TABLE}.survey_properties_delighted_operating_system ;;
  }

  dimension: survey_properties_delighted_source {
    type: string
    sql: ${TABLE}.survey_properties_delighted_source ;;
  }

  dimension: survey_properties_upff {
    type: string
    sql: ${TABLE}.survey_properties_upff ;;
  }

  dimension: survey_properties_user_i_d {
    type: string
    sql: ${TABLE}.survey_properties_user_i_d ;;
  }

  dimension: survey_question_answer {
    type: string
    sql: ${TABLE}.survey_question_answer ;;
  }

  dimension: survey_question_id {
    type: string
    sql: ${TABLE}.survey_question_id ;;
  }

  dimension: survey_question_name {
    type: string
    sql: ${TABLE}.survey_question_name ;;
  }

  dimension: survey_question_text {
    type: string
    sql: ${TABLE}.survey_question_text ;;
  }

  dimension: survey_tags {
    type: string
    sql: ${TABLE}.survey_tags ;;
  }

  dimension: survey_tool {
    type: string
    sql: ${TABLE}.survey_tool ;;
  }

  dimension: survey_type {
    type: string
    sql: ${TABLE}.survey_type ;;
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

  dimension: user_id {
    type: string
    tags: ["user_id"]
    sql: ${TABLE}.user_id ;;
  }

  dimension: uuid {
    type: number
    value_format_name: id
    sql: ${TABLE}.uuid ;;
  }

  dimension_group: uuid_ts {
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
    sql: ${TABLE}.uuid_ts ;;
  }


  #Get Promoters by case
  dimension: promoters {
    case: {
                  when: {
                    sql: ${TABLE}.survey_question_answer = 9 OR
                        ${TABLE}.survey_question_answer = 10 ;;
                    label: "Promoters"
                  }
                  when: {
                    sql: ${TABLE}.survey_question_answer  = 7 OR
                        ${TABLE}.survey_question_answer  = 8 ;;
                    label: "Passives"
                  }
                  when: {
                    sql: ${TABLE}.survey_question_answer <= 6 ;;
                    label: "Detractors"
                  }
                   else: "Nevers"
      }
  }

  measure: detractors {
    type: count_distinct
    sql: ${TABLE}.user_id ;;
    filters: {
      field: http_api_purchase_event.topic
      value: "customer.product.renewed,customer.product.created"
    }
    filters: {
      field: promoters # Reference fields from other joined views with view_name.field_name syntax
      value: "Detractors" # Minus sign means "not" in this case, but check notation docs for details
    }
  }

  measure: passives {
    type: count_distinct
    sql: ${TABLE}.user_id ;;
    filters: {
      field: http_api_purchase_event.topic
      value: "customer.product.renewed,customer.product.created"
    }
    filters: {
      field: promoters # Reference fields from other joined views with view_name.field_name syntax
      value: "Passives" # Minus sign means "not" in this case, but check notation docs for details
    }
  }

  measure: promoterz {
    type: count_distinct
    sql: ${TABLE}.user_id;;
    filters: {
      field: http_api_purchase_event.topic
      value: "customer.product.renewed,customer.product.created"
    }
    filters: {
      field: promoters # Reference fields from other joined views with view_name.field_name syntax
      value: "Promoters" # Minus sign means "not" in this case, but check notation docs for details
    }
  }


  measure: count {
    type: count
    drill_fields: [id, survey_question_name, context_integration_name, context_library_name, survey_name]
  }


  measure: count_distinct_userids{
    type: count_distinct
    sql: ${user_id};;
  }

}
