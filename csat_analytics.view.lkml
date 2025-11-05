view: csat_analytics {

    derived_table: {
      sql: with

              survey_select AS
              (
              SELECT
                _ AS id
                , brand_name
                , Comments AS verbatim
                , Ending AS ending
                , How_easy_was_it_to_get_your_issue_resolved_ AS q1
                , How_satisfied_were_you_with_the_support_you_received_ AS q2
                , How_would_you_rate_the_friendliness_and_professionalism_of_the_support_representative_ AS q3
                , Our_goal_is_to_show_care__compassion_or_respect_in_every_support_interaction___Did_your_recent_experience_feel_that_way_ AS q4
                , Response_Type AS response_type
                , Stage_Date__UTC_ AS stage_date
                , Start_Date__UTC_ AS start_date
                , Submit_Date__UTC_ AS submit_date
                , Tags AS tags
                , ticket_id
                , Were_your_questions_addressed_and_or_resolved_ AS q5
              FROM ad_hoc.csat_survey
              )

              , survey_analytics AS
              (
              SELECT
                submit_date
                , round(avg(q1), 3) AS avg_q1_scr
                , round(avg(q2), 3) AS avg_q2_scr
                , round(avg(q3), 3) AS avg_q3_scr
                , round(avg(q4), 3) AS avg_q4_scr
                , round(avg(q5), 3) AS avg_q5_scr
              FROM survey_select
              GROUP BY 1
              )

              select * from survey_analytics ;;
    }

    measure: count {
      type: count
      drill_fields: [detail*]
    }

    dimension_group: submit_date {
      type: time
      sql: ${TABLE}.submit_date ;;
    }

 #   dimension: avg_q1_scr {
 #   type: number
 #   sql: ${TABLE}.avg_q1_scr ;;
 #  }

  #  dimension: avg_q2_scr {
  #    type: number
  #    sql: ${TABLE}.avg_q2_scr ;;
  #  }

  #  dimension: avg_q3_scr {
  #    type: number
  #    sql: ${TABLE}.avg_q3_scr ;;
  #  }

  #  dimension: avg_q4_scr {
  #    type: number
  #    sql: ${TABLE}.avg_q4_scr ;;
  #  }

  #  dimension: avg_q5_scr {
  #    type: number
  #    sql: ${TABLE}.avg_q5_scr ;;
  #  }

  measure: avg_q1_scr {
    type: average
    sql: ${TABLE}.avg_q1_scr ;;
    value_format_name: decimal_2
  }

  measure: avg_q2_scr {
    type: average
    sql: ${TABLE}.avg_q2_scr ;;
    value_format_name: decimal_2
  }

  measure: avg_q3_scr {
    type: average
    sql: ${TABLE}.avg_q3_scr ;;
    value_format_name: decimal_2
  }

  measure: avg_q4_scr {
    type: average
    sql: ${TABLE}.avg_q4_scr ;;
    value_format_name: decimal_2
  }

  measure: avg_q5_scr {
    type: average
    sql: ${TABLE}.avg_q5_scr ;;
    value_format_name: decimal_2
  }

    set: detail {
      fields: [
        submit_date_time,
        avg_q1_scr,
        avg_q2_scr,
        avg_q3_scr,
        avg_q4_scr,
        avg_q5_scr
      ]
    }
  }
