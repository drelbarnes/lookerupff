view: csat_analytics {
    derived_table: {
      sql: with

              survey_select AS
              (
              SELECT
                _ AS id
                , brand_name
                , Comment AS verbatim
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
                format_timestamp('%m-%Y', TIMESTAMP(submit_date)) AS month_year
                , round(avg(q1), 2) AS avg_q1_scr
                , round(avg(q2), 2) AS avg_q2_scr
                , round(avg(q3), 2) AS avg_q3_scr
                , round(avg(q4), 2) AS avg_q4_scr
                , round(avg(q5), 2) AS avg_q5_scr
              FROM survey_select
              GROUP BY 1
              )

              select * from survey_analytics ;;
    }

    measure: count {
      type: count
      drill_fields: [detail*]
    }

    dimension: month_year {
      type: string
      sql: ${TABLE}.month_year ;;
    }

    dimension: avg_q1_scr {
      type: number
      sql: ${TABLE}.avg_q1_scr ;;
    }

    dimension: avg_q2_scr {
      type: number
      sql: ${TABLE}.avg_q2_scr ;;
    }

    dimension: avg_q3_scr {
      type: number
      sql: ${TABLE}.avg_q3_scr ;;
    }

    dimension: avg_q4_scr {
      type: number
      sql: ${TABLE}.avg_q4_scr ;;
    }

    dimension: avg_q5_scr {
      type: number
      sql: ${TABLE}.avg_q5_scr ;;
    }

    set: detail {
      fields: [
        month_year,
        avg_q1_scr,
        avg_q2_scr,
        avg_q3_scr,
        avg_q4_scr,
        avg_q5_scr
      ]
    }
  }
