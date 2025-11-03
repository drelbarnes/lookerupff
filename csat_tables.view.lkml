
view: csat_tables {
  derived_table: {
    sql: with

      a1 AS
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
              , Were_your_questions_addressed_and_or_resolved_ AS q5
              , Response_Type AS response_type
              , Stage_Date__UTC_ AS stage_date
              , Start_Date__UTC_ AS start_date
              , Submit_Date__UTC_ AS submit_date
              , Tags AS tags
              , ticket_id
              , DATE(Submit_Date__UTC_) AS ds
      FROM ad_hoc.csat_survey
      )

      select
        ds
        , count(q1) as q1_n
        , count(q2) as q2_n
        , count(q3) as q3_n
        , count(q4) as q4_n
        , count(q5) as q5_n
      from a1
      group by 1 ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: ds {
    type: date
    datatype: date
    sql: ${TABLE}.ds ;;
  }

  measure: q1_n {
    type: number
    sql: ${TABLE}.q1_n ;;
  }

  measure: q2_n {
    type: number
    sql: ${TABLE}.q2_n ;;
  }

  measure: q3_n {
    type: number
    sql: ${TABLE}.q3_n ;;
  }

  measure: q4_n {
    type: number
    sql: ${TABLE}.q4_n ;;
  }

  measure: q5_n {
    type: number
    sql: ${TABLE}.q5_n ;;
  }

  set: detail {
    fields: [
      ds,
      q1_n,
      q2_n,
      q3_n,
      q4_n,
      q5_n
    ]
  }
}
