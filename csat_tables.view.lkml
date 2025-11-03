
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
                      ,sum(q1) as q1_s
                      ,sum(q2) as q2_s
                      ,sum(q3) as q3_s
                      ,sum(q4) as q4_s
                      ,sum(q5) as q5_s
                    from a1
                    group by ds ;;
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

    dimension: q1_s {
      type: number
      sql: ${TABLE}.q1_s ;;
    }

    dimension: q2_s {
      type: number
      sql: ${TABLE}.q2_s ;;
    }

    dimension: q3_s {
      type: number
      sql: ${TABLE}.q3_s ;;
    }

    dimension: q4_s {
      type: number
      sql: ${TABLE}.q4_s ;;
    }

    dimension: q5_s {
      type: number
      sql: ${TABLE}.q5_s ;;
    }

    set: detail {
      fields: [
        ds,
        q1_s,
        q2_s,
        q3_s,
        q4_s,
        q5_s
      ]
    }
  }
