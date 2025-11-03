view: csat_survey {
    derived_table: {
      sql: SELECT
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
      FROM ad_hoc.csat_survey ;;
    }

    measure: count {
      type: count
      drill_fields: [detail*]
    }

    dimension: id {
      type: string
      sql: ${TABLE}.id ;;
    }

    dimension: brand_name {
      type: string
      sql: ${TABLE}.brand_name ;;
    }

    dimension: verbatim {
      type: string
      sql: ${TABLE}.verbatim ;;
    }

    dimension: ending {
      type: string
      sql: ${TABLE}.ending ;;
    }

    measure: q1 {
      type: number
      sql: ${TABLE}.q1 ;;
    }

    measure: q2 {
      type: number
      sql: ${TABLE}.q2 ;;
    }

    measure: q3 {
      type: number
      sql: ${TABLE}.q3 ;;
    }

    measure: q4 {
      type: number
      sql: ${TABLE}.q4 ;;
    }

    dimension: response_type {
      type: string
      sql: ${TABLE}.response_type ;;
    }

    dimension: stage_date {
      type: string
      sql: ${TABLE}.stage_date ;;
    }

    dimension_group: start_date {
      type: time
      sql: ${TABLE}.start_date ;;
    }

    dimension_group: submit_date {
      type: time
      sql: ${TABLE}.submit_date ;;
    }

    dimension: tags {
      type: string
      sql: ${TABLE}.tags ;;
    }

    dimension: ticket_id {
      type: number
      sql: ${TABLE}.ticket_id ;;
    }

    measure: q5 {
      type: number
      sql: ${TABLE}.q5 ;;
    }


    set: detail {
      fields: [
        id,
        brand_name,
        verbatim,
        ending,
        q1,
        q2,
        q3,
        q4,
        response_type,
        stage_date,
        start_date_time,
        submit_date_time,
        tags,
        ticket_id,
        q5
      ]
    }
  }
