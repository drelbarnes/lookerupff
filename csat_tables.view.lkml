
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
                            , EXTRACT(MONTH FROM CAST(Stage_Date__UTC_ AS DATE)) AS month_number
                    FROM ad_hoc.csat_survey
                    ),

                    a2 AS
                    (
                    SELECT
                      month_number
                      , sum(CASE WHEN q1 = 1 THEN 1 END) AS q1_1
                      , sum(CASE WHEN q1 = 2 THEN 1 END) AS q1_2
                      , sum(CASE WHEN q1 = 3 THEN 1 END) AS q1_3
                      , sum(CASE WHEN q1 = 4 THEN 1 END) AS q1_4
                      , sum(CASE WHEN q1 = 5 THEN 1 END) AS q1_5
                      , sum(CASE WHEN q2 = 1 THEN 1 END) AS q2_1
                      , sum(CASE WHEN q2 = 2 THEN 1 END) AS q2_2
                      , sum(CASE WHEN q2 = 3 THEN 1 END) AS q2_3
                      , sum(CASE WHEN q2 = 4 THEN 1 END) AS q2_4
                      , sum(CASE WHEN q2 = 5 THEN 1 END) AS q2_5
                      , sum(CASE WHEN q3 = 1 THEN 1 END) AS q3_1
                      , sum(CASE WHEN q3 = 2 THEN 1 END) AS q3_2
                      , sum(CASE WHEN q3 = 3 THEN 1 END) AS q3_3
                      , sum(CASE WHEN q3 = 4 THEN 1 END) AS q3_4
                      , sum(CASE WHEN q3 = 5 THEN 1 END) AS q3_5
                      , sum(CASE WHEN q4 = 0 THEN 1 END) AS q4_0
                      , sum(CASE WHEN q4 = 1 THEN 1 END) AS q4_1
                      , sum(CASE WHEN q5 = 0 THEN 1 END) AS q5_0
                      , sum(CASE WHEN q5 = 1 THEN 1 END) AS q5_1
                      , sum(CASE WHEN q1 is NOT NULL THEN 1 END) AS q1_total
                      , sum(CASE WHEN q2 is NOT NULL THEN 1 END) AS q2_total
                      , sum(CASE WHEN q3 is NOT NULL THEN 1 END) AS q3_total
                      , sum(CASE WHEN q4 is NOT NULL THEN 1 END) AS q4_total
                      , sum(CASE WHEN q5 is NOT NULL THEN 1 END) AS q5_total
                    FROM a1
                    GROUP BY month_number
                    )

                select * from a2 ;;
    }

    measure: count {
      type: count
      drill_fields: [detail*]
    }

    dimension: month_number {
      type: number
      sql: ${TABLE}.month_number ;;
    }

    dimension: q1_1 {
      type: number
      sql: ${TABLE}.q1_1 ;;
    }

    dimension: q1_2 {
      type: number
      sql: ${TABLE}.q1_2 ;;
    }

    dimension: q1_3 {
      type: number
      sql: ${TABLE}.q1_3 ;;
    }

    dimension: q1_4 {
      type: number
      sql: ${TABLE}.q1_4 ;;
    }

    dimension: q1_5 {
      type: number
      sql: ${TABLE}.q1_5 ;;
    }

    dimension: q2_1 {
      type: number
      sql: ${TABLE}.q2_1 ;;
    }

    dimension: q2_2 {
      type: number
      sql: ${TABLE}.q2_2 ;;
    }

    dimension: q2_3 {
      type: number
      sql: ${TABLE}.q2_3 ;;
    }

    dimension: q2_4 {
      type: number
      sql: ${TABLE}.q2_4 ;;
    }

    dimension: q2_5 {
      type: number
      sql: ${TABLE}.q2_5 ;;
    }

    dimension: q3_1 {
      type: number
      sql: ${TABLE}.q3_1 ;;
    }

    dimension: q3_2 {
      type: number
      sql: ${TABLE}.q3_2 ;;
    }

    dimension: q3_3 {
      type: number
      sql: ${TABLE}.q3_3 ;;
    }

    dimension: q3_4 {
      type: number
      sql: ${TABLE}.q3_4 ;;
    }

    dimension: q3_5 {
      type: number
      sql: ${TABLE}.q3_5 ;;
    }

    dimension: q4_0 {
      type: number
      sql: ${TABLE}.q4_0 ;;
    }

    dimension: q4_1 {
      type: number
      sql: ${TABLE}.q4_1 ;;
    }

    dimension: q5_0 {
      type: number
      sql: ${TABLE}.q5_0 ;;
    }

    dimension: q5_1 {
      type: number
      sql: ${TABLE}.q5_1 ;;
    }

    dimension: q1_total {
      type: number
      sql: ${TABLE}.q1_total ;;
    }

    dimension: q2_total {
      type: number
      sql: ${TABLE}.q2_total ;;
    }

    dimension: q3_total {
      type: number
      sql: ${TABLE}.q3_total ;;
    }

    dimension: q4_total {
      type: number
      sql: ${TABLE}.q4_total ;;
    }

    dimension: q5_total {
      type: number
      sql: ${TABLE}.q5_total ;;
    }

    set: detail {
      fields: [
        month_number,
        q1_1,
        q1_2,
        q1_3,
        q1_4,
        q1_5,
        q2_1,
        q2_2,
        q2_3,
        q2_4,
        q2_5,
        q3_1,
        q3_2,
        q3_3,
        q3_4,
        q3_5,
        q4_0,
        q4_1,
        q5_0,
        q5_1,
        q1_total,
        q2_total,
        q3_total,
        q4_total,
        q5_total
      ]
    }
  }
