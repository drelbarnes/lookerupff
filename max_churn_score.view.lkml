view: max_churn_score {
  derived_table: {
    sql: with t1 as (
    SELECT user_id
    , email
    , churn_prediction_predicted_get_churn_probability_score as churn_score
    , original_timestamp
    , ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY churn_prediction_predicted_get_churn_probability_score DESC) as col
    -- the partitioning by user_id is required to filter out users that have multiple scores per ingestion. Conservatively, we keep the highest score.
    FROM `up-faith-and-family-216419.looker.get_churn_scores` t1
    WHERE original_timestamp = (SELECT MAX(original_timestamp) FROM `up-faith-and-family-216419.looker.get_churn_scores` t2 WHERE t1.user_id = t2.user_id)
    )
    SELECT user_id
    , email
    , churn_score
    FROM t1
    WHERE col = 1;;
  }

  dimension: user_id {
    type: number
    primary_key: yes
    tags: ["user_id"]
    sql: ${TABLE}.user_id ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: churn_score {
    type: number
    sql: ${TABLE}.churn_score ;;
  }

  dimension: email {
    type: string
    tags: ["email"]
    sql: ${TABLE}.email ;;
  }

  set: detail {
    fields: [churn_score, email]
  }
}
