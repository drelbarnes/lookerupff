view: max_churn_score {
  derived_table: {
    sql: select max(churn_prediction_predicted_get_churn_probability_score) as churn_score, email, user_id from `up-faith-and-family-216419.looker.get_churn_scores` GROUP BY 2,3
      ;;
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
