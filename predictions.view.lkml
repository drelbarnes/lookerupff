######################## TRAINING/TESTING INPUTS #############################
explore: training_input {}
explore: testing_input {}
# If necessary, uncomment the line below to include explore_source.

include: "upff_google.model.lkml"

view: training_input {
  derived_table: {
    explore_source: bigquery_subscribers_v2 {
      column: day_of_week {}
      column: customer_created_at_day {}
      column: days_played {field: bigquery_conversion_model_firstplay.days_played}
      column: user_id {}
      column: state {}
      column: get_status {}
      column: addwatchlist_count { field: bigquery_conversion_model_addwatchlist.addwatchlist_count }
      column: removewatchlist_count { field: bigquery_conversion_model_removewatchlist.removewatchlist_count }
      column: error_count { field: bigquery_conversion_model_error.error_count }
      column: view_count { field: bigquery_conversion_model_view.view_count }
#       column: promoters { field: bigquery_delighted_survey_question_answered.promoters }
       column: platform {}
      column: number_of_platforms {}
#       column: bates_play { field: bigquery_conversion_model_firstplay.bates_play}
#       column: heartland_play { field: bigquery_conversion_model_firstplay.heartland_play}
#       column: other_play { field: bigquery_conversion_model_firstplay.other_play }
#       column: bates_duration { field: bigquery_conversion_model_timeupdate.bates_duration }
#       column: heartland_duration { field: bigquery_conversion_model_timeupdate.heartland_duration }
#       column: other_duration { field: bigquery_conversion_model_timeupdate.other_duration }
#       derived_column: bates {sql:bates_play*bates_duration;;}
#       derived_column: heartland {sql:heartland_play*heartland_duration;;}
#       derived_column: other {sql: other_play*other_duration;;}
      column: bates_play_day_1 { field: bigquery_conversion_model_firstplay.bates_play_day_1 }
      column: bates_play_day_2 { field: bigquery_conversion_model_firstplay.bates_play_day_2 }
      column: bates_play_day_3 { field: bigquery_conversion_model_firstplay.bates_play_day_3 }
      column: bates_play_day_4 { field: bigquery_conversion_model_firstplay.bates_play_day_4 }
      column: heartland_play_day_1 { field: bigquery_conversion_model_firstplay.heartland_play_day_1 }
      column: heartland_play_day_2 { field: bigquery_conversion_model_firstplay.heartland_play_day_2 }
      column: heartland_play_day_3 { field: bigquery_conversion_model_firstplay.heartland_play_day_3 }
      column: heartland_play_day_4 { field: bigquery_conversion_model_firstplay.heartland_play_day_4 }
      column: other_play_day_1 { field: bigquery_conversion_model_firstplay.other_play_day_1 }
      column: other_play_day_2 { field: bigquery_conversion_model_firstplay.other_play_day_2 }
      column: other_play_day_3 { field: bigquery_conversion_model_firstplay.other_play_day_3 }
      column: other_play_day_4 { field: bigquery_conversion_model_firstplay.other_play_day_4 }
      column: bates_duration_day_1 { field: bigquery_conversion_model_timeupdate.bates_duration_day_1 }
      column: bates_duration_day_2 { field: bigquery_conversion_model_timeupdate.bates_duration_day_2 }
      column: bates_duration_day_3 { field: bigquery_conversion_model_timeupdate.bates_duration_day_3 }
      column: bates_duration_day_4 { field: bigquery_conversion_model_timeupdate.bates_duration_day_4 }
#       derived_column: bates_day_1 {sql: bates_play_day_1*bates_duration_day_1;;}
#       derived_column: bates_day_2 {sql: bates_play_day_2*bates_duration_day_2;;}
#       derived_column: bates_day_3 {sql: bates_play_day_3*bates_duration_day_3;;}
#       derived_column: bates_day_4 {sql: bates_play_day_4*bates_duration_day_4;;}
      column: heartland_duration_day_1 { field: bigquery_conversion_model_timeupdate.heartland_duration_day_1 }
      column: heartland_duration_day_2 { field: bigquery_conversion_model_timeupdate.heartland_duration_day_2 }
      column: heartland_duration_day_3 { field: bigquery_conversion_model_timeupdate.heartland_duration_day_3 }
      column: heartland_duration_day_4 { field: bigquery_conversion_model_timeupdate.heartland_duration_day_4 }
#       derived_column: heartland_day_1 {sql: heartland_play_day_1*heartland_duration_day_1;;}
#       derived_column: heartland_day_2 {sql: heartland_play_day_2*heartland_duration_day_2;;}
#       derived_column: heartland_day_3 {sql: heartland_play_day_3*heartland_duration_day_3;;}
#       derived_column: heartland_day_4 {sql: heartland_play_day_4*heartland_duration_day_4;;}
      column: other_duration_day_1 { field: bigquery_conversion_model_timeupdate.other_duration_day_1 }
      column: other_duration_day_2 { field: bigquery_conversion_model_timeupdate.other_duration_day_2 }
      column: other_duration_day_3 { field: bigquery_conversion_model_timeupdate.other_duration_day_3 }
      column: other_duration_day_4 { field: bigquery_conversion_model_timeupdate.other_duration_day_4 }
#       derived_column: other_day_1 {sql: other_play_day_1*other_duration_day_1;;}
#       derived_column: other_day_2 {sql: other_play_day_2*other_duration_day_2;;}
#       derived_column: other_day_3 {sql: other_play_day_3*other_duration_day_3;;}
#       derived_column: other_day_4 {sql: other_play_day_4*other_duration_day_4;;}


      expression_custom_filter: ${bigquery_subscribers_v2.customer_created_date}<=add_days(-18,now()) AND ${bigquery_subscribers_v2.customer_created_date}>=date(2018,12,14);;
    }
  }

}

# If necessary, uncomment the line below to include explore_source.
# include: "upff_google.model.lkml"

view: testing_input {
  derived_table: {
    explore_source: bigquery_subscribers_v2 {
      column: day_of_week {}
      column: customer_created_at_day {}
      column: days_played {field: bigquery_conversion_model_firstplay.days_played}
      column: user_id {}
      column: state {}
      column: get_status {}
      column: addwatchlist_count { field: bigquery_conversion_model_addwatchlist.addwatchlist_count }
      column: removewatchlist_count { field: bigquery_conversion_model_removewatchlist.removewatchlist_count }
      column: error_count { field: bigquery_conversion_model_error.error_count }
      column: view_count { field: bigquery_conversion_model_view.view_count }
      column: platform {}
      column: number_of_platforms {}
#       column: bates_play { field: bigquery_conversion_model_firstplay.bates_play}
#       column: heartland_play { field: bigquery_conversion_model_firstplay.heartland_play}
#       column: other_play { field: bigquery_conversion_model_firstplay.other_play }
#       column: bates_duration { field: bigquery_conversion_model_timeupdate.bates_duration }
#       column: heartland_duration { field: bigquery_conversion_model_timeupdate.heartland_duration }
#       column: other_duration { field: bigquery_conversion_model_timeupdate.other_duration }
#       derived_column: bates {sql:bates_play*bates_duration;;}
#       derived_column: heartland {sql:heartland_play*heartland_duration;;}
#       derived_column: other {sql: other_play*other_duration;;}
      column: bates_play_day_1 { field: bigquery_conversion_model_firstplay.bates_play_day_1 }
      column: bates_play_day_2 { field: bigquery_conversion_model_firstplay.bates_play_day_2 }
      column: bates_play_day_3 { field: bigquery_conversion_model_firstplay.bates_play_day_3 }
      column: bates_play_day_4 { field: bigquery_conversion_model_firstplay.bates_play_day_4 }
      column: heartland_play_day_1 { field: bigquery_conversion_model_firstplay.heartland_play_day_1 }
      column: heartland_play_day_2 { field: bigquery_conversion_model_firstplay.heartland_play_day_2 }
      column: heartland_play_day_3 { field: bigquery_conversion_model_firstplay.heartland_play_day_3 }
      column: heartland_play_day_4 { field: bigquery_conversion_model_firstplay.heartland_play_day_4 }
      column: other_play_day_1 { field: bigquery_conversion_model_firstplay.other_play_day_1 }
      column: other_play_day_2 { field: bigquery_conversion_model_firstplay.other_play_day_2 }
      column: other_play_day_3 { field: bigquery_conversion_model_firstplay.other_play_day_3 }
      column: other_play_day_4 { field: bigquery_conversion_model_firstplay.other_play_day_4 }
      column: bates_duration_day_1 { field: bigquery_conversion_model_timeupdate.bates_duration_day_1 }
      column: bates_duration_day_2 { field: bigquery_conversion_model_timeupdate.bates_duration_day_2 }
      column: bates_duration_day_3 { field: bigquery_conversion_model_timeupdate.bates_duration_day_3 }
      column: bates_duration_day_4 { field: bigquery_conversion_model_timeupdate.bates_duration_day_4 }
#       derived_column: bates_day_1 {sql: bates_play_day_1*bates_duration_day_1;;}
#       derived_column: bates_day_2 {sql: bates_play_day_2*bates_duration_day_2;;}
#       derived_column: bates_day_3 {sql: bates_play_day_3*bates_duration_day_3;;}
#       derived_column: bates_day_4 {sql: bates_play_day_4*bates_duration_day_4;;}
      column: heartland_duration_day_1 { field: bigquery_conversion_model_timeupdate.heartland_duration_day_1 }
      column: heartland_duration_day_2 { field: bigquery_conversion_model_timeupdate.heartland_duration_day_2 }
      column: heartland_duration_day_3 { field: bigquery_conversion_model_timeupdate.heartland_duration_day_3 }
      column: heartland_duration_day_4 { field: bigquery_conversion_model_timeupdate.heartland_duration_day_4 }
#       derived_column: heartland_day_1 {sql: heartland_play_day_1*heartland_duration_day_1;;}
#       derived_column: heartland_day_2 {sql: heartland_play_day_2*heartland_duration_day_2;;}
#       derived_column: heartland_day_3 {sql: heartland_play_day_3*heartland_duration_day_3;;}
#       derived_column: heartland_day_4 {sql: heartland_play_day_4*heartland_duration_day_4;;}
      column: other_duration_day_1 { field: bigquery_conversion_model_timeupdate.other_duration_day_1 }
      column: other_duration_day_2 { field: bigquery_conversion_model_timeupdate.other_duration_day_2 }
      column: other_duration_day_3 { field: bigquery_conversion_model_timeupdate.other_duration_day_3 }
      column: other_duration_day_4 { field: bigquery_conversion_model_timeupdate.other_duration_day_4 }
#       derived_column: other_day_1 {sql: other_play_day_1*other_duration_day_1;;}
#       derived_column: other_day_2 {sql: other_play_day_2*other_duration_day_2;;}
#       derived_column: other_day_3 {sql: other_play_day_3*other_duration_day_3;;}
#       derived_column: other_day_4 {sql: other_play_day_4*other_duration_day_4;;}

      expression_custom_filter: ${bigquery_subscribers_v2.customer_created_date}<add_days(-15,now()) AND ${bigquery_subscribers_v2.customer_created_date}>add_days(-18,now());;
    }
  }

}

######################## MODEL #############################
view: future_purchase_model {
  derived_table: {
    datagroup_trigger:upff_google_datagroup
    sql_create:
      CREATE OR REPLACE MODEL ${SQL_TABLE_NAME}
      OPTIONS(model_type='logistic_reg'
        , labels=['get_status']
        , min_rel_progress = 0.00000005
        , max_iterations = 40
        ) AS
      SELECT
         * EXCEPT(user_id)
      FROM ${training_input.SQL_TABLE_NAME};;
  }
}
######################## TRAINING INFORMATION #############################
explore:  future_purchase_model_evaluation {}
explore: future_purchase_model_training_info {}
explore: roc_curve {}
explore: confusion_matrix {}
# VIEWS:
view: future_purchase_model_evaluation {
  derived_table: {
    sql: SELECT * FROM ml.EVALUATE(
          MODEL ${future_purchase_model.SQL_TABLE_NAME},
          (SELECT * FROM ${testing_input.SQL_TABLE_NAME}), struct(0.46 as threshold));;
  }
  dimension: recall {
    type: number
    value_format_name:percent_2
    description: "How false positives/negatives are penalized. True positives over all positives."
  }
  dimension: accuracy {type: number value_format_name:percent_2}
  ### Accuracy of the model evaluations ###
  dimension: f1_score {type: number value_format_name:percent_3}
  dimension: log_loss {type: number}
  dimension: roc_auc {type: number}
}
view: confusion_matrix {
  derived_table: {
    sql: SELECT * FROM ml.confusion_matrix(
        MODEL ${future_purchase_model.SQL_TABLE_NAME},
        (SELECT * FROM ${testing_input.SQL_TABLE_NAME}),struct(0.46 as threshold));;
  }

  dimension: expected_label {}
  dimension: _0 {}
  dimension: _1 {}
  }

view: roc_curve {
  derived_table: {
    sql: SELECT * FROM ml.ROC_CURVE(
        MODEL ${future_purchase_model.SQL_TABLE_NAME},
        (SELECT * FROM ${testing_input.SQL_TABLE_NAME}));;
  }
  dimension: threshold {
    type: number
    value_format_name: decimal_4
  }
  dimension: recall {type: number value_format_name: percent_2}
  dimension: false_positive_rate {type: number}
  dimension: true_positives {type: number }
  dimension: false_positives {type: number}
  dimension: true_negatives {type: number}
  dimension: false_negatives {type: number }
  dimension: precision {
    type:  number
    value_format_name: percent_2
    sql:  ${true_positives} / NULLIF((${true_positives} + ${false_positives}),0);;
    description: "Equal to true positives over all positives. Indicative of how false positives are penalized. Set high to get no false positives"
  }
  measure: total_false_positives {
    type: sum
    sql: ${false_positives} ;;
  }
  measure: total_true_positives {
    type: sum
    sql: ${true_positives} ;;
  }
  dimension: threshold_accuracy {
    type: number
    value_format_name: percent_2
    sql:  1.0*(${true_positives} + ${true_negatives}) / NULLIF((${true_positives} + ${true_negatives} + ${false_positives} + ${false_negatives}),0);;
  }
  dimension: threshold_f1 {
    type: number
    value_format_name: percent_3
    sql: 2.0*${recall}*${precision} / NULLIF((${recall}+${precision}),0);;
  }
}
view: future_purchase_model_training_info {
  derived_table: {
    sql: SELECT  * FROM ml.TRAINING_INFO(MODEL ${future_purchase_model.SQL_TABLE_NAME});;
  }
  dimension: training_run {type: number}
  dimension: iteration {type: number}
  dimension: loss_raw {sql: ${TABLE}.loss;; type: number hidden:yes}
  dimension: eval_loss {type: number}
  dimension: duration_ms {label:"Duration (ms)" type: number}
  dimension: learning_rate {type: number}
  measure: total_iterations {
    type: count
  }
  measure: loss {
    value_format_name: decimal_2
    type: sum
    sql:  ${loss_raw} ;;
  }
  measure: total_training_time {
    type: sum
    label:"Total Training Time (sec)"
    sql: ${duration_ms}/1000 ;;
    value_format_name: decimal_1
  }
  measure: average_iteration_time {
    type: average
    label:"Average Iteration Time (sec)"
    sql: ${duration_ms}/1000 ;;
    value_format_name: decimal_1
  }
}

# ############################################ WEIGHTS #################################
explore: cat_weights {}
view: cat_weights {
  derived_table: {
    sql: select a.*, category.category as cat, category.weight as catweight from ml.weights(
        MODEL ${future_purchase_model.SQL_TABLE_NAME}) as a, UNNEST(category_weights) AS category
        ;;
  }

  dimension: cat{type:string}
  dimension: catweight {type:number}

}
explore: weights {}
view: weights {
  derived_table: {
    sql: select * from ml.weights(
      MODEL ${future_purchase_model.SQL_TABLE_NAME});;
  }

  dimension: processed_input {type:string}
  dimension: weight {type:number}
}
########################################## PREDICT FUTURE ############################
explore: future_purchase_prediction {}
view: future_input {
  derived_table: {
  explore_source: bigquery_subscribers_v2 {
    column: day_of_week {}
    column: customer_created_at_day {}
    column: days_played {field: bigquery_conversion_model_firstplay.days_played}
    column: user_id {}
    column: email {}
    column: moptin {}
    column: state {}
    column: get_status {}
    column: addwatchlist_count { field: bigquery_conversion_model_addwatchlist.addwatchlist_count }
    column: removewatchlist_count { field: bigquery_conversion_model_removewatchlist.removewatchlist_count }
    column: error_count { field: bigquery_conversion_model_error.error_count }
    column: view_count { field: bigquery_conversion_model_view.view_count }
    column: platform {}
    column: number_of_platforms {}
#       column: bates_play { field: bigquery_conversion_model_firstplay.bates_play}
#       column: heartland_play { field: bigquery_conversion_model_firstplay.heartland_play}
#       column: other_play { field: bigquery_conversion_model_firstplay.other_play }
#       column: bates_duration { field: bigquery_conversion_model_timeupdate.bates_duration }
#       column: heartland_duration { field: bigquery_conversion_model_timeupdate.heartland_duration }
#       column: other_duration { field: bigquery_conversion_model_timeupdate.other_duration }
#       derived_column: bates {sql:bates_play*bates_duration;;}
#       derived_column: heartland {sql:heartland_play*heartland_duration;;}
#       derived_column: other {sql: other_play*other_duration;;}
    column: bates_play_day_1 { field: bigquery_conversion_model_firstplay.bates_play_day_1 }
    column: bates_play_day_2 { field: bigquery_conversion_model_firstplay.bates_play_day_2 }
    column: bates_play_day_3 { field: bigquery_conversion_model_firstplay.bates_play_day_3 }
    column: bates_play_day_4 { field: bigquery_conversion_model_firstplay.bates_play_day_4 }
    column: heartland_play_day_1 { field: bigquery_conversion_model_firstplay.heartland_play_day_1 }
    column: heartland_play_day_2 { field: bigquery_conversion_model_firstplay.heartland_play_day_2 }
    column: heartland_play_day_3 { field: bigquery_conversion_model_firstplay.heartland_play_day_3 }
    column: heartland_play_day_4 { field: bigquery_conversion_model_firstplay.heartland_play_day_4 }
    column: other_play_day_1 { field: bigquery_conversion_model_firstplay.other_play_day_1 }
    column: other_play_day_2 { field: bigquery_conversion_model_firstplay.other_play_day_2 }
    column: other_play_day_3 { field: bigquery_conversion_model_firstplay.other_play_day_3 }
    column: other_play_day_4 { field: bigquery_conversion_model_firstplay.other_play_day_4 }
    column: bates_duration_day_1 { field: bigquery_conversion_model_timeupdate.bates_duration_day_1 }
    column: bates_duration_day_2 { field: bigquery_conversion_model_timeupdate.bates_duration_day_2 }
    column: bates_duration_day_3 { field: bigquery_conversion_model_timeupdate.bates_duration_day_3 }
    column: bates_duration_day_4 { field: bigquery_conversion_model_timeupdate.bates_duration_day_4 }
#       derived_column: bates_day_1 {sql: bates_play_day_1*bates_duration_day_1;;}
#       derived_column: bates_day_2 {sql: bates_play_day_2*bates_duration_day_2;;}
#       derived_column: bates_day_3 {sql: bates_play_day_3*bates_duration_day_3;;}
#       derived_column: bates_day_4 {sql: bates_play_day_4*bates_duration_day_4;;}
    column: heartland_duration_day_1 { field: bigquery_conversion_model_timeupdate.heartland_duration_day_1 }
    column: heartland_duration_day_2 { field: bigquery_conversion_model_timeupdate.heartland_duration_day_2 }
    column: heartland_duration_day_3 { field: bigquery_conversion_model_timeupdate.heartland_duration_day_3 }
    column: heartland_duration_day_4 { field: bigquery_conversion_model_timeupdate.heartland_duration_day_4 }
#       derived_column: heartland_day_1 {sql: heartland_play_day_1*heartland_duration_day_1;;}
#       derived_column: heartland_day_2 {sql: heartland_play_day_2*heartland_duration_day_2;;}
#       derived_column: heartland_day_3 {sql: heartland_play_day_3*heartland_duration_day_3;;}
#       derived_column: heartland_day_4 {sql: heartland_play_day_4*heartland_duration_day_4;;}
    column: other_duration_day_1 { field: bigquery_conversion_model_timeupdate.other_duration_day_1 }
    column: other_duration_day_2 { field: bigquery_conversion_model_timeupdate.other_duration_day_2 }
    column: other_duration_day_3 { field: bigquery_conversion_model_timeupdate.other_duration_day_3 }
    column: other_duration_day_4 { field: bigquery_conversion_model_timeupdate.other_duration_day_4 }
#       derived_column: other_day_1 {sql: other_play_day_1*other_duration_day_1;;}
#       derived_column: other_day_2 {sql: other_play_day_2*other_duration_day_2;;}
#       derived_column: other_day_3 {sql: other_play_day_3*other_duration_day_3;;}
#       derived_column: other_day_4 {sql: other_play_day_4*other_duration_day_4;;}

    expression_custom_filter: ${bigquery_subscribers_v2.customer_created_date}<add_days(-8,now()) AND ${bigquery_subscribers_v2.customer_created_date}>=add_days(-15,now());;
  }
  }

  dimension: count { type: number }
  dimension: views { type: number }
  dimension: timecode { type: number }
  dimension: number_of_platforms_by_user { type: number }
  dimension: addwatchlist { type: number }
  #dimension: signin { type: number }
  dimension: user_id {
    tags: ["user_id"]
    type: number
  }
  dimension: email {
    tags: ["email"]
    type:string
  }
  dimension: platform {}
  dimension: source {}
  dimension: frequency {}
  dimension: day_of_week {}
  dimension: moptin {
    type: yesno
  }
  dimension: state {}
  dimension: promoters {}
}
view: future_purchase_prediction {
  derived_table: {
    sql: SELECT * FROM ml.PREDICT(
          MODEL ${future_purchase_model.SQL_TABLE_NAME},
          (SELECT * FROM ${future_input.SQL_TABLE_NAME}));;
  }
  dimension: day_of_week {}
  dimension: days_played {}
  dimension: user_id {
    tags: ["user_id"]
    type: number
  }
  dimension: email {
    tags: ["email"]
    type: string
  }
  dimension: moptin {
    type: yesno
  }
  dimension: frequency {}
  dimension: state {}
  dimension: get_status {}
  dimension: addwatchlist_count {}
  dimension: removewatchlist_count {}
  dimension: error_count {}
  dimension: view_count {}
  dimension: promoters {}
  dimension: platform {}
  dimension: bates_play_day_1 {}
  dimension: bates_play_day_2 {}
  dimension: bates_play_day_3 {}
  dimension: bates_play_day_4 {}
  dimension: heartland_play_day_1 {}
  dimension: heartland_play_day_2 {}
  dimension: heartland_play_day_3 {}
  dimension: heartland_play_day_4 {}
  dimension: other_play_day_1 {}
  dimension: other_play_day_2 {}
  dimension: other_play_day_3 {}
  dimension: other_play_day_4 {}
  dimension: bates_duration_day_1 {}
  dimension: bates_duration_day_2 {}
  dimension: bates_duration_day_3 {}
  dimension: bates_duration_day_4 {}
  dimension: heartland_duration_day_1 { }
  dimension: heartland_duration_day_2 { }
  dimension: heartland_duration_day_3 { }
  dimension: heartland_duration_day_4 { }
  dimension: other_duration_day_1 { }
  dimension: other_duration_day_2 { }
  dimension: other_duration_day_3 { }
  dimension: other_duration_day_4 {}


  #dimension: signin { type: number }
  dimension: predicted_get_status {
    type: number
    description: "Binary classification based on max predicted value"
  }
  dimension: predicted_get_status_probability {
    value_format_name: percent_2
    type: number
    sql:  ${TABLE}.predicted_get_status_probs[ORDINAL(1)].prob;;
  }

  dimension: predicted_get_probability_score{
    #value_format_name: id
    #value_format: "0"
    type: number
    sql:  CAST(${TABLE}.predicted_get_status_probs[ORDINAL(1)].prob * 100 AS INT64);;
  }

  dimension: predicted_probability {
    type: number
    sql:  ${predicted_get_status_probability};;
  }

  measure: max_predicted_score {
    type: max
    value_format_name: percent_2
    sql: ${predicted_get_status_probability} ;;
  }

  measure: average_predicted_score {
    type: average
    value_format_name: percent_2
    sql: ${predicted_get_status_probability} ;;
  }

  measure: average_predicted_score_of_trialist {
    type: average
    value_format_name: decimal_2
    sql: ${predicted_get_status_probability} * 100 ;;
  }


  measure: count_userId {
    type: count_distinct
    sql:  ${user_id} ;;
  }

}
