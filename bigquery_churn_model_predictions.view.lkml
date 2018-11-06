######################## TRAINING/TESTING INPUTS #############################
explore: churn_training_input {}
explore: churn_testing_input {}
# If necessary, uncomment the line below to include explore_source.

include: "upff_google.model.lkml"

view: churn_training_input {
  derived_table: {
    explore_source: bigquery_churn_model {
      column: customer_id {}
      column: num {}
      column: state {}
      column: addwatchlist {}
      column: bates_duration {}
      column: bates_plays {}
      column: churn_status {}
      column: error {}
      column: heartland_duration {}
      column: heartland_plays {}
      column: other_duration {}
      column: other_plays {}
      column: platform {}
      column: removewatchlist {}
      column: view {}
#       derived_column: bates_plays_num {sql:bates_plays*(num+1);;}
#       derived_column: bates_duration_num {sql:bates_duration*(num+1);;}
#       derived_column: heartland_plays_num {sql:heartland_plays*(num+1);;}
#       derived_column: other_plays_num {sql:other_plays*(num+1);;}
#       derived_column: heartland_duration_num {sql:heartland_duration*(num+1);;}
#       derived_column: other_duration_num {sql:other_duration*(num+1);;}
      filters: {
        field: bigquery_churn_model.event_created_at_date
        value: "2018/07/05 to 2018/10/20"
      }
    }
  }
  dimension: customer_id {
    type: number
  }
  dimension: num {
    type: number
  }
  dimension: state {}
  dimension: addwatchlist {
    type: number
  }
  dimension: bates_duration {
    type: number
  }
  dimension: bates_plays {
    type: number
  }
  dimension: churn_status {
    type: number
  }
  dimension: error {
    type: number
  }
  dimension: heartland_duration {
    type: number
  }
  dimension: heartland_plays {
    type: number
  }
  dimension: other_duration {
    type: number
  }
  dimension: other_plays {
    type: number
  }
  dimension: platform {}
  dimension: removewatchlist {
    type: number
  }
  dimension: view {
    type: number
  }
}


# If necessary, uncomment the line below to include explore_source.

# include: "upff_google.model.lkml"

view: churn_testing_input {
  derived_table: {
    explore_source: bigquery_churn_model {
      column: customer_id {}
      column: num {}
      column: max_num {}
      column: state {}
      column: addwatchlist {}
      column: bates_duration {}
      column: bates_plays {}
      column: churn_status {}
      column: error {}
      column: heartland_duration {}
      column: heartland_plays {}
      column: other_duration {}
      column: other_plays {}
      column: platform {}
      column: removewatchlist {}
      column: view {}
#       derived_column: bates_plays_num {sql:bates_plays*(num+1);;}
#       derived_column: bates_duration_num {sql:bates_duration*(num+1);;}
#       derived_column: heartland_plays_num {sql:heartland_plays*(num+1);;}
#       derived_column: other_plays_num {sql:other_plays*(num+1);;}
#       derived_column: heartland_duration_num {sql:heartland_duration*(num+1);;}
#       derived_column: other_duration_num {sql:other_duration*(num+1);;}
      filters: {
        field: bigquery_churn_model.event_created_at_date
        value: "after 15 days ago"
      }
      expression_custom_filter: ${bigquery_churn_model.max_num}=${bigquery_churn_model.num};;
    }
  }
  dimension: customer_id {
    type: number
  }
  dimension: max_num { type:number}
  dimension: num {
    type: number
  }
  dimension: state {}
  dimension: addwatchlist {
    type: number
  }
  dimension: bates_duration {
    type: number
  }
  dimension: bates_plays {
    type: number
  }
  dimension: churn_status {
    type: number
  }
  dimension: error {
    type: number
  }
  dimension: heartland_duration {
    type: number
  }
  dimension: heartland_plays {
    type: number
  }
  dimension: other_duration {
    type: number
  }
  dimension: other_plays {
    type: number
  }
  dimension: platform {}
  dimension: removewatchlist {
    type: number
  }
  dimension: view {
    type: number
  }
}

######################## MODEL #############################
view: churn_model {
  derived_table: {
    datagroup_trigger:upff_google_datagroup
    sql_create:
      CREATE OR REPLACE MODEL ${SQL_TABLE_NAME}
      OPTIONS(model_type='logistic_reg'
        , labels=['churn_status']
        , min_rel_progress = 0.00000005
        , max_iterations = 99
        ) AS
      SELECT
         * EXCEPT(customer_id)
      FROM ${churn_training_input.SQL_TABLE_NAME};;
  }
}
######################## TRAINING INFORMATION #############################
explore:  churn_model_evaluation {}
explore: churn_model_training_info {}
explore: churn_roc_curve {}
explore: churn_confusion_matrix {}
# VIEWS:
view: churn_model_evaluation {
  derived_table: {
    sql: SELECT * FROM ml.EVALUATE(
          MODEL ${churn_model.SQL_TABLE_NAME},
          (SELECT * FROM ${churn_testing_input.SQL_TABLE_NAME}), struct(0.5 as threshold));;
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
view: churn_confusion_matrix {
  derived_table: {
    sql: SELECT * FROM ml.confusion_matrix(
        MODEL ${churn_model.SQL_TABLE_NAME},
        (SELECT * FROM ${churn_testing_input.SQL_TABLE_NAME}));;
  }

  dimension: expected_label {}
  dimension: _0 {}
  dimension: _1 {}
}

view: churn_roc_curve {
  derived_table: {
    sql: SELECT * FROM ml.ROC_CURVE(
        MODEL ${churn_model.SQL_TABLE_NAME},
        (SELECT * FROM ${churn_testing_input.SQL_TABLE_NAME}));;
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
view: churn_model_training_info {
  derived_table: {
    sql: SELECT  * FROM ml.TRAINING_INFO(MODEL ${churn_model.SQL_TABLE_NAME});;
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
explore: churn_cat_weights {}
view: churn_cat_weights {
  derived_table: {
    sql: select a.*, category.category as cat, category.weight as catweight from ml.weights(
        MODEL ${churn_model.SQL_TABLE_NAME}) as a, UNNEST(category_weights) AS category
        ;;
  }

  dimension: cat{type:string}
  dimension: catweight {type:number}

}
explore: churn_weights {}
view: churn_weights {
  derived_table: {
    sql: select * from ml.weights(
      MODEL ${churn_model.SQL_TABLE_NAME});;
  }

  dimension: processed_input {type:string}
  dimension: weight {type:number}

# ########################################## PREDICT FUTURE ############################
# explore: future_purchase_prediction {}
# view: future_input {
#   derived_table: {
#     explore_source: bigquery_subscribers_v2 {
#       column: day_of_week {}
#       column: days_played {field: bigquery_conversion_model_firstplay.days_played}
#       column: customer_id {}
#       column: frequency {}
#       column: state {}
#       column: get_status {}
#       column: addwatchlist_count { field: bigquery_conversion_model_addwatchlist.addwatchlist_count }
#       column: removewatchlist_count { field: bigquery_conversion_model_removewatchlist.removewatchlist_count }
#       column: error_count { field: bigquery_conversion_model_error.error_count }
#       column: view_count { field: bigquery_conversion_model_view.view_count }
#       column: promoters { field: bigquery_delighted_survey_question_answered.promoters }
#       column: platform {}
#       column: marketing_opt_in {}
#       column: number_of_platforms {}
# #       column: bates_play { field: bigquery_conversion_model_firstplay.bates_play}
# #       column: heartland_play { field: bigquery_conversion_model_firstplay.heartland_play}
# #       column: other_play { field: bigquery_conversion_model_firstplay.other_play }
# #       column: bates_duration { field: bigquery_conversion_model_timeupdate.bates_duration }
# #       column: heartland_duration { field: bigquery_conversion_model_timeupdate.heartland_duration }
# #       column: other_duration { field: bigquery_conversion_model_timeupdate.other_duration }
# #       derived_column: bates {sql:bates_play*bates_duration;;}
# #       derived_column: heartland {sql:heartland_play*heartland_duration;;}
# #       derived_column: other {sql: other_play*other_duration;;}
#       column: bates_play_day_1 { field: bigquery_conversion_model_firstplay.bates_play_day_1 }
#       column: bates_play_day_2 { field: bigquery_conversion_model_firstplay.bates_play_day_2 }
#       column: bates_play_day_3 { field: bigquery_conversion_model_firstplay.bates_play_day_3 }
#       column: bates_play_day_4 { field: bigquery_conversion_model_firstplay.bates_play_day_4 }
#       column: heartland_play_day_1 { field: bigquery_conversion_model_firstplay.heartland_play_day_1 }
#       column: heartland_play_day_2 { field: bigquery_conversion_model_firstplay.heartland_play_day_2 }
#       column: heartland_play_day_3 { field: bigquery_conversion_model_firstplay.heartland_play_day_3 }
#       column: heartland_play_day_4 { field: bigquery_conversion_model_firstplay.heartland_play_day_4 }
#       column: other_play_day_1 { field: bigquery_conversion_model_firstplay.other_play_day_1 }
#       column: other_play_day_2 { field: bigquery_conversion_model_firstplay.other_play_day_2 }
#       column: other_play_day_3 { field: bigquery_conversion_model_firstplay.other_play_day_3 }
#       column: other_play_day_4 { field: bigquery_conversion_model_firstplay.other_play_day_4 }
#       column: bates_duration_day_1 { field: bigquery_conversion_model_timeupdate.bates_duration_day_1 }
#       column: bates_duration_day_2 { field: bigquery_conversion_model_timeupdate.bates_duration_day_2 }
#       column: bates_duration_day_3 { field: bigquery_conversion_model_timeupdate.bates_duration_day_3 }
#       column: bates_duration_day_4 { field: bigquery_conversion_model_timeupdate.bates_duration_day_4 }
# #       derived_column: bates_day_1 {sql: bates_play_day_1*bates_duration_day_1;;}
# #       derived_column: bates_day_2 {sql: bates_play_day_2*bates_duration_day_2;;}
# #       derived_column: bates_day_3 {sql: bates_play_day_3*bates_duration_day_3;;}
# #       derived_column: bates_day_4 {sql: bates_play_day_4*bates_duration_day_4;;}
#       column: heartland_duration_day_1 { field: bigquery_conversion_model_timeupdate.heartland_duration_day_1 }
#       column: heartland_duration_day_2 { field: bigquery_conversion_model_timeupdate.heartland_duration_day_2 }
#       column: heartland_duration_day_3 { field: bigquery_conversion_model_timeupdate.heartland_duration_day_3 }
#       column: heartland_duration_day_4 { field: bigquery_conversion_model_timeupdate.heartland_duration_day_4 }
# #       derived_column: heartland_day_1 {sql: heartland_play_day_1*heartland_duration_day_1;;}
# #       derived_column: heartland_day_2 {sql: heartland_play_day_2*heartland_duration_day_2;;}
# #       derived_column: heartland_day_3 {sql: heartland_play_day_3*heartland_duration_day_3;;}
# #       derived_column: heartland_day_4 {sql: heartland_play_day_4*heartland_duration_day_4;;}
#       column: other_duration_day_1 { field: bigquery_conversion_model_timeupdate.other_duration_day_1 }
#       column: other_duration_day_2 { field: bigquery_conversion_model_timeupdate.other_duration_day_2 }
#       column: other_duration_day_3 { field: bigquery_conversion_model_timeupdate.other_duration_day_3 }
#       column: other_duration_day_4 { field: bigquery_conversion_model_timeupdate.other_duration_day_4 }
# #       derived_column: other_day_1 {sql: other_play_day_1*other_duration_day_1;;}
# #       derived_column: other_day_2 {sql: other_play_day_2*other_duration_day_2;;}
# #       derived_column: other_day_3 {sql: other_play_day_3*other_duration_day_3;;}
# #       derived_column: other_day_4 {sql: other_play_day_4*other_duration_day_4;;}
#
#       expression_custom_filter: ${bigquery_subscribers_v2.subscription_length}>11 AND ${bigquery_subscribers_v2.subscription_length}<=14;;
#       filters: {
#         field: bigquery_subscribers_v2.get_status
#         value: "NULL"
#       }
#     }
#   }
#
#   dimension: count { type: number }
#   dimension: views { type: number }
#   dimension: timecode { type: number }
#   dimension: number_of_platforms_by_user { type: number }
#   dimension: addwatchlist { type: number }
#   #dimension: signin { type: number }
#   dimension: user_id {}
#   dimension: email {}
#   dimension: platform {}
#   dimension: source {}
#   dimension: frequency {}
#   dimension: day_of_week {}
#   dimension: marketing_opt_in {
#     type: number
#   }
#   dimension: state {}
#
#   dimension: promoters {}
# }
# view: future_purchase_prediction {
#   derived_table: {
#     sql: SELECT * FROM ml.PREDICT(
#           MODEL ${future_purchase_model.SQL_TABLE_NAME},
#           (SELECT * FROM ${future_input.SQL_TABLE_NAME}));;
#   }
#   dimension: day_of_week {}
#   dimension: days_played {}
#   dimension: customer_id {}
#   dimension: frequency {}
#   dimension: state {}
#   dimension: get_status {}
#   dimension: addwatchlist_count {}
#   dimension: removewatchlist_count {}
#   dimension: error_count {}
#   dimension: view_count {}
#   dimension: promoters {}
#   dimension: platform {}
#   dimension: bates_play_day_1 {}
#   dimension: bates_play_day_2 {}
#   dimension: bates_play_day_3 {}
#   dimension: bates_play_day_4 {}
#   dimension: heartland_play_day_1 {}
#   dimension: heartland_play_day_2 {}
#   dimension: heartland_play_day_3 {}
#   dimension: heartland_play_day_4 {}
#   dimension: other_play_day_1 {}
#   dimension: other_play_day_2 {}
#   dimension: other_play_day_3 {}
#   dimension: other_play_day_4 {}
#   dimension: bates_duration_day_1 {}
#   dimension: bates_duration_day_2 {}
#   dimension: bates_duration_day_3 {}
#   dimension: bates_duration_day_4 {}
#   dimension: heartland_duration_day_1 { }
#   dimension: heartland_duration_day_2 { }
#   dimension: heartland_duration_day_3 { }
#   dimension: heartland_duration_day_4 { }
#   dimension: other_duration_day_1 { }
#   dimension: other_duration_day_2 { }
#   dimension: other_duration_day_3 { }
#   dimension: other_duration_day_4 {}
#
#
#   #dimension: signin { type: number }
#   dimension: predicted_get_status {
#     type: number
#     description: "Binary classification based on max predicted value"
#   }
#   dimension: predicted_get_status_probability {
#     value_format_name: percent_2
#     type: number
#     sql:  ${TABLE}.predicted_get_status_probs[ORDINAL(1)].prob;;
#   }
#   measure: max_predicted_score {
#     type: max
#     value_format_name: percent_2
#     sql: ${predicted_get_status_probability} ;;
#   }
#   measure: average_predicted_score {
#     type: average
#     value_format_name: percent_2
#     sql: ${predicted_get_status_probability} ;;
#   }
}
