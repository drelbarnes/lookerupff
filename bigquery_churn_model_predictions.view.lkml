######################## TRAINING/TESTING INPUTS #############################
explore: churn_training_input {}
explore: churn_testing_input {}
# If necessary, uncomment the line below to include explore_source.

include: "upff_google.model.lkml"

view: churn_training_input {
  derived_table: {
    explore_source: bigquery_churn_model {
      column: customer_id {}
      column: marketing_optin {}
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
      derived_column: bates_plays_num {sql:bates_plays*(num+1);;}
      derived_column: bates_duration_num {sql:bates_duration*(num+1);;}
      derived_column: heartland_plays_num {sql:heartland_plays*(num+1);;}
      derived_column: other_plays_num {sql:other_plays*(num+1);;}
      derived_column: heartland_duration_num {sql:heartland_duration*(num+1);;}
      derived_column: other_duration_num {sql:other_duration*(num+1);;}
      expression_custom_filter: ${bigquery_churn_model.event_created_at_date} <= add_days(-9,now()) AND ${bigquery_churn_model.event_created_at_date}>=date(2018,12,28);;
    }
  }
  dimension: customer_id {
    type: string
  }
  dimension: marketing_optin {type:number}
  dimension: num {type:number}
  dimension: state {type:string}
  dimension: addwatchlist {type:number}
  dimension: bates_duration {type:number}
  dimension: bates_plays {type:number}
  dimension: churn_status {type:number}
  dimension: error {type:number}
  dimension: heartland_duration {type:number}
  dimension: heartland_plays {type:number}
  dimension: other_duration {type:number}
  dimension: other_plays {type:number}
  dimension: platform {type:string}
  dimension: removewatchlist {type:number}
  dimension: view {type:number}


}


# If necessary, uncomment the line below to include explore_source.

# include: "upff_google.model.lkml"

view: churn_testing_input {
  derived_table: {
    explore_source: bigquery_churn_model {
      column: customer_id {}
      column: marketing_optin {}
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
      derived_column: bates_plays_num {sql:bates_plays*(num+1);;}
      derived_column: bates_duration_num {sql:bates_duration*(num+1);;}
      derived_column: heartland_plays_num {sql:heartland_plays*(num+1);;}
      derived_column: other_plays_num {sql:other_plays*(num+1);;}
      derived_column: heartland_duration_num {sql:heartland_duration*(num+1);;}
      derived_column: other_duration_num {sql:other_duration*(num+1);;}
      expression_custom_filter: ${bigquery_churn_model.event_created_at_date} < now() AND ${bigquery_churn_model.event_created_at_date} > add_days(-9,now()) ;;
    }
  }
  dimension: customer_id {
    type: string
  }
  dimension: marketing_optin {type:number}
  dimension: num {type:number}
  dimension: state {type:string}
  dimension: addwatchlist {type:number}
  dimension: bates_duration {type:number}
  dimension: bates_plays {type:number}
  dimension: churn_status {type:number}
  dimension: error {type:number}
  dimension: heartland_duration {type:number}
  dimension: heartland_plays {type:number}
  dimension: other_duration {type:number}
  dimension: other_plays {type:number}
  dimension: platform {type:string}
  dimension: removewatchlist {type:number}
  dimension: view {type:number}

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
        , max_iterations = 10
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
          (SELECT * FROM ${churn_testing_input.SQL_TABLE_NAME}), struct(0.17 as threshold));;
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
        (SELECT * FROM ${churn_testing_input.SQL_TABLE_NAME}),struct(0.17 as threshold));;
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
  dimension: weight {type:number}}

# ########################################## PREDICT FUTURE ############################
explore: churn_prediction {}
view: churn_future_input {
  derived_table: {
    explore_source: bigquery_churn_model {
      column: customer_id {}
      column: marketing_optin {}
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
      derived_column: bates_plays_num {sql:bates_plays*(num+1);;}
      derived_column: bates_duration_num {sql:bates_duration*(num+1);;}
      derived_column: heartland_plays_num {sql:heartland_plays*(num+1);;}
      derived_column: other_plays_num {sql:other_plays*(num+1);;}
      derived_column: heartland_duration_num {sql:heartland_duration*(num+1);;}
      derived_column: other_duration_num {sql:other_duration*(num+1);;}

      expression_custom_filter: ${bigquery_churn_model.end_date_date} >= now() ;;
    }
  }
  dimension: customer_id {
    type: string
  }
  dimension: marketing_optin {type:number}
  dimension: num {type:number}
  dimension: state {type:string}
  dimension: addwatchlist {type:number}
  dimension: bates_duration {type:number}
  dimension: bates_plays {type:number}
  dimension: churn_status {type:number}
  dimension: error {type:number}
  dimension: heartland_duration {type:number}
  dimension: heartland_plays {type:number}
  dimension: other_duration {type:number}
  dimension: other_plays {type:number}
  dimension: platform {type:string}
  dimension: removewatchlist {type:number}
  dimension: view {type:number}

}

view: churn_prediction {
  derived_table: {
    sql: SELECT * FROM ml.PREDICT(
          MODEL ${churn_model.SQL_TABLE_NAME},
          (SELECT * FROM ${churn_future_input.SQL_TABLE_NAME}),struct(0.17 as threshold));;
  }

  dimension: customer_id {type: number}
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

  dimension: predicted_churn_status {
    type: number
    description: "Binary classification based on max predicted value"
  }
  dimension: predicted_churn_status_probability {
    value_format_name: percent_2
    type: number
    sql:  ${TABLE}.predicted_churn_status_probs[ORDINAL(1)].prob;;
  }
  measure: max_predicted_score {
    type: max
    value_format_name: percent_2
    sql: ${predicted_churn_status_probability} ;;
  }
  measure: min_predicted_score {
    type: min
    value_format_name: percent_2
    sql: ${predicted_churn_status_probability} ;;
  }
  measure: average_predicted_score {
    type: average
    value_format_name: percent_2
    sql: ${predicted_churn_status_probability} ;;
  }
  measure: median_predicted_score {
    type: median
    value_format_name: percent_2
    sql: ${predicted_churn_status_probability} ;;
  }

  measure: count {
    type: count
  }

}
