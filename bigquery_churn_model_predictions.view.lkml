######################## TRAINING/TESTING INPUTS #############################
explore: churn_training_input {}
explore: churn_testing_input {}

# Do NOT include the model file from here. This file is already included BY the
# model, so including the model back creates a circular include and a validation error.
# include: "upff_google.model.lkml"

view: churn_training_input {
  derived_table: {
    # No datagroup trigger: persist_for has no trigger check, so nothing fires on a schedule.
    # The table is only rebuilt when someone queries it after it has expired.
    persist_for: "8760 hours"
    explore_source: bigquery_churn_model {
      column: customer_id {}
      column: email_open {}
      column: marketing_optin {}
      column: num {}
      column: state {}
      column: bates_duration {}
      column: churn_status {}
      column: error {}
      column: heartland_duration {}
      column: other_duration {}
      column: platform {}
      column: view {}
      column: month_day {}
      column: one_week_duration {}
      column: two_week_duration {}
      column: three_week_duration {}
      column: four_week_duration {}
      column: set_cancel {}
      column: undo_cancel {}
      column: charge_failed {}
      expression_custom_filter: (${bigquery_churn_model.event_created_at_date} < add_days(-1,now()) AND ${bigquery_churn_model.event_created_at_date} >= add_days(-30,now())) AND ${bigquery_churn_model.random} <= 0.7 ;;
    }
  }
  dimension: customer_id {type: string}
  dimension: marketing_optin {type: number}
  dimension: num {type: number}
  dimension: state {type: string}
  dimension: email_open {type: number}
  dimension: bates_duration {type: number}
  dimension: churn_status {type: number}
  dimension: error {type: number}
  dimension: heartland_duration {type: number}
  dimension: other_duration {type: number}
  dimension: platform {type: string}
  dimension: view {type: number}
  dimension: month_day {type: number}
  dimension: one_week_duration {type: number}
  dimension: two_week_duration {type: number}
  dimension: three_week_duration {type: number}
  dimension: four_week_duration {type: number}
  dimension: set_cancel {type: number}
  dimension: undo_cancel {type: number}
  dimension: charge_failed {type: number}
}

view: churn_testing_input {
  derived_table: {
    persist_for: "8760 hours"
    explore_source: bigquery_churn_model {
      column: customer_id {}
      column: end_date2 {}
      column: marketing_optin {}
      column: num {}
      column: state {}
      column: email_open {}
      column: bates_duration {}
      column: churn_status {}
      column: error {}
      column: heartland_duration {}
      column: other_duration {}
      column: platform {}
      column: view {}
      column: month_day {}
      column: one_week_duration {}
      column: two_week_duration {}
      column: three_week_duration {}
      column: four_week_duration {}
      column: set_cancel {}
      column: undo_cancel {}
      column: charge_failed {}
      expression_custom_filter: (${bigquery_churn_model.event_created_at_date} < add_days(-1,now()) AND ${bigquery_churn_model.event_created_at_date} >= add_days(-31,now())) AND ${bigquery_churn_model.random} > 0.7 ;;
    }
  }
  dimension: customer_id {type: string}
  dimension: marketing_optin {type: number}
  dimension: num {type: number}
  dimension: state {type: string}
  dimension: email_open {type: number}
  dimension: bates_duration {type: number}
  dimension: churn_status {type: number}
  dimension: error {type: number}
  dimension: heartland_duration {type: number}
  dimension: other_duration {type: number}
  dimension: platform {type: string}
  dimension: view {type: number}
  dimension: month_day {type: number}
  dimension: one_week_duration {type: number}
  dimension: two_week_duration {type: number}
  dimension: three_week_duration {type: number}
  dimension: four_week_duration {type: number}
  dimension: set_cancel {type: number}
  dimension: undo_cancel {type: number}
  dimension: charge_failed {type: number}
}

######################## MODEL #############################
view: churn_model {
  derived_table: {
    # sql_create derived tables must be persistent, otherwise Looker errors.
    # persist_for satisfies that without any trigger.
    persist_for: "8760 hours"
    sql_create:
      CREATE OR REPLACE MODEL ${SQL_TABLE_NAME}
      OPTIONS(model_type='logistic_reg'
        , labels=['churn_status']
        , min_rel_progress = 0.0005
        , max_iterations = 20
        ) AS
      SELECT
         * EXCEPT(customer_id)
      FROM ${churn_training_input.SQL_TABLE_NAME} ;;
  }
}

######################## TRAINING INFORMATION #############################
explore: churn_model_evaluation {}
explore: churn_model_training_info {}
explore: churn_roc_curve {}
explore: churn_confusion_matrix {}

view: churn_model_evaluation {
  derived_table: {
    sql: SELECT * FROM ml.EVALUATE(
          MODEL ${churn_model.SQL_TABLE_NAME},
          (SELECT * FROM ${churn_testing_input.SQL_TABLE_NAME}), STRUCT(0.17 AS threshold)) ;;
  }
  dimension: recall {
    type: number
    value_format_name: percent_2
    description: "True positives over all actual positives."
  }
  dimension: accuracy {type: number value_format_name: percent_2}
  dimension: f1_score {type: number value_format_name: percent_3}
  dimension: log_loss {type: number}
  dimension: roc_auc {type: number}
}

view: churn_confusion_matrix {
  derived_table: {
    sql: SELECT * FROM ml.CONFUSION_MATRIX(
        MODEL ${churn_model.SQL_TABLE_NAME},
        (SELECT * FROM ${churn_testing_input.SQL_TABLE_NAME}), STRUCT(0.16 AS threshold)) ;;
  }
  dimension: expected_label {}
  dimension: _0 {}
  dimension: _1 {}
}

view: churn_roc_curve {
  derived_table: {
    sql: SELECT * FROM ml.ROC_CURVE(
        MODEL ${churn_model.SQL_TABLE_NAME},
        (SELECT * FROM ${churn_testing_input.SQL_TABLE_NAME})) ;;
  }
  dimension: threshold {type: number value_format_name: decimal_4}
  dimension: recall {type: number value_format_name: percent_2}
  dimension: false_positive_rate {type: number}
  dimension: true_positives {type: number}
  dimension: false_positives {type: number}
  dimension: true_negatives {type: number}
  dimension: false_negatives {type: number}
  dimension: precision {
    type: number
    value_format_name: percent_2
    sql: ${true_positives} / NULLIF((${true_positives} + ${false_positives}),0) ;;
    description: "True positives over all predicted positives."
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
    sql: 1.0*(${true_positives} + ${true_negatives}) / NULLIF((${true_positives} + ${true_negatives} + ${false_positives} + ${false_negatives}),0) ;;
  }
  dimension: threshold_f1 {
    type: number
    value_format_name: percent_3
    sql: 2.0*${recall}*${precision} / NULLIF((${recall}+${precision}),0) ;;
  }
}

view: churn_model_training_info {
  derived_table: {
    sql: SELECT * FROM ml.TRAINING_INFO(MODEL ${churn_model.SQL_TABLE_NAME}) ;;
  }
  dimension: training_run {type: number}
  dimension: iteration {type: number}
  dimension: loss_raw {sql: ${TABLE}.loss ;; type: number hidden: yes}
  dimension: eval_loss {type: number}
  dimension: duration_ms {label: "Duration (ms)" type: number}
  dimension: learning_rate {type: number}
  measure: total_iterations {type: count}
  measure: loss {
    value_format_name: decimal_2
    type: sum
    sql: ${loss_raw} ;;
  }
  measure: total_training_time {
    type: sum
    label: "Total Training Time (sec)"
    sql: ${duration_ms}/1000 ;;
    value_format_name: decimal_1
  }
  measure: average_iteration_time {
    type: average
    label: "Average Iteration Time (sec)"
    sql: ${duration_ms}/1000 ;;
    value_format_name: decimal_1
  }
}

############################################# WEIGHTS #################################
explore: churn_cat_weights {}
view: churn_cat_weights {
  derived_table: {
    sql: SELECT a.*, category.category AS cat, category.weight AS catweight
         FROM ml.WEIGHTS(MODEL ${churn_model.SQL_TABLE_NAME}) AS a,
         UNNEST(category_weights) AS category ;;
  }
  dimension: cat {type: string}
  dimension: catweight {type: number}
}

explore: churn_weights {}
view: churn_weights {
  derived_table: {
    sql: SELECT * FROM ml.WEIGHTS(MODEL ${churn_model.SQL_TABLE_NAME}) ;;
  }
  dimension: processed_input {type: string}
  dimension: weight {type: number}
}

########################################### PREDICT FUTURE ############################
explore: churn_prediction {}

view: churn_future_input {
  derived_table: {
    explore_source: bigquery_churn_model {
      column: customer_id {}
      column: marketing_optin {}
      column: num {}
      column: email {}
      column: state {}
      column: bates_duration {}
      column: churn_status {}
      column: error {}
      column: heartland_duration {}
      column: other_duration {}
      column: platform {}
      column: view {}
      column: month_day {}
      column: one_week_duration {}
      column: two_week_duration {}
      column: three_week_duration {}
      column: four_week_duration {}
      column: set_cancel {}
      column: undo_cancel {}
      column: charge_failed {}
      column: email_open {}
      expression_custom_filter: ${bigquery_churn_model.end_date2} >= now() ;;
    }
  }
  dimension: customer_id {
    type: string
    tags: ["user_id"]
  }
  dimension: email {
    type: string
    tags: ["email"]
  }
  dimension: marketing_optin {type: number}
  dimension: num {type: number}
  dimension: state {type: string}
  dimension: bates_duration {type: number}
  dimension: churn_status {type: number}
  dimension: error {type: number}
  dimension: email_open {type: number}
  dimension: heartland_duration {type: number}
  dimension: other_duration {type: number}
  dimension: platform {type: string}
  dimension: view {type: number}
  dimension: month_day {type: number}
  dimension: one_week_duration {type: number}
  dimension: two_week_duration {type: number}
  dimension: three_week_duration {type: number}
  dimension: four_week_duration {type: number}
  dimension: set_cancel {type: number}
  dimension: undo_cancel {type: number}
  dimension: charge_failed {type: number}
}

view: churn_prediction {
  derived_table: {
    sql: SELECT * FROM ml.PREDICT(
          MODEL ${churn_model.SQL_TABLE_NAME},
          (SELECT * FROM ${churn_future_input.SQL_TABLE_NAME}), STRUCT(0.17 AS threshold)) ;;
  }

  dimension: customer_id {
    # Was type: number, but customer_id is a string everywhere upstream.
    type: string
    tags: ["user_id"]
  }
  dimension: email {
    type: string
    tags: ["email"]
  }
  dimension: num {type: number}
  dimension: state {type: string}
  dimension: bates_duration {type: number}
  dimension: churn_status {type: number}
  dimension: error {type: number}
  dimension: heartland_duration {type: number}
  dimension: other_duration {type: number}
  dimension: platform {type: string}
  dimension: view {type: number}
  dimension: month_day {type: number}

  # Removed: bates_plays, heartland_plays, other_plays.
  # Those columns are commented out of churn_future_input, so selecting them
  # threw "Unrecognized name" errors in BigQuery.

  dimension: predicted_churn_status {
    type: number
    description: "Binary classification based on the 0.17 threshold"
  }
  dimension: predicted_churn_status_probability {
    type: number
    value_format_name: percent_2
    sql: ${TABLE}.predicted_churn_status_probs[ORDINAL(1)].prob ;;
  }
  dimension: predicted_get_churn_probability_score {
    type: number
    sql: CAST(${TABLE}.predicted_churn_status_probs[ORDINAL(1)].prob * 100 AS INT64) ;;
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
  measure: count {type: count}
}
