######################## TRAINING/TESTING INPUTS #############################
explore: training_input {}
explore: testing_input {}

# Do NOT include the model file from here. This file is already included BY the
# model, so including the model back is circular and fails validation.
# include: "upff_google.model.lkml"

view: training_input {
  derived_table: {
    # No datagroup / sql_trigger_value: nothing fires on a schedule.
    # persist_for only builds the table when someone queries it and it is missing or expired.
    persist_for: "8760 hours"
    explore_source: bigquery_subscribers_v2 {
      column: day_of_week {}
      column: customer_created_at_day {}
      column: user_id {}
      column: state {}
      column: get_status {}
      column: addwatchlist_count { field: bigquery_conversion_model_addwatchlist.addwatchlist_count }
      column: removewatchlist_count { field: bigquery_conversion_model_removewatchlist.removewatchlist_count }
      column: error_count { field: bigquery_conversion_model_error.error_count }
      column: view_count { field: bigquery_conversion_model_view.view_count }
      column: platform {}
      column: number_of_platforms {}
      column: bates_duration_day_1 { field: bigquery_conversion_model_timeupdate.bates_duration_day_1 }
      column: bates_duration_day_2 { field: bigquery_conversion_model_timeupdate.bates_duration_day_2 }
      column: bates_duration_day_3 { field: bigquery_conversion_model_timeupdate.bates_duration_day_3 }
      column: bates_duration_day_4 { field: bigquery_conversion_model_timeupdate.bates_duration_day_4 }
      column: heartland_duration_day_1 { field: bigquery_conversion_model_timeupdate.heartland_duration_day_1 }
      column: heartland_duration_day_2 { field: bigquery_conversion_model_timeupdate.heartland_duration_day_2 }
      column: heartland_duration_day_3 { field: bigquery_conversion_model_timeupdate.heartland_duration_day_3 }
      column: heartland_duration_day_4 { field: bigquery_conversion_model_timeupdate.heartland_duration_day_4 }
      column: other_duration_day_1 { field: bigquery_conversion_model_timeupdate.other_duration_day_1 }
      column: other_duration_day_2 { field: bigquery_conversion_model_timeupdate.other_duration_day_2 }
      column: other_duration_day_3 { field: bigquery_conversion_model_timeupdate.other_duration_day_3 }
      column: other_duration_day_4 { field: bigquery_conversion_model_timeupdate.other_duration_day_4 }
      expression_custom_filter: ${bigquery_subscribers_v2.random} >= 0.3 AND ${bigquery_subscribers_v2.customer_created_date} < add_days(-14,now()) AND ${bigquery_subscribers_v2.customer_created_date} >= add_days(-45,now()) ;;
    }
  }
  # This view had no fields, which leaves its explore empty. Dimensions added to match the columns.
  dimension: user_id {type: number}
  dimension: day_of_week {}
  dimension: customer_created_at_day {}
  dimension: state {}
  dimension: get_status {type: number}
  dimension: addwatchlist_count {type: number}
  dimension: removewatchlist_count {type: number}
  dimension: error_count {type: number}
  dimension: view_count {type: number}
  dimension: platform {}
  dimension: number_of_platforms {type: number}
  dimension: bates_duration_day_1 {type: number}
  dimension: bates_duration_day_2 {type: number}
  dimension: bates_duration_day_3 {type: number}
  dimension: bates_duration_day_4 {type: number}
  dimension: heartland_duration_day_1 {type: number}
  dimension: heartland_duration_day_2 {type: number}
  dimension: heartland_duration_day_3 {type: number}
  dimension: heartland_duration_day_4 {type: number}
  dimension: other_duration_day_1 {type: number}
  dimension: other_duration_day_2 {type: number}
  dimension: other_duration_day_3 {type: number}
  dimension: other_duration_day_4 {type: number}
  measure: count {type: count}
}

view: testing_input {
  derived_table: {
    persist_for: "8760 hours"
    explore_source: bigquery_subscribers_v2 {
      column: day_of_week {}
      column: customer_created_at_day {}
      column: user_id {}
      column: state {}
      column: get_status {}
      column: addwatchlist_count { field: bigquery_conversion_model_addwatchlist.addwatchlist_count }
      column: removewatchlist_count { field: bigquery_conversion_model_removewatchlist.removewatchlist_count }
      column: error_count { field: bigquery_conversion_model_error.error_count }
      column: view_count { field: bigquery_conversion_model_view.view_count }
      column: platform {}
      column: number_of_platforms {}
      column: bates_duration_day_1 { field: bigquery_conversion_model_timeupdate.bates_duration_day_1 }
      column: bates_duration_day_2 { field: bigquery_conversion_model_timeupdate.bates_duration_day_2 }
      column: bates_duration_day_3 { field: bigquery_conversion_model_timeupdate.bates_duration_day_3 }
      column: bates_duration_day_4 { field: bigquery_conversion_model_timeupdate.bates_duration_day_4 }
      column: heartland_duration_day_1 { field: bigquery_conversion_model_timeupdate.heartland_duration_day_1 }
      column: heartland_duration_day_2 { field: bigquery_conversion_model_timeupdate.heartland_duration_day_2 }
      column: heartland_duration_day_3 { field: bigquery_conversion_model_timeupdate.heartland_duration_day_3 }
      column: heartland_duration_day_4 { field: bigquery_conversion_model_timeupdate.heartland_duration_day_4 }
      column: other_duration_day_1 { field: bigquery_conversion_model_timeupdate.other_duration_day_1 }
      column: other_duration_day_2 { field: bigquery_conversion_model_timeupdate.other_duration_day_2 }
      column: other_duration_day_3 { field: bigquery_conversion_model_timeupdate.other_duration_day_3 }
      column: other_duration_day_4 { field: bigquery_conversion_model_timeupdate.other_duration_day_4 }
      expression_custom_filter: ${bigquery_subscribers_v2.random} < 0.3 AND ${bigquery_subscribers_v2.customer_created_date} < add_days(-14,now()) AND ${bigquery_subscribers_v2.customer_created_date} >= add_days(-45,now()) ;;
    }
  }
  dimension: user_id {type: number}
  dimension: day_of_week {}
  dimension: customer_created_at_day {}
  dimension: state {}
  dimension: get_status {type: number}
  dimension: addwatchlist_count {type: number}
  dimension: removewatchlist_count {type: number}
  dimension: error_count {type: number}
  dimension: view_count {type: number}
  dimension: platform {}
  dimension: number_of_platforms {type: number}
  dimension: bates_duration_day_1 {type: number}
  dimension: bates_duration_day_2 {type: number}
  dimension: bates_duration_day_3 {type: number}
  dimension: bates_duration_day_4 {type: number}
  dimension: heartland_duration_day_1 {type: number}
  dimension: heartland_duration_day_2 {type: number}
  dimension: heartland_duration_day_3 {type: number}
  dimension: heartland_duration_day_4 {type: number}
  dimension: other_duration_day_1 {type: number}
  dimension: other_duration_day_2 {type: number}
  dimension: other_duration_day_3 {type: number}
  dimension: other_duration_day_4 {type: number}
  measure: count {type: count}
}

######################## MODEL #############################
view: future_purchase_model {
  derived_table: {
    # Was: datagroup_trigger: upff_google_datagroup  (fired on the datagroup schedule)
    # sql_create must be persistent; persist_for satisfies that with no trigger.
    persist_for: "8760 hours"
    sql_create:
      CREATE OR REPLACE MODEL ${SQL_TABLE_NAME}
      OPTIONS(model_type='logistic_reg'
        , labels=['get_status']
        , min_rel_progress = 0.00000005
        , max_iterations = 40
        ) AS
      SELECT
         * EXCEPT(user_id)
      FROM ${training_input.SQL_TABLE_NAME} ;;
  }
}

######################## TRAINING INFORMATION #############################
explore: future_purchase_model_evaluation {}
explore: future_purchase_model_training_info {}
explore: roc_curve {}
explore: confusion_matrix {}

view: future_purchase_model_evaluation {
  derived_table: {
    sql: SELECT * FROM ml.EVALUATE(
          MODEL ${future_purchase_model.SQL_TABLE_NAME},
          (SELECT * FROM ${testing_input.SQL_TABLE_NAME}), STRUCT(0.46 AS threshold)) ;;
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

view: confusion_matrix {
  derived_table: {
    sql: SELECT * FROM ml.CONFUSION_MATRIX(
        MODEL ${future_purchase_model.SQL_TABLE_NAME},
        (SELECT * FROM ${testing_input.SQL_TABLE_NAME}), STRUCT(0.46 AS threshold)) ;;
  }
  dimension: expected_label {}
  dimension: _0 {}
  dimension: _1 {}
}

view: roc_curve {
  derived_table: {
    sql: SELECT * FROM ml.ROC_CURVE(
        MODEL ${future_purchase_model.SQL_TABLE_NAME},
        (SELECT * FROM ${testing_input.SQL_TABLE_NAME})) ;;
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

view: future_purchase_model_training_info {
  derived_table: {
    sql: SELECT * FROM ml.TRAINING_INFO(MODEL ${future_purchase_model.SQL_TABLE_NAME}) ;;
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
explore: cat_weights {}
view: cat_weights {
  derived_table: {
    sql: SELECT a.*, category.category AS cat, category.weight AS catweight
         FROM ml.WEIGHTS(MODEL ${future_purchase_model.SQL_TABLE_NAME}) AS a,
         UNNEST(category_weights) AS category ;;
  }
  dimension: cat {type: string}
  dimension: catweight {type: number}
}

explore: weights {}
view: weights {
  derived_table: {
    sql: SELECT * FROM ml.WEIGHTS(MODEL ${future_purchase_model.SQL_TABLE_NAME}) ;;
  }
  dimension: processed_input {type: string}
  dimension: weight {type: number}
}

########################################## PREDICT FUTURE ############################
explore: future_purchase_prediction {}

# Ephemeral (no persistence): runs only when future_purchase_prediction is queried.
view: future_input {
  derived_table: {
    explore_source: bigquery_subscribers_v2 {
      column: day_of_week {}
      column: customer_created_at_day {}
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
      column: bates_duration_day_1 { field: bigquery_conversion_model_timeupdate.bates_duration_day_1 }
      column: bates_duration_day_2 { field: bigquery_conversion_model_timeupdate.bates_duration_day_2 }
      column: bates_duration_day_3 { field: bigquery_conversion_model_timeupdate.bates_duration_day_3 }
      column: bates_duration_day_4 { field: bigquery_conversion_model_timeupdate.bates_duration_day_4 }
      column: heartland_duration_day_1 { field: bigquery_conversion_model_timeupdate.heartland_duration_day_1 }
      column: heartland_duration_day_2 { field: bigquery_conversion_model_timeupdate.heartland_duration_day_2 }
      column: heartland_duration_day_3 { field: bigquery_conversion_model_timeupdate.heartland_duration_day_3 }
      column: heartland_duration_day_4 { field: bigquery_conversion_model_timeupdate.heartland_duration_day_4 }
      column: other_duration_day_1 { field: bigquery_conversion_model_timeupdate.other_duration_day_1 }
      column: other_duration_day_2 { field: bigquery_conversion_model_timeupdate.other_duration_day_2 }
      column: other_duration_day_3 { field: bigquery_conversion_model_timeupdate.other_duration_day_3 }
      column: other_duration_day_4 { field: bigquery_conversion_model_timeupdate.other_duration_day_4 }
      expression_custom_filter: ${bigquery_subscribers_v2.customer_created_date} < add_days(-8,now()) AND ${bigquery_subscribers_v2.customer_created_date} >= add_days(-14,now()) ;;
    }
  }
  # Removed dimensions that pointed at columns this table doesn't have:
  # count, views, timecode, number_of_platforms_by_user, addwatchlist, source, frequency, promoters.
  dimension: user_id {
    type: number
    tags: ["user_id"]
  }
  dimension: email {
    type: string
    tags: ["email"]
  }
  # Was type: yesno with no sql. A yesno column in a native derived table lands as 'Yes'/'No' text,
  # so the default yesno SQL fails. string avoids that.
  dimension: moptin {type: string}
  dimension: day_of_week {}
  dimension: customer_created_at_day {}
  dimension: state {}
  dimension: platform {}
  dimension: number_of_platforms {type: number}
  dimension: get_status {type: number}
  dimension: addwatchlist_count {type: number}
  dimension: removewatchlist_count {type: number}
  dimension: error_count {type: number}
  dimension: view_count {type: number}
}

# Ephemeral: ML.PREDICT runs only when this explore is queried.
view: future_purchase_prediction {
  derived_table: {
    sql: SELECT * FROM ml.PREDICT(
          MODEL ${future_purchase_model.SQL_TABLE_NAME},
          (SELECT * FROM ${future_input.SQL_TABLE_NAME})) ;;
  }
  dimension: day_of_week {}
  dimension: customer_created_at_day {}
  dimension: user_id {
    type: number
    tags: ["user_id"]
  }
  dimension: email {
    type: string
    tags: ["email"]
  }
  dimension: moptin {type: string}
  dimension: state {}
  dimension: get_status {type: number}
  dimension: addwatchlist_count {type: number}
  dimension: removewatchlist_count {type: number}
  dimension: error_count {type: number}
  dimension: view_count {type: number}
  dimension: platform {}
  dimension: number_of_platforms {type: number}
  dimension: bates_duration_day_1 {type: number}
  dimension: bates_duration_day_2 {type: number}
  dimension: bates_duration_day_3 {type: number}
  dimension: bates_duration_day_4 {type: number}
  dimension: heartland_duration_day_1 {type: number}
  dimension: heartland_duration_day_2 {type: number}
  dimension: heartland_duration_day_3 {type: number}
  dimension: heartland_duration_day_4 {type: number}
  dimension: other_duration_day_1 {type: number}
  dimension: other_duration_day_2 {type: number}
  dimension: other_duration_day_3 {type: number}
  dimension: other_duration_day_4 {type: number}

  # Removed dimensions with no matching column in the PREDICT output:
  # days_played, frequency, promoters, and all *_play_day_1..4 fields.

  dimension: predicted_get_status {
    type: number
    description: "Binary classification at the default 0.5 threshold"
  }
  dimension: predicted_get_status_probability {
    type: number
    value_format_name: percent_2
    sql: ${TABLE}.predicted_get_status_probs[ORDINAL(1)].prob ;;
  }
  dimension: predicted_get_probability_score {
    type: number
    sql: CAST(${TABLE}.predicted_get_status_probs[ORDINAL(1)].prob * 100 AS INT64) ;;
  }
  dimension: predicted_probability {
    type: number
    sql: ${predicted_get_status_probability} ;;
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
    sql: ${user_id} ;;
  }
}
