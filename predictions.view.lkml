######################## TRAINING/TESTING INPUTS #############################
explore: training_input {}
explore: testing_input {}
# If necessary, uncomment the line below to include explore_source.

include: "upff_google.model.lkml"

view: training_input {
  derived_table: {
    explore_source: bigquery_subscribers_v2 {
      column: user_id { field: bigquery_conversion_model_firstplay.user_id }
      column: get_status {}
      column: addwatchlist_count { field: bigquery_conversion_model_addwatchlist.addwatchlist_count }
      column: error_count { field: bigquery_conversion_model_error.error_count }
      column: bates_play { field: bigquery_conversion_model_firstplay.bates_play }
      column: heartland_play { field: bigquery_conversion_model_firstplay.heartland_play }
      column: other_play { field: bigquery_conversion_model_firstplay.other_play }
      column: removewatchlist_count { field: bigquery_conversion_model_removewatchlist.removewatchlist_count }
      column: bates_duration { field: bigquery_conversion_model_timeupdate.bates_duration }
      column: heartland_duration { field: bigquery_conversion_model_timeupdate.heartland_duration }
      derived_column: bates_2 {sql:bates_play*bates_duration;;}
      derived_column: heartland_2 {sql:heartland_play*heartland_duration;;}
      derived_column: other_2 {sql:other_play*other_duration;;}
      derived_column: total_play {sql:bates_play+heartland_play+other_play;;}
      derived_column: total_duration {sql:bates_duration+heartland_duration+other_play;;}
      column: other_duration { field: bigquery_conversion_model_timeupdate.other_duration }
      column: view_count { field: bigquery_conversion_model_view.view_count }
      column: promoters { field: bigquery_delighted_survey_question_answered.promoters }
      column: platform {}
      column: frequency {}
      column: state {}
      filters: {
        field: bigquery_subscribers_v2.customer_created_date
        value: "after 150 days ago,before 45 days ago"
      }
      filters: {
        field: bigquery_subscribers_v2.get_status
        value: "NOT NULL"
      }
    }
  }
  dimension: total_play {}
  dimension: total_duration {}
  dimension: user_id {}
  dimension: get_status {  }
  dimension: addwatchlist_count {
    type: number
  }

  dimension: platform {}
  dimension: state {}
  dimension: frequency {}

  dimension: error_count {
    type: number
  }
  dimension: bates_play {
    type: number
  }
  dimension: heartland_play {
    type: number
  }
  dimension: other_play {
    type: number
  }
  dimension: removewatchlist_count {
    type: number
  }
  dimension: bates_duration {
    type: number
  }
  dimension: heartland_duration {
    type: number
  }
  dimension: other_duration {
    type: number
  }

  dimension: bates_2 {
    type: number
  }
  dimension: heartland_2 {
    type: number
  }
  dimension: other_2 {
    type: number
  }
  dimension: view_count {
    type: number
  }
  dimension: promoters {}
}


# If necessary, uncomment the line below to include explore_source.

# include: "upff_google.model.lkml"

view: testing_input {
  derived_table: {
    explore_source: bigquery_subscribers_v2 {
      column: user_id { field: bigquery_conversion_model_firstplay.user_id }
      column: get_status {}
      column: addwatchlist_count { field: bigquery_conversion_model_addwatchlist.addwatchlist_count }
      column: error_count { field: bigquery_conversion_model_error.error_count }
      column: bates_play { field: bigquery_conversion_model_firstplay.bates_play }
      column: heartland_play { field: bigquery_conversion_model_firstplay.heartland_play }
      column: other_play { field: bigquery_conversion_model_firstplay.other_play }
      column: removewatchlist_count { field: bigquery_conversion_model_removewatchlist.removewatchlist_count }
      column: bates_duration { field: bigquery_conversion_model_timeupdate.bates_duration }
      column: heartland_duration { field: bigquery_conversion_model_timeupdate.heartland_duration }
      column: other_duration { field: bigquery_conversion_model_timeupdate.other_duration }
      derived_column: bates_2 {sql:bates_play*bates_duration;;}
      derived_column: heartland_2 {sql:heartland_play*heartland_duration;;}
      derived_column: other_2 {sql:other_play*other_duration;;}
      derived_column: total_play {sql:bates_play+heartland_play+other_play;;}
      derived_column: total_duration {sql:bates_duration+heartland_duration+other_play;;}
      column: view_count { field: bigquery_conversion_model_view.view_count }
      column: promoters { field: bigquery_delighted_survey_question_answered.promoters }
      column: platform {}
      column: frequency {}
      column: state {}
      filters: {
        field: bigquery_subscribers_v2.customer_created_date
        value: "after 45 days ago,before 14 days ago"
      }
      filters: {
        field: bigquery_subscribers_v2.get_status
        value: "NOT NULL"
      }
    }
  }
  dimension: total_play {}
  dimension: total_duration {}
  dimension: user_id {}
  dimension: platform {}
  dimension: state {}
  dimension: frequency {}
  dimension: get_status {  }
  dimension: addwatchlist_count {
    type: number
  }
  dimension: error_count {
    type: number
  }
  dimension: bates_play {
    type: number
  }
  dimension: heartland_play {
    type: number
  }
  dimension: other_play {
    type: number
  }
  dimension: removewatchlist_count {
    type: number
  }
  dimension: bates_duration {
    type: number
  }
  dimension: heartland_duration {
    type: number
  }
  dimension: other_duration {
    type: number
  }
  dimension: bates_2 {
    type: number
  }
  dimension: heartland_2 {
    type: number
  }
  dimension: other_2 {
    type: number
  }

  dimension: view_count {
    type: number
  }
  dimension: promoters {}
}

######################## MODEL #############################
view: future_purchase_model {
  derived_table: {
    datagroup_trigger:upff_google_datagroup
    sql_create:
      CREATE OR REPLACE MODEL ${SQL_TABLE_NAME}
      OPTIONS(model_type='logistic_reg'
        , labels=['get_status']
        , min_rel_progress = 0.005
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
# VIEWS:
view: future_purchase_model_evaluation {
  derived_table: {
    sql: SELECT * FROM ml.EVALUATE(
          MODEL ${future_purchase_model.SQL_TABLE_NAME},
          (SELECT * FROM ${testing_input.SQL_TABLE_NAME}));;
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
  derived_table: {explore_source: bigquery_derived_all_firstplay {
      column: count {}
      column: number_of_platforms_by_user {}
      column: user_id {}
      column: platform {}
      column: source {}
      column: frequency { field: bigquery_subscribers.frequency }
      column: day_of_week { field: bigquery_subscribers.day_of_week }
      column: marketing_opt_in { field: bigquery_subscribers.marketing_opt_in }
      column: state { field: bigquery_subscribers.state }
      column: get_status { field: bigquery_subscribers.get_status }
      #column: subscription_length { field: bigquery_subscribers.subscription_length }
      column: promoters { field: bigquery_delighted_survey_question_answered.promoters }
      column: addwatchlist { field: bigquery_subscribers.addwatchlist_count }
      #column: signin { field: bigquery_subscribers.signin_count }
      column: views { field: bigquery_views.views_count }
      column: timecode { field: bigquery_subscribers.timecode_count }

      filters: {
        field: bigquery_subscribers.customer_created_time
        value: "after 14 days ago"
      }

      filters: {
        field: bigquery_derived_timeupdate.timecode_count
        value: "not 0"
      }
      expression_custom_filter: ${bigquery_derived_all_firstplay.timestamp_date} >= ${bigquery_subscribers.customer_created_date} AND ${bigquery_derived_all_firstplay.timestamp_date}<= add_days(14,${bigquery_subscribers.customer_created_date});;
    }
  }
  dimension: count { type: number }
  dimension: views { type: number }
  dimension: timecode { type: number }
  dimension: number_of_platforms_by_user { type: number }
  dimension: addwatchlist { type: number }
  #dimension: signin { type: number }
  dimension: user_id {}
  dimension: email {}
  dimension: platform {}
  dimension: source {}
  dimension: frequency {}
  dimension: day_of_week {}
  dimension: marketing_opt_in {
    type: number
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
  dimension: user_id {}
  #dimension: subscription_length {}
  dimension: promoters {}
  dimension: addwatchlist{ type: number }
  dimension: views { type: number }
  #dimension: timecode { type: number }
  dimension: number_of_platforms_by_user { type: number }

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
  # parameters to allow for dynamic inputs on User Finder dashboard
  parameter: campaign_cost_per_recipient {
    description: "Enter estimated cost per recipient for targeted campaign in USD"
    type: number
    default_value: "0.75"
    allowed_value: {
      label: "$0.25"
      value: "0.25"
    }
    allowed_value: {
      label: "$0.50"
      value: "0.50"
    }
    allowed_value: {
      label: "$0.75"
      value: "0.75"
    }
    allowed_value: {
      label: "$1.00"
      value: "1.00"
    }
    allowed_value: {
      label: "$1.25"
      value: "1.25"
    }
  }
  measure: estimated_campaign_cost_per_recipient {
    label:"Est. Campaign Cost per Recipient"
    type: max
    sql: {% parameter campaign_cost_per_recipient %} ;;
    value_format_name: usd
  }
  parameter: lifetime_revenue_per_customer {
    description: "Enter estimated lifetime value per customer"
    type: number
    default_value: "150.00"
    allowed_value: {
      label: "$100"
      value: "100.00"
    }
    allowed_value: {
      label: "$125"
      value: "125.00"
    }
    allowed_value: {
      label: "$150"
      value: "150.00"
    }
    allowed_value: {
      label: "$175"
      value: "175.00"
    }
    allowed_value: {
      label: "$200"
      value: "200.00"
    }
  }
  measure: estimated_lifetime_revenue_per_customer {
    label:"Est. Lifetime Revenue per Customer"
    type: max
    sql: {% parameter lifetime_revenue_per_customer %} ;;
    value_format_name: usd
  }
  parameter: conversion_boost_from_campaign {
    description: "Enter % increase in customer acquisition as a result of targeted campaign"
    type: number
    default_value: "0.30"
    allowed_value: {
      label: "10.0%"
      value: "0.10"
    }
    allowed_value: {
      label: "20.0%"
      value: "0.20"
    }
    allowed_value: {
      label: "30.0%"
      value: "0.30"
    }
    allowed_value: {
      label: "40.0%"
      value: "0.40"
    }
    allowed_value: {
      label: "50.0%"
      value: "0.50"
    }
  }
  measure: estimated_conversion_boost_from_campaign {
    label:"Est. Conversion Boost from Campaign"
    type: max
    sql: {% parameter conversion_boost_from_campaign %} ;;
    value_format_name: percent_1
  }
}
