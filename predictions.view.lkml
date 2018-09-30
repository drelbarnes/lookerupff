######################## TRAINING/TESTING INPUTS #############################
view: training_input {
  derived_table: {
    explore_source: bigquery_derived_all_firstplay {
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
      #column: subscription_length { field: bigquery_subscribers.days_since_created }
      column: promoters { field: bigquery_delighted_survey_question_answered.promoters }
      column: addwatchlist { field: bigquery_subscribers.addwatchlist_count }
      #column: signin { field: bigquery_subscribers.signin_count }
      column: views { field: bigquery_subscribers.views_count }
      #column: timecode { field: bigquery_subscribers.timecode }

      filters: {
        field: bigquery_subscribers.customer_created_time
        value: "after 150 days ago,before 30 days ago"
      }
      filters: {
        field: bigquery_subscribers.get_status
        value: "NOT NULL"
      }

      expression_custom_filter: ${bigquery_derived_all_firstplay.timestamp_date} >= ${bigquery_subscribers.customer_created_date} AND ${bigquery_derived_all_firstplay.timestamp_date}<= add_days(14,${bigquery_subscribers.customer_created_date});;
    }
  }
  dimension: count { type: number }
  dimension: views { type: number }
  dimension: number_of_platforms_by_user { type: number }
  #dimension: signin { type: number }
  dimension: addwatchlist { type: number }

  dimension: user_id {}
  dimension: platform {}
  dimension: source {}
  dimension: frequency {}
  dimension: day_of_week {}
  dimension: marketing_opt_in {
    type: number
  }
  dimension: state {}
  dimension: get_status {
    type: number
  }

  dimension: promoters {}
}
view: testing_input {
  derived_table: {
    explore_source: bigquery_derived_all_firstplay {
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
      #column: subscription_length { field: bigquery_subscribers.days_since_created }
      column: promoters { field: bigquery_delighted_survey_question_answered.promoters }
      column: addwatchlist { field: bigquery_subscribers.addwatchlist_count }
      #column: signin { field: bigquery_subscribers.signin_count }
      column: views { field: bigquery_subscribers.views_count }
      #column: timecode { field: bigquery_subscribers.timecode }

      filters: {
        field: bigquery_subscribers.customer_created_time
        value: "after 30 days ago,before 14 days ago"
      }
      filters: {
        field: bigquery_subscribers.get_status
        value: "NOT NULL"
      }

      expression_custom_filter: ${bigquery_derived_all_firstplay.timestamp_date} >= ${bigquery_subscribers.customer_created_date} AND ${bigquery_derived_all_firstplay.timestamp_date}<= add_days(14,${bigquery_subscribers.customer_created_date});;
    }
  }
  dimension: count {
    type: number
  }
  dimension: views { type: number }
  dimension: number_of_platforms_by_user { type: number }
  dimension: addwatchlist { type: number }
  #dimension: signin { type: number }
  dimension: user_id {}
  dimension: platform {}
  dimension: source {}
  dimension: frequency {}
  dimension: day_of_week {}
  dimension: marketing_opt_in {
    type: number
  }
  dimension: state {}
  dimension: get_status {
    type: number
  }

  dimension: days_since_created {
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
      column: views { field: bigquery_subscribers.views_count }
      #column: timecode { field: bigquery_subscribers.timecode }
      #column: addwatchlist { field: bigquery_derived_addwatchlist.count }

      filters: {
        field: bigquery_subscribers.customer_created_time
        value: "after 14 days ago"
      }
      expression_custom_filter: ${bigquery_derived_all_firstplay.timestamp_date} >= ${bigquery_subscribers.customer_created_date} AND ${bigquery_derived_all_firstplay.timestamp_date}<= add_days(14,${bigquery_subscribers.customer_created_date});;
    }
  }
  dimension: count { type: number }
  dimension: views { type: number }
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
