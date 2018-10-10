######################## TRAINING/TESTING INPUTS #############################
explore: training_input {}
explore: testing_input {}
# If necessary, uncomment the line below to include explore_source.

include: "upff_google.model.lkml"

view: training_input {
  derived_table: {
    explore_source: bigquery_subscribers_v2 {
      column: day_of_week {}
      column: days_played {field: bigquery_conversion_model_firstplay.days_played}
      column: customer_id {}
      column: frequency {}
      column: state {}
      column: get_status {}
      column: addwatchlist_count { field: bigquery_conversion_model_addwatchlist.addwatchlist_count }
      column: removewatchlist_count { field: bigquery_conversion_model_removewatchlist.removewatchlist_count }
      column: error_count { field: bigquery_conversion_model_error.error_count }
      column: view_count { field: bigquery_conversion_model_view.view_count }
      column: promoters { field: bigquery_delighted_survey_question_answered.promoters }
       column: platform {}
#       column: bates_play { field: bigquery_conversion_model_firstplay.bates_play}
#       column: heartland_play { field: bigquery_conversion_model_firstplay.heartland_play}
#       column: other_play { field: bigquery_conversion_model_firstplay.other_play }
#       column: bates_duration { field: bigquery_conversion_model_timeupdate.bates_duration }
#       column: heartland_duration { field: bigquery_conversion_model_timeupdate.heartland_duration }
#       column: other_duration { field: bigquery_conversion_model_timeupdate.other_duration }
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
# #       column: bates_duration_day_6 { field: bigquery_conversion_model_timeupdate.bates_duration_day_6 }
# #       column: bates_duration_day_7 { field: bigquery_conversion_model_timeupdate.bates_duration_day_7 }
# #       column: bates_duration_day_8 { field: bigquery_conversion_model_timeupdate.bates_duration_day_8 }
# #       column: bates_duration_day_9 { field: bigquery_conversion_model_timeupdate.bates_duration_day_9 }
# #       column: bates_duration_day_10 { field: bigquery_conversion_model_timeupdate.bates_duration_day_10 }
# #       column: bates_duration_day_11 { field: bigquery_conversion_model_timeupdate.bates_duration_day_11 }
# #       column: bates_duration_day_12 { field: bigquery_conversion_model_timeupdate.bates_duration_day_12 }
# #       column: bates_duration_day_13 { field: bigquery_conversion_model_timeupdate.bates_duration_day_13 }
# #       column: bates_duration_day_14 { field: bigquery_conversion_model_timeupdate.bates_duration_day_14 }
#       derived_column: bates_day_1 {sql: bates_play_day_1*bates_duration_day_1;;}
#       derived_column: bates_day_2 {sql: bates_play_day_2*bates_duration_day_2;;}
#       derived_column: bates_day_3 {sql: bates_play_day_3*bates_duration_day_3;;}
# #       derived_column: bates_day_4 {sql: bates_play_day_4*bates_duration_day_4;;}
# #       derived_column: bates_day_5 {sql: bates_play_day_5*bates_duration_day_5;;}
# #       derived_column: bates_day_6 {sql: bates_play_day_6*bates_duration_day_6;;}
# #       derived_column: bates_day_7 {sql: bates_play_day_7*bates_duration_day_7;;}
# #       derived_column: bates_day_8 {sql: bates_play_day_8*bates_duration_day_8;;}
# #       derived_column: bates_day_9 {sql: bates_play_day_9*bates_duration_day_9;;}
# #       derived_column: bates_day_10 {sql: bates_play_day_10*bates_duration_day_10;;}
# #       derived_column: bates_day_11 {sql: bates_play_day_11*bates_duration_day_11;;}
# #       derived_column: bates_day_12 {sql: bates_play_day_12*bates_duration_day_12;;}
# #       derived_column: bates_day_13 {sql: bates_play_day_13*bates_duration_day_13;;}
# #       derived_column: bates_day_14 {sql: bates_play_day_14*bates_duration_day_14;;}
      column: heartland_duration_day_1 { field: bigquery_conversion_model_timeupdate.heartland_duration_day_1 }
      column: heartland_duration_day_2 { field: bigquery_conversion_model_timeupdate.heartland_duration_day_2 }
      column: heartland_duration_day_3 { field: bigquery_conversion_model_timeupdate.heartland_duration_day_3 }
# #       column: heartland_duration_day_4 { field: bigquery_conversion_model_timeupdate.heartland_duration_day_4 }
# #       column: heartland_duration_day_5 { field: bigquery_conversion_model_timeupdate.heartland_duration_day_5 }
# #       column: heartland_duration_day_6 { field: bigquery_conversion_model_timeupdate.heartland_duration_day_6 }
# #       column: heartland_duration_day_7 { field: bigquery_conversion_model_timeupdate.heartland_duration_day_7 }
# #       column: heartland_duration_day_8 { field: bigquery_conversion_model_timeupdate.heartland_duration_day_8 }
# #       column: heartland_duration_day_9 { field: bigquery_conversion_model_timeupdate.heartland_duration_day_9 }
# #       column: heartland_duration_day_10 { field: bigquery_conversion_model_timeupdate.heartland_duration_day_10 }
# #       column: heartland_duration_day_11 { field: bigquery_conversion_model_timeupdate.heartland_duration_day_11 }
# #       column: heartland_duration_day_12 { field: bigquery_conversion_model_timeupdate.heartland_duration_day_12 }
# #       column: heartland_duration_day_13 { field: bigquery_conversion_model_timeupdate.heartland_duration_day_13 }
# #       column: heartland_duration_day_14 { field: bigquery_conversion_model_timeupdate.heartland_duration_day_14 }
#       derived_column: heartland_day_1 {sql: heartland_play_day_1*heartland_duration_day_1;;}
#       derived_column: heartland_day_2 {sql: heartland_play_day_2*heartland_duration_day_2;;}
#       derived_column: heartland_day_3 {sql: heartland_play_day_3*heartland_duration_day_3;;}
# #       derived_column: heartland_day_4 {sql: heartland_play_day_4*heartland_duration_day_4;;}
# #       derived_column: heartland_day_5 {sql: heartland_play_day_5*heartland_duration_day_5;;}
# #       derived_column: heartland_day_6 {sql: heartland_play_day_6*heartland_duration_day_6;;}
# #       derived_column: heartland_day_7 {sql: heartland_play_day_7*heartland_duration_day_7;;}
# #       derived_column: heartland_day_8 {sql: heartland_play_day_8*heartland_duration_day_8;;}
# #       derived_column: heartland_day_9 {sql: heartland_play_day_9*heartland_duration_day_9;;}
# #       derived_column: heartland_day_10 {sql: heartland_play_day_10*heartland_duration_day_10;;}
# #       derived_column: heartland_day_11 {sql: heartland_play_day_11*heartland_duration_day_11;;}
# #       derived_column: heartland_day_12 {sql: heartland_play_day_12*heartland_duration_day_12;;}
# #       derived_column: heartland_day_13 {sql: heartland_play_day_13*heartland_duration_day_13;;}
# #       derived_column: heartland_day_14 {sql: heartland_play_day_14*heartland_duration_day_14;;}
      column: other_duration_day_1 { field: bigquery_conversion_model_timeupdate.other_duration_day_1 }
      column: other_duration_day_2 { field: bigquery_conversion_model_timeupdate.other_duration_day_2 }
      column: other_duration_day_3 { field: bigquery_conversion_model_timeupdate.other_duration_day_3 }
# #       column: other_duration_day_4 { field: bigquery_conversion_model_timeupdate.other_duration_day_4 }
# #       column: other_duration_day_5 { field: bigquery_conversion_model_timeupdate.other_duration_day_5 }
# #       column: other_duration_day_6 { field: bigquery_conversion_model_timeupdate.other_duration_day_6 }
# #       column: other_duration_day_7 { field: bigquery_conversion_model_timeupdate.other_duration_day_7 }
# #       column: other_duration_day_8 { field: bigquery_conversion_model_timeupdate.other_duration_day_8 }
# #       column: other_duration_day_9 { field: bigquery_conversion_model_timeupdate.other_duration_day_9 }
# #       column: other_duration_day_10 { field: bigquery_conversion_model_timeupdate.other_duration_day_10 }
# #       column: other_duration_day_11 { field: bigquery_conversion_model_timeupdate.other_duration_day_11 }
# #       column: other_duration_day_12 { field: bigquery_conversion_model_timeupdate.other_duration_day_12 }
# #       column: other_duration_day_13 { field: bigquery_conversion_model_timeupdate.other_duration_day_13 }
# #       column: other_duration_day_14 { field: bigquery_conversion_model_timeupdate.other_duration_day_14 }
#       derived_column: other_day_1 {sql: other_play_day_1*other_duration_day_1;;}
#       derived_column: other_day_2 {sql: other_play_day_2*other_duration_day_2;;}
#       derived_column: other_day_3 {sql: other_play_day_3*other_duration_day_3;;}
# #       derived_column: other_day_4 {sql: other_play_day_4*other_duration_day_4;;}
# #       derived_column: other_day_5 {sql: other_play_day_5*other_duration_day_5;;}
# #       derived_column: other_day_6 {sql: other_play_day_6*other_duration_day_6;;}
# #       derived_column: other_day_7 {sql: other_play_day_7*other_duration_day_7;;}
# #       derived_column: other_day_8 {sql: other_play_day_8*other_duration_day_8;;}
# #       derived_column: other_day_9 {sql: other_play_day_9*other_duration_day_9;;}
# #       derived_column: other_day_10 {sql: other_play_day_10*other_duration_day_10;;}
# #       derived_column: other_day_11 {sql: other_play_day_11*other_duration_day_11;;}
# #       derived_column: other_day_12 {sql: other_play_day_12*other_duration_day_12;;}
# #       derived_column: other_day_13 {sql: other_play_day_13*other_duration_day_13;;}
# #       derived_column: other_day_14 {sql: other_play_day_14*other_duration_day_14;;}

      expression_custom_filter: ${bigquery_subscribers_v2.subscription_length}>28 AND ${bigquery_subscribers_v2.subscription_length}<=90;;
      filters: {
        field: bigquery_subscribers_v2.get_status
        value: "NOT NULL"
      }
    }
  }
  dimension: customer_id {
    type: number
  }

  dimension: days_played {type:number}
  dimension: day_of_week {}

  dimension: frequency {}

  dimension: state {}

  dimension: get_status {
    type: number
  }
  dimension: addwatchlist_count {
    type: number
  }
  dimension: removewatchlist_count {
    type: number
  }
  dimension: error_count {
    type: number
  }
  dimension: view_count {
    type: number
  }
  dimension: promoters {}
  dimension: platform {}
  dimension: bates_play_day_1 {
    type: number
  }
  dimension: bates_play_day_2 {
    type: number
  }
  dimension: bates_play_day_3 {
    type: number
  }
  dimension: bates_play_day_4 {
    type: number
  }
  dimension: bates_play_day_5 {
    type: number
  }
  dimension: bates_play_day_6 {
    type: number
  }
  dimension: bates_play_day_7 {
    type: number
  }
  dimension: bates_play_day_8 {
    type: number
  }
  dimension: bates_play_day_9 {
    type: number
  }
  dimension: bates_play_day_10 {
    type: number
  }
  dimension: bates_play_day_11 {
    type: number
  }
  dimension: bates_play_day_12 {
    type: number
  }
  dimension: bates_play_day_13 {
    type: number
  }
  dimension: bates_play_day_14 {
    type: number
  }

#   dimension: bates_day_1 {
#     type: number
#   }
#   dimension: bates_day_2 {
#     type: number
#   }
#   dimension: bates_day_3 {
#     type: number
#   }
#   dimension: bates_day_4 {
#     type: number
#   }
#   dimension: bates_day_5 {
#     type: number
#   }
#   dimension: bates_day_6 {
#     type: number
#   }
#   dimension: bates_day_7 {
#     type: number
#   }
#   dimension: bates_day_8 {
#     type: number
#   }
#   dimension: bates_day_9 {
#     type: number
#   }
#   dimension: bates_day_10 {
#     type: number
#   }
#   dimension: bates_day_11 {
#     type: number
#   }
#   dimension: bates_day_12 {
#     type: number
#   }
#   dimension: bates_day_13 {
#     type: number
#   }
#   dimension: bates_day_14 {
#     type: number
#   }

  dimension: heartland_play_day_1 {
    type: number
  }
  dimension: heartland_play_day_2 {
    type: number
  }
  dimension: heartland_play_day_3 {
    type: number
  }
  dimension: heartland_play_day_4 {
    type: number
  }
  dimension: heartland_play_day_5 {
    type: number
  }
  dimension: heartland_play_day_6 {
    type: number
  }
  dimension: heartland_play_day_7 {
    type: number
  }
  dimension: heartland_play_day_8 {
    type: number
  }
  dimension: heartland_play_day_9 {
    type: number
  }
  dimension: heartland_play_day_10 {
    type: number
  }
  dimension: heartland_play_day_11 {
    type: number
  }
  dimension: heartland_play_day_12 {
    type: number
  }
  dimension: heartland_play_day_13 {
    type: number
  }
  dimension: heartland_play_day_14 {
    type: number
  }

#   dimension: heartland_day_1 {
#     type: number
#   }
#   dimension: heartland_day_2 {
#     type: number
#   }
#   dimension: heartland_day_3 {
#     type: number
#   }
#   dimension: heartland_day_4 {
#     type: number
#   }
#   dimension: heartland_day_5 {
#     type: number
#   }
#   dimension: heartland_day_6 {
#     type: number
#   }
#   dimension: heartland_day_7 {
#     type: number
#   }
#   dimension: heartland_day_8 {
#     type: number
#   }
#   dimension: heartland_day_9 {
#     type: number
#   }
#   dimension: heartland_day_10 {
#     type: number
#   }
#   dimension: heartland_day_11 {
#     type: number
#   }
#   dimension: heartland_day_12 {
#     type: number
#   }
#   dimension: heartland_day_13 {
#     type: number
#   }
#   dimension: heartland_day_14 {
#     type: number
#   }

  dimension: other_play_day_1 {
    type: number
  }
  dimension: other_play_day_2 {
    type: number
  }
  dimension: other_play_day_3 {
    type: number
  }
  dimension: other_play_day_4 {
    type: number
  }
  dimension: other_play_day_5 {
    type: number
  }
  dimension: other_play_day_6 {
    type: number
  }
  dimension: other_play_day_7 {
    type: number
  }
  dimension: other_play_day_8 {
    type: number
  }
  dimension: other_play_day_9 {
    type: number
  }
  dimension: other_play_day_10 {
    type: number
  }
  dimension: other_play_day_11 {
    type: number
  }
  dimension: other_play_day_12 {
    type: number
  }
  dimension: other_play_day_13 {
    type: number
  }
  dimension: other_play_day_14 {
    type: number
  }

#   dimension: other_day_1 {
#     type: number
#   }
#   dimension: other_day_2 {
#     type: number
#   }
#   dimension: other_day_3 {
#     type: number
#   }
#   dimension: other_day_4 {
#     type: number
#   }
#   dimension: other_day_5 {
#     type: number
#   }
#   dimension: other_day_6 {
#     type: number
#   }
#   dimension: other_day_7 {
#     type: number
#   }
#   dimension: other_day_8 {
#     type: number
#   }
#   dimension: other_day_9 {
#     type: number
#   }
#   dimension: other_day_10 {
#     type: number
#   }
#   dimension: other_day_11 {
#     type: number
#   }
#   dimension: other_day_12 {
#     type: number
#   }
#   dimension: other_day_13 {
#     type: number
#   }
#   dimension: other_day_14 {
#     type: number
#   }

  dimension: bates_duration_day_1 {
    type: number
  }
  dimension: bates_duration_day_2 {
    type: number
  }
  dimension: bates_duration_day_3 {
    type: number
  }
  dimension: bates_duration_day_4 {
    type: number
  }
  dimension: bates_duration_day_5 {
    type: number
  }
  dimension: bates_duration_day_6 {
    type: number
  }
  dimension: bates_duration_day_7 {
    type: number
  }
  dimension: bates_duration_day_8 {
    type: number
  }
  dimension: bates_duration_day_9 {
    type: number
  }
  dimension: bates_duration_day_10 {
    type: number
  }
  dimension: bates_duration_day_11 {
    type: number
  }
  dimension: bates_duration_day_12 {
    type: number
  }
  dimension: bates_duration_day_13 {
    type: number
  }
  dimension: bates_duration_day_14 {
    type: number
  }
  dimension: heartland_duration_day_1 {
    type: number
  }
  dimension: heartland_duration_day_2 {
    type: number
  }
  dimension: heartland_duration_day_3 {
    type: number
  }
  dimension: heartland_duration_day_4 {
    type: number
  }
  dimension: heartland_duration_day_5 {
    type: number
  }
  dimension: heartland_duration_day_6 {
    type: number
  }
  dimension: heartland_duration_day_7 {
    type: number
  }
  dimension: heartland_duration_day_8 {
    type: number
  }
  dimension: heartland_duration_day_9 {
    type: number
  }
  dimension: heartland_duration_day_10 {
    type: number
  }
  dimension: heartland_duration_day_11 {
    type: number
  }
  dimension: heartland_duration_day_12 {
    type: number
  }
  dimension: heartland_duration_day_13 {
    type: number
  }
  dimension: heartland_duration_day_14 {
    type: number
  }
  dimension: other_duration_day_1 {
    type: number
  }
  dimension: other_duration_day_2 {
    type: number
  }
  dimension: other_duration_day_3 {
    type: number
  }
  dimension: other_duration_day_4 {
    type: number
  }
  dimension: other_duration_day_5 {
    type: number
  }
  dimension: other_duration_day_6 {
    type: number
  }
  dimension: other_duration_day_7 {
    type: number
  }
  dimension: other_duration_day_8 {
    type: number
  }
  dimension: other_duration_day_9 {
    type: number
  }
  dimension: other_duration_day_10 {
    type: number
  }
  dimension: other_duration_day_11 {
    type: number
  }
  dimension: other_duration_day_12 {
    type: number
  }
  dimension: other_duration_day_13 {
    type: number
  }
  dimension: other_duration_day_14 {
    type: number
  }
}


# If necessary, uncomment the line below to include explore_source.

# include: "upff_google.model.lkml"

view: testing_input {
  derived_table: {
    explore_source: bigquery_subscribers_v2 {
      column: day_of_week {}
      column: days_played {field: bigquery_conversion_model_firstplay.days_played}
      column: customer_id {}
      column: frequency {}
      column: state {}
      column: get_status {}
      column: addwatchlist_count { field: bigquery_conversion_model_addwatchlist.addwatchlist_count }
      column: removewatchlist_count { field: bigquery_conversion_model_removewatchlist.removewatchlist_count }
      column: error_count { field: bigquery_conversion_model_error.error_count }
      column: view_count { field: bigquery_conversion_model_view.view_count }
      column: promoters { field: bigquery_delighted_survey_question_answered.promoters }
       column: platform {}
#       column: bates_play { field: bigquery_conversion_model_firstplay.bates_play}
#       column: heartland_play { field: bigquery_conversion_model_firstplay.heartland_play}
#       column: other_play { field: bigquery_conversion_model_firstplay.other_play }
#       column: bates_duration { field: bigquery_conversion_model_timeupdate.bates_duration }
#       column: heartland_duration { field: bigquery_conversion_model_timeupdate.heartland_duration }
#       column: other_duration { field: bigquery_conversion_model_timeupdate.other_duration }
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
# #       column: bates_duration_day_6 { field: bigquery_conversion_model_timeupdate.bates_duration_day_6 }
# #       column: bates_duration_day_7 { field: bigquery_conversion_model_timeupdate.bates_duration_day_7 }
# #       column: bates_duration_day_8 { field: bigquery_conversion_model_timeupdate.bates_duration_day_8 }
# #       column: bates_duration_day_9 { field: bigquery_conversion_model_timeupdate.bates_duration_day_9 }
# #       column: bates_duration_day_10 { field: bigquery_conversion_model_timeupdate.bates_duration_day_10 }
# #       column: bates_duration_day_11 { field: bigquery_conversion_model_timeupdate.bates_duration_day_11 }
# #       column: bates_duration_day_12 { field: bigquery_conversion_model_timeupdate.bates_duration_day_12 }
# #       column: bates_duration_day_13 { field: bigquery_conversion_model_timeupdate.bates_duration_day_13 }
# #       column: bates_duration_day_14 { field: bigquery_conversion_model_timeupdate.bates_duration_day_14 }
#       derived_column: bates_day_1 {sql: bates_play_day_1*bates_duration_day_1;;}
#       derived_column: bates_day_2 {sql: bates_play_day_2*bates_duration_day_2;;}
#       derived_column: bates_day_3 {sql: bates_play_day_3*bates_duration_day_3;;}
# #       derived_column: bates_day_4 {sql: bates_play_day_4*bates_duration_day_4;;}
# #       derived_column: bates_day_5 {sql: bates_play_day_5*bates_duration_day_5;;}
# #       derived_column: bates_day_6 {sql: bates_play_day_6*bates_duration_day_6;;}
# #       derived_column: bates_day_7 {sql: bates_play_day_7*bates_duration_day_7;;}
# #       derived_column: bates_day_8 {sql: bates_play_day_8*bates_duration_day_8;;}
# #       derived_column: bates_day_9 {sql: bates_play_day_9*bates_duration_day_9;;}
# #       derived_column: bates_day_10 {sql: bates_play_day_10*bates_duration_day_10;;}
# #       derived_column: bates_day_11 {sql: bates_play_day_11*bates_duration_day_11;;}
# #       derived_column: bates_day_12 {sql: bates_play_day_12*bates_duration_day_12;;}
# #       derived_column: bates_day_13 {sql: bates_play_day_13*bates_duration_day_13;;}
# #       derived_column: bates_day_14 {sql: bates_play_day_14*bates_duration_day_14;;}
      column: heartland_duration_day_1 { field: bigquery_conversion_model_timeupdate.heartland_duration_day_1 }
      column: heartland_duration_day_2 { field: bigquery_conversion_model_timeupdate.heartland_duration_day_2 }
      column: heartland_duration_day_3 { field: bigquery_conversion_model_timeupdate.heartland_duration_day_3 }
# #       column: heartland_duration_day_4 { field: bigquery_conversion_model_timeupdate.heartland_duration_day_4 }
# #       column: heartland_duration_day_5 { field: bigquery_conversion_model_timeupdate.heartland_duration_day_5 }
# #       column: heartland_duration_day_6 { field: bigquery_conversion_model_timeupdate.heartland_duration_day_6 }
# #       column: heartland_duration_day_7 { field: bigquery_conversion_model_timeupdate.heartland_duration_day_7 }
# #       column: heartland_duration_day_8 { field: bigquery_conversion_model_timeupdate.heartland_duration_day_8 }
# #       column: heartland_duration_day_9 { field: bigquery_conversion_model_timeupdate.heartland_duration_day_9 }
# #       column: heartland_duration_day_10 { field: bigquery_conversion_model_timeupdate.heartland_duration_day_10 }
# #       column: heartland_duration_day_11 { field: bigquery_conversion_model_timeupdate.heartland_duration_day_11 }
# #       column: heartland_duration_day_12 { field: bigquery_conversion_model_timeupdate.heartland_duration_day_12 }
# #       column: heartland_duration_day_13 { field: bigquery_conversion_model_timeupdate.heartland_duration_day_13 }
# #       column: heartland_duration_day_14 { field: bigquery_conversion_model_timeupdate.heartland_duration_day_14 }
#       derived_column: heartland_day_1 {sql: heartland_play_day_1*heartland_duration_day_1;;}
#       derived_column: heartland_day_2 {sql: heartland_play_day_2*heartland_duration_day_2;;}
#       derived_column: heartland_day_3 {sql: heartland_play_day_3*heartland_duration_day_3;;}
# #       derived_column: heartland_day_4 {sql: heartland_play_day_4*heartland_duration_day_4;;}
# #       derived_column: heartland_day_5 {sql: heartland_play_day_5*heartland_duration_day_5;;}
# #       derived_column: heartland_day_6 {sql: heartland_play_day_6*heartland_duration_day_6;;}
# #       derived_column: heartland_day_7 {sql: heartland_play_day_7*heartland_duration_day_7;;}
# #       derived_column: heartland_day_8 {sql: heartland_play_day_8*heartland_duration_day_8;;}
# #       derived_column: heartland_day_9 {sql: heartland_play_day_9*heartland_duration_day_9;;}
# #       derived_column: heartland_day_10 {sql: heartland_play_day_10*heartland_duration_day_10;;}
# #       derived_column: heartland_day_11 {sql: heartland_play_day_11*heartland_duration_day_11;;}
# #       derived_column: heartland_day_12 {sql: heartland_play_day_12*heartland_duration_day_12;;}
# #       derived_column: heartland_day_13 {sql: heartland_play_day_13*heartland_duration_day_13;;}
# #       derived_column: heartland_day_14 {sql: heartland_play_day_14*heartland_duration_day_14;;}
      column: other_duration_day_1 { field: bigquery_conversion_model_timeupdate.other_duration_day_1 }
      column: other_duration_day_2 { field: bigquery_conversion_model_timeupdate.other_duration_day_2 }
      column: other_duration_day_3 { field: bigquery_conversion_model_timeupdate.other_duration_day_3 }
# #       column: other_duration_day_4 { field: bigquery_conversion_model_timeupdate.other_duration_day_4 }
# #       column: other_duration_day_5 { field: bigquery_conversion_model_timeupdate.other_duration_day_5 }
# #       column: other_duration_day_6 { field: bigquery_conversion_model_timeupdate.other_duration_day_6 }
# #       column: other_duration_day_7 { field: bigquery_conversion_model_timeupdate.other_duration_day_7 }
# #       column: other_duration_day_8 { field: bigquery_conversion_model_timeupdate.other_duration_day_8 }
# #       column: other_duration_day_9 { field: bigquery_conversion_model_timeupdate.other_duration_day_9 }
# #       column: other_duration_day_10 { field: bigquery_conversion_model_timeupdate.other_duration_day_10 }
# #       column: other_duration_day_11 { field: bigquery_conversion_model_timeupdate.other_duration_day_11 }
# #       column: other_duration_day_12 { field: bigquery_conversion_model_timeupdate.other_duration_day_12 }
# #       column: other_duration_day_13 { field: bigquery_conversion_model_timeupdate.other_duration_day_13 }
# #       column: other_duration_day_14 { field: bigquery_conversion_model_timeupdate.other_duration_day_14 }
#       derived_column: other_day_1 {sql: other_play_day_1*other_duration_day_1;;}
#       derived_column: other_day_2 {sql: other_play_day_2*other_duration_day_2;;}
#       derived_column: other_day_3 {sql: other_play_day_3*other_duration_day_3;;}
# #       derived_column: other_day_4 {sql: other_play_day_4*other_duration_day_4;;}
# #       derived_column: other_day_5 {sql: other_play_day_5*other_duration_day_5;;}
# #       derived_column: other_day_6 {sql: other_play_day_6*other_duration_day_6;;}
# #       derived_column: other_day_7 {sql: other_play_day_7*other_duration_day_7;;}
# #       derived_column: other_day_8 {sql: other_play_day_8*other_duration_day_8;;}
# #       derived_column: other_day_9 {sql: other_play_day_9*other_duration_day_9;;}
# #       derived_column: other_day_10 {sql: other_play_day_10*other_duration_day_10;;}
# #       derived_column: other_day_11 {sql: other_play_day_11*other_duration_day_11;;}
# #       derived_column: other_day_12 {sql: other_play_day_12*other_duration_day_12;;}
# #       derived_column: other_day_13 {sql: other_play_day_13*other_duration_day_13;;}
# #       derived_column: other_day_14 {sql: other_play_day_14*other_duration_day_14;;}

      expression_custom_filter: ${bigquery_subscribers_v2.subscription_length}>14 AND ${bigquery_subscribers_v2.subscription_length}<=28;;
      filters: {
        field: bigquery_subscribers_v2.get_status
        value: "NOT NULL"
      }
    }
  }
  dimension: customer_id {
    type: number
  }

  dimension: state {}
  dimension: days_played {type:number}
  dimension: day_of_week {}
  dimension: frequency {}
  dimension: get_status {
    type: number
  }
  dimension: addwatchlist_count {
    type: number
  }
  dimension: removewatchlist_count {
    type: number
  }
  dimension: error_count {
    type: number
  }
  dimension: view_count {
    type: number
  }
  dimension: promoters {}
  dimension: platform {}
  dimension: bates_play_day_1 {
    type: number
  }
  dimension: bates_play_day_2 {
    type: number
  }
  dimension: bates_play_day_3 {
    type: number
  }
  dimension: bates_play_day_4 {
    type: number
  }
  dimension: bates_play_day_5 {
    type: number
  }
  dimension: bates_play_day_6 {
    type: number
  }
  dimension: bates_play_day_7 {
    type: number
  }
  dimension: bates_play_day_8 {
    type: number
  }
  dimension: bates_play_day_9 {
    type: number
  }
  dimension: bates_play_day_10 {
    type: number
  }
  dimension: bates_play_day_11 {
    type: number
  }
  dimension: bates_play_day_12 {
    type: number
  }
  dimension: bates_play_day_13 {
    type: number
  }
  dimension: bates_play_day_14 {
    type: number
  }

  dimension: bates_day_1 {
    type: number
  }
  dimension: bates_day_2 {
    type: number
  }
  dimension: bates_day_3 {
    type: number
  }
  dimension: bates_day_4 {
    type: number
  }
  dimension: bates_day_5 {
    type: number
  }
  dimension: bates_day_6 {
    type: number
  }
  dimension: bates_day_7 {
    type: number
  }
  dimension: bates_day_8 {
    type: number
  }
  dimension: bates_day_9 {
    type: number
  }
  dimension: bates_day_10 {
    type: number
  }
  dimension: bates_day_11 {
    type: number
  }
  dimension: bates_day_12 {
    type: number
  }
  dimension: bates_day_13 {
    type: number
  }
  dimension: bates_day_14 {
    type: number
  }

  dimension: heartland_play_day_1 {
    type: number
  }
  dimension: heartland_play_day_2 {
    type: number
  }
  dimension: heartland_play_day_3 {
    type: number
  }
  dimension: heartland_play_day_4 {
    type: number
  }
  dimension: heartland_play_day_5 {
    type: number
  }
  dimension: heartland_play_day_6 {
    type: number
  }
  dimension: heartland_play_day_7 {
    type: number
  }
  dimension: heartland_play_day_8 {
    type: number
  }
  dimension: heartland_play_day_9 {
    type: number
  }
  dimension: heartland_play_day_10 {
    type: number
  }
  dimension: heartland_play_day_11 {
    type: number
  }
  dimension: heartland_play_day_12 {
    type: number
  }
  dimension: heartland_play_day_13 {
    type: number
  }
  dimension: heartland_play_day_14 {
    type: number
  }

  dimension: heartland_day_1 {
    type: number
  }
  dimension: heartland_day_2 {
    type: number
  }
  dimension: heartland_day_3 {
    type: number
  }
  dimension: heartland_day_4 {
    type: number
  }
  dimension: heartland_day_5 {
    type: number
  }
  dimension: heartland_day_6 {
    type: number
  }
  dimension: heartland_day_7 {
    type: number
  }
  dimension: heartland_day_8 {
    type: number
  }
  dimension: heartland_day_9 {
    type: number
  }
  dimension: heartland_day_10 {
    type: number
  }
  dimension: heartland_day_11 {
    type: number
  }
  dimension: heartland_day_12 {
    type: number
  }
  dimension: heartland_day_13 {
    type: number
  }
  dimension: heartland_day_14 {
    type: number
  }

  dimension: other_play_day_1 {
    type: number
  }
  dimension: other_play_day_2 {
    type: number
  }
  dimension: other_play_day_3 {
    type: number
  }
  dimension: other_play_day_4 {
    type: number
  }
  dimension: other_play_day_5 {
    type: number
  }
  dimension: other_play_day_6 {
    type: number
  }
  dimension: other_play_day_7 {
    type: number
  }
  dimension: other_play_day_8 {
    type: number
  }
  dimension: other_play_day_9 {
    type: number
  }
  dimension: other_play_day_10 {
    type: number
  }
  dimension: other_play_day_11 {
    type: number
  }
  dimension: other_play_day_12 {
    type: number
  }
  dimension: other_play_day_13 {
    type: number
  }
  dimension: other_play_day_14 {
    type: number
  }

  dimension: other_day_1 {
    type: number
  }
  dimension: other_day_2 {
    type: number
  }
  dimension: other_day_3 {
    type: number
  }
  dimension: other_day_4 {
    type: number
  }
  dimension: other_day_5 {
    type: number
  }
  dimension: other_day_6 {
    type: number
  }
  dimension: other_day_7 {
    type: number
  }
  dimension: other_day_8 {
    type: number
  }
  dimension: other_day_9 {
    type: number
  }
  dimension: other_day_10 {
    type: number
  }
  dimension: other_day_11 {
    type: number
  }
  dimension: other_day_12 {
    type: number
  }
  dimension: other_day_13 {
    type: number
  }
  dimension: other_day_14 {
    type: number
  }

  dimension: bates_duration_day_1 {
    type: number
  }
  dimension: bates_duration_day_2 {
    type: number
  }
  dimension: bates_duration_day_3 {
    type: number
  }
  dimension: bates_duration_day_4 {
    type: number
  }
  dimension: bates_duration_day_5 {
    type: number
  }
  dimension: bates_duration_day_6 {
    type: number
  }
  dimension: bates_duration_day_7 {
    type: number
  }
  dimension: bates_duration_day_8 {
    type: number
  }
  dimension: bates_duration_day_9 {
    type: number
  }
  dimension: bates_duration_day_10 {
    type: number
  }
  dimension: bates_duration_day_11 {
    type: number
  }
  dimension: bates_duration_day_12 {
    type: number
  }
  dimension: bates_duration_day_13 {
    type: number
  }
  dimension: bates_duration_day_14 {
    type: number
  }
  dimension: heartland_duration_day_1 {
    type: number
  }
  dimension: heartland_duration_day_2 {
    type: number
  }
  dimension: heartland_duration_day_3 {
    type: number
  }
  dimension: heartland_duration_day_4 {
    type: number
  }
  dimension: heartland_duration_day_5 {
    type: number
  }
  dimension: heartland_duration_day_6 {
    type: number
  }
  dimension: heartland_duration_day_7 {
    type: number
  }
  dimension: heartland_duration_day_8 {
    type: number
  }
  dimension: heartland_duration_day_9 {
    type: number
  }
  dimension: heartland_duration_day_10 {
    type: number
  }
  dimension: heartland_duration_day_11 {
    type: number
  }
  dimension: heartland_duration_day_12 {
    type: number
  }
  dimension: heartland_duration_day_13 {
    type: number
  }
  dimension: heartland_duration_day_14 {
    type: number
  }
  dimension: other_duration_day_1 {
    type: number
  }
  dimension: other_duration_day_2 {
    type: number
  }
  dimension: other_duration_day_3 {
    type: number
  }
  dimension: other_duration_day_4 {
    type: number
  }
  dimension: other_duration_day_5 {
    type: number
  }
  dimension: other_duration_day_6 {
    type: number
  }
  dimension: other_duration_day_7 {
    type: number
  }
  dimension: other_duration_day_8 {
    type: number
  }
  dimension: other_duration_day_9 {
    type: number
  }
  dimension: other_duration_day_10 {
    type: number
  }
  dimension: other_duration_day_11 {
    type: number
  }
  dimension: other_duration_day_12 {
    type: number
  }
  dimension: other_duration_day_13 {
    type: number
  }
  dimension: other_duration_day_14 {
    type: number
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
        , min_rel_progress = 0.0005
        , max_iterations = 99
        ) AS
      SELECT
         * EXCEPT(customer_id,state)
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
          (SELECT * FROM ${testing_input.SQL_TABLE_NAME}), struct(0.45 as threshold));;
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
        (SELECT * FROM ${testing_input.SQL_TABLE_NAME}));;
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
