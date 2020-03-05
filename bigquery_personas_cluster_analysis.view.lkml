include: "upff_google.model.lkml"
explore: training {}


view: training {
    derived_table: {
      explore_source: bigquery_personas_v2 {
        column: email {}
#         column: addwatchlist_recent {}
#         column: addwatchlist_older {}
        column: bates_recent {}
        column: bates_older {}
        column: churns {}
#         column: error_recent {}
#         column: error_older {}
        column: heartland_recent {}
        column: heartland_older {}
        column: marketing_optin {}
        column: other_recent {}
        column: other_older {}
        column: renewals {}
#         column: removewatchlist_recent {}
#         column: removewatchlist_older {}
        column: web {}
        column: android {}
        column: ios {}
        column: roku {}
        column: men {}
        column: women {}
      }
    }
    dimension: email {}
    dimension: web {type:number}
    dimension: ios {type:number}
    dimension: roku {type:number}
    dimension: android {type:number}
    dimension: men {type:number}
    dimension: women {type:number}

#     dimension: addwatchlist_recent {
#       type: number
#     }
#
#   dimension: addwatchlist_older {
#     type: number
#   }

    dimension: bates_recent {
      type: number
    }

  dimension: bates_older {
    type: number
  }

    dimension: churns {
      type: number
    }
#     dimension: error_recent {
#       type: number
#     }
#   dimension: error_older {
#     type: number
#   }

    dimension: heartland_recent {
      type: number
    }
  dimension: heartland_older {
    type: number
  }
    dimension: marketing_optin {
      type: number
    }

    dimension: other_recent {
      type: number
    }

  dimension: other_older {
    type: number
  }

    dimension: renewals {
      type: number
    }

#     dimension: removewatchlist_recent {
#       type: number
#     }
#   dimension: removewatchlist_older {
#     type: number
#   }


}

######################## MODEL #############################

view: cluster_model {
  derived_table: {
    datagroup_trigger:upff_google_datagroup
    sql_create:
      CREATE OR REPLACE MODEL ${SQL_TABLE_NAME}
      OPTIONS(model_type='kmeans',
              DISTANCE_TYPE = 'COSINE',
              STANDARDIZE_FEATURES=TRUE
        ) AS
      SELECT
         * EXCEPT(email)
      FROM ${training.SQL_TABLE_NAME};;
  }
}
######################## MODEL EVALUATION #############################
explore: model_evaluation {}
view: model_evaluation {
  derived_table: {
    sql: SELECT * FROM ml.EVALUATE(
          MODEL ${cluster_model.SQL_TABLE_NAME},
          (SELECT * FROM ${training.SQL_TABLE_NAME}));;
          }
    dimension: davies_bouldin_index {type:number}
    dimension: mean_squared_distance {type: number}
          }

######################## MODEL PREDICTIONS #############################
explore: cluster_prediction {}
view: cluster_prediction {
  derived_table: {
    sql: SELECT * FROM ml.PREDICT(
          MODEL ${cluster_model.SQL_TABLE_NAME},
          (SELECT * FROM ${training.SQL_TABLE_NAME}));;
  }

  dimension: email {}
#   dimension: addwatchlist_recent {}
#   dimension: addwatchlist_older {}
  dimension: bates_recent {}
  dimension: bates_older {}
  dimension: churns {}
#   dimension: error_recent {}
#   dimension: error_older {}
  dimension: heartland_recent {}
  dimension: heartland_older {}
  dimension: marketing_optin {}
  dimension: other_recent {}
  dimension: other_older {}
  dimension: renewals {}
#   dimension: removewatchlist_recent {}
#   dimension: removewatchlist_older {}
  dimension: web {}
  dimension: android {}
  dimension: ios {}
  dimension: roku {}
  dimension: men {}
  dimension: women {}

  measure: women_ {
    type: average
    sql: ${women} ;;
    value_format: "0.00"
  }

  measure: men_ {
    type: average
    sql: ${men} ;;
    value_format: "0.00"
  }

#   measure: addwatchlist_recent_ {
#     type: average
#     sql: ${addwatchlist_recent} ;;
#     value_format: "0.00"
#   }

#   measure: addwatchlist_older_ {
#     type: average
#     sql: ${addwatchlist_older} ;;
#     value_format:  "0.00"
#   }

  measure: bates_recent_ {
    type: average
    sql: ${bates_recent} ;;
    value_format:  "0.0"
  }

  measure: bates_older_ {
    type: average
    sql: ${bates_older} ;;
    value_format: "0.0"
  }

  measure: churns_ {
    type: average
    sql: ${churns} ;;
    value_format: "0.00"
  }
#   measure: error_recent_ {
#     type: average
#     sql: ${error_recent} ;;
#     value_format:  "0.00"
#   }
#   measure: error_older_ {
#     type: average
#     sql: ${error_older} ;;
#     value_format: "0.00"
#   }
  measure: heartland_recent_ {
    type: average
    sql: ${heartland_recent} ;;
    value_format:  "0.0"
  }
  measure: heartland_older_ {
    type: average
    sql: ${heartland_older} ;;
    value_format:  "0.0"
  }
  measure: marketing_optin_ {
    type: average
    sql: ${marketing_optin} ;;
    value_format: "0.00"
  }
  measure: other_recent_ {
    type: average
    sql: ${other_recent} ;;
    value_format:  "0.0"
  }
  measure: other_older_ {
    type: average
    sql: ${other_older} ;;
    value_format:  "0.0"
  }
  measure: renewals_ {
    type: average
    sql: ${renewals} ;;
    value_format: "0.0"
  }
  measure: web_ {
    type: average
    sql: ${web};;
  value_format: "0.00"}

  measure: ios_ {
      type: average
      sql: ${ios};;
    value_format: "0.00"}

  measure: android_ {
      type: average
      sql: ${android};;
    value_format: "0.00"}

  measure: roku_ {
      type: average
      sql: ${roku};;
    value_format: "0.00"}

#   measure: removewatchlist_recent_ {
#     type: average
#     sql: ${removewatchlist_recent} ;;
#     value_format: "0.00"
#   }
#   measure: removewatchlist_older_ {
#     type: average
#     sql: ${removewatchlist_older} ;;
#     value_format: "0.00"
#   }

  measure: count_ {
    type: count_distinct
    sql: ${email} ;;
  }


  dimension:centroid_id {type:string}
  dimension: nearest_centroids_distance {type:number}

  }
