connection: "google_bigquery_db"

include: "*.view.lkml"

datagroup: google_bigquery_db_default_datagroup {
  # sql_trigger: SELECT MAX(id) FROM etl_log;;
  max_cache_age: "1 hour"
}

persist_with: google_bigquery_db_default_datagroup


explore: bigquery_subscribers {

  label: "Subscribers"

}

explore: bigquery_derived_all_firstplay { label: "All First Play"

  join: bigquery_subscribers {
    type:  left_outer
    sql_on: ${bigquery_subscribers.customer_id} = SAFE_CAST(${bigquery_derived_all_firstplay.user_id} AS INT64);;
    relationship: one_to_one
  }

  }
explore: bigquery_android_firstplay {

  label: "First Play"

    join: bigquery_subscribers {
      type:  inner
      sql_on: ${bigquery_subscribers.customer_id} = SAFE_CAST(${bigquery_android_firstplay.user_id} AS INT64);;
      relationship: one_to_many
    }

}


# include all views in this project
# include: "my_dashboard.dashboard.lookml"   # include a LookML dashboard called my_dashboard

# # Select the views that should be a part of this model,
# # and define the joins that connect them together.
#
# explore: order_items {
#   join: orders {
#     relationship: many_to_one
#     sql_on: ${orders.id} = ${order_items.order_id} ;;
#   }
#
#   join: users {
#     relationship: many_to_one
#     sql_on: ${users.id} = ${orders.user_id} ;;
#   }
# }
