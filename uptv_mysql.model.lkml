connection: "gilmore_the_merrier"

include: "/views/*.view.lkml"
# include all views in the views/ folder in this project
# include: "/**/*.view.lkml"                 # include all views in this project
# include: "my_dashboard.dashboard.lookml"   # include a LookML dashboard called my_dashboard

explore: uptv_gtm_users {
  label: "GilMORE The Merrier - Users"

  join: gtm_entries {
    type: left_outer
    relationship: one_to_many
    sql_on: ${uptv_gtm_users.id}=${gtm_entries.user_id} ;;
  }

  join: gtm_entries_used {
    type: left_outer
    relationship: one_to_many
    sql: ${uptv_gtm_users.id}=${gtm_entries_used.user_id} ;;
  }
}

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
