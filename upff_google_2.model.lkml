connection: "google_bigquery_db_2"

include: "bigquery_vimeott_webinar_ads.view.lkml"
include: "bigquery_vimeott_webinar_organic_paid_web.view.lkml"
include: "bigquery_vimeott_webinar_top_web.view.lkml"
include: "bigquery_firebase_events.view.lkml"

explore: bigquery_firebase_events {
  label: "Push Notifications"
}
explore: bigquery_vimeott_webinar_ads{}
explore: bigquery_vimeott_webinar_organic_paid_web {}
explore: bigquery_vimeott_webinar_top_web {}
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
