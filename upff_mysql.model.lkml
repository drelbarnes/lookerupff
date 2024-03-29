connection: "upff_mysql"

include: "mysql_roku_firstplays.view"
include: "mysql_email_campaigns.view"
include: "mysql_upff_category_items.view"


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


explore: mysql_upff_category_items {
  label: "Category Items"
}


explore: mysql_roku_firstplays {
  label: "Roku Firstplays"
}

explore: mysql_email_campaigns {

}
