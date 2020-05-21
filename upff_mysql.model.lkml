connection: "upff_mysql"

include: "mysql_roku_firstplays.view"
include: "mysql_email_campaigns.view"
include: "mysql_get_email_automation_emails.view"
include: "mysql_get_email_automations.view"

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

explore: mysql_roku_firstplays {
  label: "Roku Firstplays"
}

explore: mysql_email_campaigns {

}

explore: mysql_get_email_automation_emails {
  label: "Email Automation Emails"
}

explore: mysql_get_email_automations {
  label: "Email Automations"
}
