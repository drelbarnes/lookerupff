connection: "admin_bang_prod"

include: "/views/bango_views/*.view.lkml"                # include all views in the views/ folder in this project

explore: users {
  label: "Bango Prod"

  join: partners {
    type: inner
    sql_on: ${users.partner_id} = ${partners.id} ;;
    relationship: many_to_many
  }

  join: statuses {
    type: inner
    sql_on: ${users.bango_user_id} = ${statuses.bango_user_id} ;;
    relationship: many_to_one
  }
}
