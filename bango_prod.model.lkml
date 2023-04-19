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
    relationship: one_to_many
  }
}

explore: statuses {
  label: "Bango Statuses"

  join: users {
    type: left_outer
    sql_on: ${statuses.bango_user_id} = ${users.bango_user_id} ;;
    relationship: many_to_one
  }
}
