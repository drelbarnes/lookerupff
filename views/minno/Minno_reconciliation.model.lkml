connection: "upff"

include: "/views/minno/reconciliation.view.lkml"
include: "/views/minno/minno_schema.view.lkml"

explore: reconciliation {
  label: "Minno reconciliation"

  join: minno_schema {
    type: left_outer
    sql_on:  ${minno_schema.report_date_date}=${reconciliation.created_at}  ;;
    relationship: many_to_many
  }
}

explore: minno_schema {
  label: "Minno Schema"
}
