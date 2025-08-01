connection: "google_bigquery_db"

include: "/views/bigquery/active_users.view.lkml"


explore:active_users {
  label: "Bigquery active users"
}
