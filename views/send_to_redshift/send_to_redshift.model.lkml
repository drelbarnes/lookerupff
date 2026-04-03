connection: "google_bigquery_db"

include: "/views/send_to_redshift/send_to_redshift.view.lkml"

explore: send_to_redshift {}
