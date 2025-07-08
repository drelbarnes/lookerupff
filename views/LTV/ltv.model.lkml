connection: "google_bigquery_db"

include: "/views/LTV/ltv.view.lkml"                # include all views in the views/ folder

explore: ltv {
  label: "Chargebee conversion rate"


}
