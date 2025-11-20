# Define the database connection to be used for this model.
connection: "upff"

datagroup: linear_tv_schedule_default_datagroup {
  sql_trigger: SELECT MAX(updated_at) FROM customers.up_airtable_reports ;;
  max_cache_age: "24 hours"
}

# ---------- INCLUDE ALL VIEWS ----------

include: "/views/up_airtable_reports.view.lkml"
include: "/views/aspire_airtable_reports.view.lkml"
include: "/views/ovation_airtable_reports.view.lkml"


# ---------- EXPLORES ----------

# UP
explore: up_airtable_reports {
  label: "Linear TV Schedule – UP"
  description: "Explore UPtv programming schedule from Airtable (UP Linear TV)"
}

# Aspire
explore: aspire_airtable_reports {
  label: "Linear TV Schedule – Aspire"
  description: "Explore Aspire programming schedule from Airtable (Aspire Linear TV)"
}

# Ovation
explore: ovation_airtable_reports {
  label: "Linear TV Schedule – Ovation"
  description: "Explore Ovation programming schedule from Airtable (Ovation Linear TV)"
}
