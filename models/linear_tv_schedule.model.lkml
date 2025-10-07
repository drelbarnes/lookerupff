# Define the database connection to be used for this model.
connection: "upff"

datagroup: linear_tv_schedule_default_datagroup {
  sql_trigger: SELECT MAX(updated_at) FROM customers.up_airtable_reports ;;
  max_cache_age: "24 hours"
}

# include all the views
include: "/views/up_airtable_reports.view.lkml"

# Datagroups define a caching policy for an Explore. To learn more,
# use the Quick Help panel on the right to see documentation.


explore: up_airtable_reports {
  label: "Linear TV Schedule"
  description: "Explore programming schedule data from Airtable (UPtv Linear TV)"
}
