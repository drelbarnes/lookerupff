# Define the database connection to be used for this model.
connection: "upff"

# include all the views
include: "/views/up_airtable_reports.view.lkml"

# Datagroups define a caching policy for an Explore. To learn more,
# use the Quick Help panel on the right to see documentation.


explore: up_airtable_reports {
  label: "Linear TV Schedule"
  description: "Explore programming schedule data from Airtable (UPtv Linear TV)"
}
