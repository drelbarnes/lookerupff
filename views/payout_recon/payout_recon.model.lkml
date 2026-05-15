connection: "google_bigquery_db"

include: "/views/payout_recon/paypal.view.lkml"
include: "/views/payout_recon/paypal_old.view.lkml"
include: "/views/payout_recon/config.view.lkml"


explore: paypal {
  label: "Payout Recon"
}
