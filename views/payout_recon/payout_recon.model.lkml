connection: "google_bigquery_db"

include: "/views/payout_recon/paypal.view.lkml"
include: "/views/payout_recon/paypal_old.view.lkml"
include: "/views/payout_recon/config.view.lkml"
include: "/views/payout_recon/paypal_recon.view.lkml"
include: "/views/payout_recon/stripe.view.lkml"
include: "/views/payout_recon/stripe_recon.view.lkml"
include: "/views/payout_recon/braintree.view.lkml"
include: "/views/payout_recon/braintree_recon.view.lkml"



explore: paypal {
  label: "Payout Recon"
}

explore: paypal_recon {
  label: "Paypal Payout Recon"
}

explore: stripe_recon {
  label: "Stripe Payout Recon"
}

explore: stripe {
  label: "Stripe Payout"
}

explore: braintree {
  label: "Braintree Recon"
}

explore: braintree_recon {
  label: "Braintree Payout Recon"
}
