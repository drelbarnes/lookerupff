connection: "upff"

include: "/views/suspicious_emails/suspicious_email.view.lkml"                # include all views in the
include: "/views/suspicious_emails/suspicious_email_gaither.view.lkml"                # include all views in the

explore: suspicious_email {
  label: "Suspicious Emails"
}

explore: suspicious_email_gaither {
  label: "Suspicious Emails"
}
