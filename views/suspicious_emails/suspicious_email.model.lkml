connection: "upff"

include: "/views/suspicious_emails/suspicious_email.view.lkml"                # include all views in the

explore: suspicious_email {
  label: "Suspicious Emails"
}
