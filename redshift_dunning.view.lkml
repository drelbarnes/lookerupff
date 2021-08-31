view: redshift_dunning {
  derived_table: {
    sql:

/* associated explore name: "Dunning Renewal (8-30-21)"

/*
* Results: Get users in July whose last two topics or status events are charge failed and renewed.
* This query will assist in revealing the recovery rate from the Vimeo OTT dunning process.
* Fetch the last two purchase or topic events for web users in the month of July.
* Convert topic events to numeric and loop on row number timestamp
*/

/* selects web customers in month of Jul21 */
with mc as
(
    select *,
    row_number() over (partition by user_id order by timestamp desc) seqnum
    from http_api.purchase_event mc
    WHERE date(timestamp) between '2021-07-01' AND '2021-07-31' AND platform = 'web'
),

/* selects two most recent topics and creates numeric status flag */
a as
(
    select ROW_NUMBER() OVER (ORDER BY timestamp DESC) row_num,
    user_id,

    CASE
      WHEN topic = 'customer.product.charge_failed' THEN 1
      WHEN topic = 'customer.product.expired' THEN 2
      WHEN topic = 'customer.product.free_trial_expired' THEN 3
      WHEN topic = 'customer.product.renewed' THEN 4
      ELSE 5
    END AS status,

    status_date,
    created_at,
    subscription_status,
    seqnum

    from mc
    WHERE seqnum IN (1,2)
),

/* selects all web customers who payment method failed */
cf as
(
    select * from a
    WHERE status = 1
),

/*
*  Fetch users with a topic or status event equal to 1 (charge failed) and JOIN with topic or status event equal to 4 (renewed)
*/

master as
(
select
  datediff(day, cf.status_date, a.status_date) as DiffinDay,
  datediff(day, cf.created_at, a.status_date) as Tenure,
  cf.user_id,
  cf.subscription_status
from cf
INNER JOIN a
ON cf.user_id = a.user_id
WHERE a.status = 4
order by cf.user_id
)

select
  mc.charge_status,
  mc.plan,
  mc.event_status,
  mc.topic,
  mc.subscription_price,
  mc.subscription_frequency,
  mc.seqnum,
  master.DiffinDay,
  master.Tenure,
  master.user_id,
  master.subscription_status
from mc
left join master
on mc.user_id=master.user_id
order by subscription_price


/* what are attributes of people who are successful recoveries as opposed to those who allow expiration to continue? */


;;

}
dimension: subscriber_type {
  sql: case
  when ${TABLE}.tenure <= 14 then 'Trial'
  when ${TABLE}.tenure > 14 then 'Paid'
  else 'Missing'
  end ;;
}

}