view: mvpds {
  derived_table: {
    sql: with amazon1 as
(select date, amazon
from svod_titles.mvpd_subs
where amazon is not null
order by 1 desc
limit 1),

allothers as
(select date, comcast+cox+"dish/sling" as others
from svod_titles.mvpd_subs
where comcast is not null
order by 1 desc
limit 1),

d2c as
(select analytics_timestamp,
       total_free_trials+total_paying as total_subs
from php.get_analytics
where date(sent_at)=current_date
order by 1 desc
limit 1)


select amazon + others + total_subs as total_subs
from amazon1,allothers,d2c ;;
  }

  measure: total_subs{
    type: number
    sql: ${TABLE}.total_subs ;;
  }
}
