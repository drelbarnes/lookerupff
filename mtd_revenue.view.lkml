view: mtd_revenue {
  derived_table: {
    sql:
with

a as (select *,1 as matching from svod_titles.date_list as a where a.date<=current_date),

b as (select customer_id,  customer_created_at, event_created_at, status, platform, 1 as matching from customers.customers),

c as (select customer_id, customer_created_at, event_created_at, status, a.date, datediff('day',customer_created_at,a.date) as daysdiff, platform
      from a inner join b on a.matching=b.matching
      where datediff('day',customer_created_at,a.date)>-1 and (status!='cancelled' or datediff('day',a.date,event_created_at)>=0)),

d as (select *, case
        when daysdiff=15 and platform='android' then .7*5.99
        when daysdiff=15 and platform='android_tv' then .7*5.99
        when daysdiff=15 and platform='ios' then .7*5.99
        when daysdiff=15 and platform='tvos' then .7*5.99
        when daysdiff=15 and platform='roku' then .8*5.99
        when daysdiff=15 and platform='web' then 5.99
        when daysdiff>15 and ((daysdiff-15)%30)=0 and platform='android' then .7*5.99
        when daysdiff>15 and ((daysdiff-15)%30)=0 and platform='android_tv' then .7*5.99
        when daysdiff>15 and ((daysdiff-15)%30)=0 and platform='ios' then .7*5.99
        when daysdiff>15 and ((daysdiff-15)%30)=0 and platform='tvos' then .7*5.99
        when daysdiff>15 and ((daysdiff-15)%30)=0 and platform='roku' then .8*5.99
        when daysdiff>15 and ((daysdiff-15)%30)=0 and platform='web' then 5.99
        else null end as revenue
      from c),

e as (select d.date,sum(revenue) as revenue, case when datepart(month,date(d.date))=6 then 3704.25 else null end as target from d where revenue is not null group by d.date)

select e.date, SUM(revenue) OVER (PARTITION by cast(datepart(month,date(e.date)) as varchar) order by date(e.date) asc rows between unbounded preceding and current row) AS running_revenue,
               SUM(target) OVER (PARTITION by cast(datepart(month,date(e.date)) as varchar) order by date(e.date) asc rows between unbounded preceding and current row) AS running_target from e
 ;;
  }

dimension: date {
  type: date
  sql: ${TABLE}.date ;;
}

measure: running_revenue {
  type: sum
  sql: ${TABLE}.running_revenue ;;
  value_format_name: usd
}

  measure: running_target {
    type: sum
    sql: ${TABLE}.running_target ;;
    value_format_name: usd
  }
}
