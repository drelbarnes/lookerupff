view: mtd_revenue {
  derived_table: {
    sql: -- select datepart(day,date(event_created_At)),event_created_at from customers.customers


with a as

(select *, case
        when status='enabled' and DATEDIFF('day', customer_created_at, event_created_at)=15 and platform='android' then .7*5.99
        when status='enabled' and DATEDIFF('day', customer_created_at, event_created_at)=15 and platform='android_tv' then .7*5.99
        when status='enabled' and DATEDIFF('day', customer_created_at, event_created_at)=15 and platform='ios' then .7*5.99
        when status='enabled' and DATEDIFF('day', customer_created_at, event_created_at)=15 and platform='tvos' then .7*5.99
        when status='enabled' and DATEDIFF('day', customer_created_at, event_created_at)=15 and platform='roku' then .8*5.99
        when status='enabled' and DATEDIFF('day', customer_created_at, event_created_at)=15 and platform='web' then 5.99
        when status='enabled' and DATEDIFF('day', customer_created_at, event_created_at)>15 and (DATEDIFF('day', customer_created_at, event_created_at)-15)%30=0 and platform='android' then .7*5.99
        when status='enabled' and DATEDIFF('day', customer_created_at, event_created_at)>15 and (DATEDIFF('day', customer_created_at, event_created_at)-15)%30=0 and platform='android_tv' then .7*5.99
        when status='enabled' and DATEDIFF('day', customer_created_at, event_created_at)>15 and (DATEDIFF('day', customer_created_at, event_created_at)-15)%30=0 and platform='ios' then .7*5.99
        when status='enabled' and DATEDIFF('day', customer_created_at, event_created_at)>15 and (DATEDIFF('day', customer_created_at, event_created_at)-15)%30=0 and platform='tvos' then .7*5.99
        when status='enabled' and DATEDIFF('day', customer_created_at, event_created_at)>15 and (DATEDIFF('day', customer_created_at, event_created_at)-15)%30=0 and platform='roku' then .8*5.99
        when status='enabled' and DATEDIFF('day', customer_created_at, event_created_at)>15 and (DATEDIFF('day', customer_created_at, event_created_at)-15)%30=0 and platform='web' then 5.99
        else null end as revenue
from customers.customers
order by event_created_at desc),

b as

(select event_created_at, sum(revenue) as revenue, case when datepart(month,date(event_created_At))=6 then 675.29 else null end as target
from a
group by event_created_at)

select date(event_created_at) as event_created_at,
SUM(revenue) OVER (PARTITION by cast(datepart(month,date(event_created_At)) as varchar) order by date(event_created_at) asc rows between unbounded preceding and current row) AS running_revenue,
SUM(target) OVER (PARTITION by cast(datepart(month,date(event_created_At)) as varchar) order by date(event_created_at) asc rows between unbounded preceding and current row) AS running_target
from b
group by date(event_created_At), revenue,target
 ;;
  }

dimension: event_created_at {
  type: date
  sql: ${TABLE}.event_created_at ;;
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
