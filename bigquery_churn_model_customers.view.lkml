view: bigquery_churn_model_customers {
  derived_table: {
    sql:
with a as
(SELECT -1+ROW_NUMBER() OVER() AS num
FROM UNNEST((SELECT SPLIT(FORMAT("%600s", ""),'') AS h FROM (SELECT NULL))) AS pos
ORDER BY num),

b as
(select *,
       case when status in ("enabled") then floor(date_diff(current_date, date_add(date(customer_created_at), interval 14 day), month))-1
            when status in ("cancelled","disabled","refunded","expired") then floor(date_diff(date(event_created_at), date_add(date(customer_created_at), interval 14 day), month))-1
       end as months_since_conversion,
       case when status in ("enabled") then date_diff(current_date, date_add(date(customer_created_at), interval 14 day), day)
            when status in ("cancelled","disabled","refunded","expired") then date_diff(date(event_created_at), date_add(date(customer_created_at), interval 14 day), day)
       end as days_since_conversion
from customers.subscribers
where status is not null or status not in ("free_trial","paused")
order by status),

c as
(select customer_id,
       max(num) as max_num
from b, a
where months_since_conversion>=num
group by customer_id),

d as
(select b.*,
        num
from b, a
where months_since_conversion>=num
order by customer_id,num)

select d.*,
       date_add(date_add(date(customer_created_at), interval 14 day),interval num*30 day) as start_date,
       case when status="enabled" or num<max_num then date_add(date_add(date(customer_created_at), interval 14 day),interval (num+1)*30 day)
            when status<>"enabled" and num=max_num then date(event_created_at) end as end_date,
       case when status="enabled" or num<max_num then 0
            when status<>"enabled" and num=max_num then 1 end as churn_status
from d inner join c on d.customer_id=c.customer_id
order by customer_id, num;;}

dimension: start_date {
  type: date
  sql: ${TABLE}.start_date ;;
}

dimension: end_date {
  type: date
  sql: ${TABLE}.end_date ;;
}

dimension: churn_status {
  type: number
  sql: ${TABLE}.churn_status ;;
}

dimension: days_since_conversion {
  type: number
  sql: ${TABLE}.days_since_conversion ;;
}

dimension: months_since_conversion {
  type: number
  sql: ${TABLE}.months_since_conversion ;;
}

  dimension: action {
    type: string
    sql: ${TABLE}.action ;;
  }

  dimension: action_type {
    type: string
    sql: ${TABLE}.action_type ;;
  }

  dimension: campaign {
    type: string
    sql: ${TABLE}.campaign ;;
  }

  dimension: city {
    type: string
    sql: ${TABLE}.city ;;
  }

  dimension: country {
    type: string
    map_layer_name: countries
    sql: ${TABLE}.country ;;
  }

  dimension: coupon_code {
    type: string
    sql: ${TABLE}.coupon_code ;;
  }

  dimension: coupon_code_id {
    type: string
    sql: ${TABLE}.coupon_code_id ;;
  }

  dimension: current_date{
    type: date
    sql: current_date;;
  }

  dimension_group: customer_created {
    type: time
    timeframes: [
      raw,
      time,
      date,
      week,
      month,
      quarter,
      year
    ]
    sql: ${TABLE}.customer_created_at ;;
  }

  dimension: customer_id {
    type: number
    primary_key: yes
    tags: ["user_id"]
    sql: ${TABLE}.customer_id ;;
  }

  dimension: email {
    type: string
    sql: ${TABLE}.email ;;
  }

  dimension_group: event_created {
    type: time
    timeframes: [
      raw,
      time,
      date,
      week,
      month,
      quarter,
      year
    ]
    sql: ${TABLE}.event_created_at ;;
  }

  dimension: first_name {
    type: string
    sql: ${TABLE}.first_name ;;
  }

  dimension: frequency {
    type: string
    sql: ${TABLE}.frequency ;;
  }

  dimension: last_name {
    type: string
    sql: ${TABLE}.last_name ;;
  }

  dimension: marketing_opt_in {
    type: number
    sql: ${TABLE}.marketing_opt_in ;;
  }

  dimension: platform {
    type: string
    sql: ${TABLE}.platform ;;
  }

  dimension: product_id {
    type: number
    sql: ${TABLE}.product_id ;;
  }

  dimension: product_name {
    type: string
    sql: ${TABLE}.product_name ;;
  }

  dimension: promotion_code {
    type: string
    sql: ${TABLE}.promotion_code ;;
  }

  dimension: promotion_id {
    type: number
    sql: ${TABLE}.promotion_id ;;
  }

  dimension: referrer {
    type: string
    sql: ${TABLE}.referrer ;;
  }

  dimension: state {
    type: string
    sql: ${TABLE}.state ;;
  }

  dimension: status {
    type: string
    sql: ${TABLE}.status ;;
  }

  measure: count {
    type: count
    drill_fields: [last_name, product_name, first_name]
  }



  }
