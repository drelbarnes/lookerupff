view: all_firstplay {
  derived_table: {
    sql:

    (with a as
        (select a.timestamp,
                b.date as release_date,
                trim(upper(split_part(series,'|',1))) as series,
                collection,
                case when series is null and upper(collection)=upper(title) then 'movie'
                     when series is not null then 'series' else 'other' end as type,
                cast(a.video_id as int) as video_id,
                trim((split_part(season,'Season',2))) as season,
                episode,trim((title)) as title,
                user_id,
                c.platform,
                'Android' as source
         from android.firstplay as a left join svod_titles.titles_id_mapping as b on a.video_id = b.id left join customers.customers as c
         on a.user_id = c.customer_id
         union all
         select a.timestamp,
                b.date as release_date,
                trim(upper(split_part(series,'|',1))) as series,
                collection,
                case when series is null and upper(collection)=upper(title) then 'movie'
                     when series is not null then 'series' else 'other' end as type,
                cast(a.video_id as int) as video_id,
                trim((split_part(season,'Season',2))) as season,
                episode,trim((title)) as title,
                user_id,
                c.platform,
                'iOS' as source
         from ios.firstplay as a left join svod_titles.titles_id_mapping as b on a.video_id = b.id left join customers.customers as c
         on a.user_id = c.customer_id
         union all
         select a.timestamp,
                b.date as release_date,
                trim(upper(split_part(series,'|',1))) as series,
                collection,
                case when series is null and upper(collection)=upper(b.title) then 'movie'
                     when series is not null then 'series' else 'other' end as type,
                cast(b.id as int) as video_id,
                trim((split_part(season,'Season',2))) as season,
                episode,trim((split_part(a.title,'-',1))) as title,
                user_id,
                c.platform,
                'Web' as source
         from javascript.firstplay as a left join svod_titles.titles_id_mapping as b on trim(upper(b.title)) = trim(upper(split_part(a.title,'-',1)))
         left join customers.customers as c
         on a.user_id = c.customer_id
        )

select a.*
from a );;
  }

  dimension: current_date {
    type: date
    sql: current_date ;;
  }

  dimension: title {
    type: string
    sql: ${TABLE}.title;;
  }

  dimension: type {
    type: string
    sql: ${TABLE}.type ;;
  }

  dimension: series {
    type: string
    sql: ${TABLE}.series ;;
  }

  dimension: season {
    type: string
    sql: ${TABLE}.season ;;
  }

  dimension: episode {
    type: string
    sql: ${TABLE}.episode ;;
  }

  dimension: status {
    type: string
    sql: ${TABLE}.status ;;
  }

  dimension: collection {
    type: string
    sql: ${TABLE}.collection ;;
  }

  dimension: platform {
    type: string
    sql: ${TABLE}.platform ;;
  }

  dimension: source {
    type: string
    sql: ${TABLE}.source ;;
  }

  dimension: user_id {
    primary_key: yes
    tags: ["user_id"]
    type: string
    sql: ${TABLE}.user_id ;;
  }

  dimension: video_id {
    type: string
    sql: ${TABLE}.video_id ;;
  }

  dimension_group: timestamp {
    type: time
    timeframes: [
      raw,
      time,
      hour_of_day,
      date,
      week,
      month,
      quarter,
      year
    ]
    sql: ${TABLE}.timestamp ;;
  }

  dimension: release_date {
    type: date
    sql: ${TABLE}.release_date ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  measure: number_of_platforms_by_user {
    type: count_distinct
    sql: ${source};;
  }

  measure: user_count {
    type: count_distinct
    sql: ${user_id} ;;
  }

  measure: plays_count {
    type: count_distinct
    sql: ${user_id}||cast(${timestamp_date} as varchar)||${video_id} ;;
  }

  measure: views_per_user {
    type: number
    sql: 1.00*${count}/${user_count} ;;
    value_format: "0.00"
  }


# ----- Sets of fields for drilling ------
  set: detail {
    fields: [
      title,
      platform,
      user_id
    ]
  }

# ------
# Filters
# ------

## filter determining time range for all "A" measures
  filter: time_a {
    type: date_time
  }

## flag for "A" measures to only include appropriate time range
  dimension: group_a {
    hidden: no
    type: yesno
    sql: {% condition time_a %} ${release_date} {% endcondition %}
      ;;
  }

## filter determining time range for all "B" measures
  filter: time_b {
    type: date_time
  }

## flag for "B" measures to only include appropriate time range
  dimension: group_b {
    hidden: no
    type: yesno
    sql: {% condition time_b %} ${release_date} {% endcondition %}
      ;;
  }

  measure: count_a {
    type: count
    filters: {
      field: group_a
      value: "yes"
    }
  }

  measure: count_b {
    type: count
    filters: {
      field: group_b
      value: "yes"
    }
  }

  parameter: date_granularity {
    type: string
    allowed_value: { value: "Day" }
    allowed_value: { value: "Week"}
    allowed_value: { value: "Month" }
    allowed_value: { value: "Quarter" }
    allowed_value: { value: "Year" }
  }

  dimension: date {
    label_from_parameter: date_granularity
    sql:
       CASE
         WHEN {% parameter date_granularity %} = 'Day' THEN
           ${timestamp_date}::VARCHAR
         WHEN {% parameter date_granularity %} = 'Week' THEN
           ${timestamp_week}::VARCHAR
         WHEN {% parameter date_granularity %} = 'Month' THEN
           ${timestamp_month}::VARCHAR
         WHEN {% parameter date_granularity %} = 'Quarter' THEN
           ${timestamp_quarter}::VARCHAR
         WHEN {% parameter date_granularity %} = 'Year' THEN
           ${timestamp_year}::VARCHAR
         ELSE
           NULL
       END ;;
  }


}
