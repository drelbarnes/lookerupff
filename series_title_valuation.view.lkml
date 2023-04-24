view: series_title_valuation {
    derived_table: {
      sql:
        --series title valuation analysis v2.0 4-24-2023

        --date range for allfirstplay query
        --declare g_begin_dt date default '2021-01-01';
        --declare g_end_dt date default '2023-04-01';
        --declare l_begin_dt date default '2021-10-01';
        --declare l_end_dt date default '2023-04-01';

        with

        user_first_play as
        (
          with

          play_data_global as
          (
          select
            *
          from allfirstplay.p0
          where user_id <> '0'
          and regexp_contains(user_id, r'^[0-9]*$')
          and date(timestamp) >= '2021-01-01'
          and date(timestamp) <= current_date()
          ),

          plays_most_granular as
          (
          select
            user_id,
            row_number() over (partition by user_id, date(timestamp), video_id order by date(timestamp)) as min_count,
            timestamp,
            collection,
            type,
            video_id,
            series,
            title,
            source,
            episode,
            email,
            winback
          from play_data_global
          order by
            user_id,
            date(timestamp),
            video_id,
          min_count
          ),

          plays_max_duration as
          (
          select
            user_id,
            video_id,
            date(timestamp) as date,
            max(min_count) as min_count
          from plays_most_granular
          group by 1,2,3
          ),

          plays_less_granular as
          (
          select
            a.*,
            row_number() over (partition by a.user_id order by a.timestamp) as play_number
          from plays_most_granular as a
          inner join plays_max_duration as b
          on a.user_id = b.user_id
          and a.video_id = b.video_id
          and date(a.timestamp) = b.date
          and a.min_count = b.min_count
          )

        select
          *
        from plays_less_granular
        where series in (
          {% parameter c_name1 %},
          {% parameter c_name2 %},
          {% parameter c_name3 %},
          {% parameter c_name4 %},
          {% parameter c_name5 %},
          {% parameter c_name6 %}
            )
        ),

        series_first_plays as
          (
          with

          first_play_p0 as
          (
          select
            *
          from user_first_play
          where date(timestamp) between '2021-11-01' and current_date()
          ),

          first_play_p1 as
          (
          select
            user_id,
            min_count,
            date(timestamp) as date_stamp,
            series
          from first_play_p0
          where play_number = 1
          ),

          first_play_p2 as
          (
          select
            series,
            count(distinct user_id) as number_first_plays
          from first_play_p1
          group by series
          )

        select * from first_play_p2
        ),

        title_durations as
        (
          with

          series_title_lookup as
          (
          select distinct
            collection,
            title,
            video_id
          from user_first_play
          ),

          durations_p0 as
          (
          select
            title,
            video_id,
            round(duration_seconds/60,0) as duration,
            metadata_series_name,
            date(timestamp) as date_stamp,
            row_number() over (partition by title order by timestamp desc) as event_num
          from php.get_titles
          ),

          durations_p1 as
          (
          select
            *
          from durations_p0
          where event_num = 1
          ),

          durations_p2 as
          (
          select
            a.*,
            b.duration
          from series_title_lookup as a
          left join durations_p1 as b
          on a.video_id = b.video_id
          )

        select distinct * from durations_p2
        ),

        completion_rate_analysis as
        (
          with

          completion_p0 as
          (
          select
            a.*,
            b.duration
          from user_first_play as a
          right join title_durations as b
          on a.video_id = b.video_id
          ),

          completion_p1 as
          (
          select
            series,
            collection,
            video_id,
            min_count,
            duration,
            round(min_count / duration, 2) as completion_rate
          from completion_p0
          ),

          completion_p2 as
          (
          select
            series,
            round(avg(completion_rate), 4) as avg_completion_rate
          from completion_p1
          group by series
          )

        select * from completion_p2
        ),

        life_time_value as
        (
          with

          vimeo_purchase_event_p0 as
          (
          select
            user_id,
            topic,
            email,
            moptin,
            subscription_status,
            platform,
            row_number() over (partition by user_id order by timestamp asc) as event_num,
            date(timestamp) as date_stamp,
            subscription_frequency
          from http_api.purchase_event
          where user_id <> '0'
          and regexp_contains(user_id, r'^[0-9]*$')
          order by
            user_id,
            date(timestamp)
          ),

          vimeo_purchase_event_q0 as
          (
          select
            user_id,
            topic,
            email,
            moptin,
            subscription_status,
            platform,
            row_number() over (partition by user_id order by timestamp desc) as event_num,
            date(timestamp) as date_stamp, subscription_frequency
          from http_api.purchase_event
          where user_id <> '0'
          and regexp_contains(user_id, r'^[0-9]*$')
          order by
            user_id,
            date(timestamp)
          ),

          distinct_purchase_event as
          (
          select
            distinct user_id,
            topic,
            extract(month from date_stamp) as month,
            extract(year from date_stamp) as year
          from vimeo_purchase_event_p0
          ),

          audience_first_event as
          (
          select
            user_id,
            min(date_stamp) as first_event_date
          from vimeo_purchase_event_p0
          group by user_id
          ),

          audience_last_event as
          (
          select
            user_id,
            topic,
            date_stamp as last_event
          from vimeo_purchase_event_q0
          where event_num = 1
          ),

          audience_current_status as
          (
          select
            user_id,
            topic,
            case when topic in (
              'customer.product.disabled',
              'customer.product.cancelled',
              'customer.product.expired',
              'customer.deleted')
            or last_event < (current_date() - 31) then 'inactive'
            else 'active' end as status,
            last_event as last_event_date
          from audience_last_event
          ),

          first_play_p0 as
          (
            select
              *
            from user_first_play
            where date(timestamp) between '2021-11-01' and current_date()
          ),

          first_play_p1 as
          (
          select
            user_id,
            min_count,
            date(timestamp) as date_stamp,
            series
          from first_play_p0
          where play_number = 1
          ),

          audience_titles_flag as
          (
          select
            distinct user_id,
            series
          from first_play_p1
          ),

          customer_lvl_analysis_p0 as
          (
          select
            a.user_id,
            a.topic as current_topic,
            b.first_event_date,
            d.series,
            c.topic as last_topic,
            c.last_event_date,
            c.status,
          date_diff(c.last_event_date, b.first_event_date, day) as tenure
          from distinct_purchase_event as a
          join audience_first_event as b on a.user_id = b.user_id
          join audience_current_status as c on a.user_id = c.user_id
          join audience_titles_flag as d on a.user_id = d.user_id
          ),

          customer_lvl_analysis_p1 as
          (
          select
            distinct user_id,
            first_event_date,
            last_event_date,
            status,
            series,
            tenure
          from customer_lvl_analysis_p0
          ),

          audience_number_months as
          (
          select
            user_id,
            sum(case when topic in ('customer.product.renewed', 'customer.product.free_trial_converted') then 1 else 0 end) as number_months_paid_tenure
          from distinct_purchase_event
          group by user_id
          ),

          customer_lvl_analysis_p2 as
          (
          select
            a.user_id,
            a.first_event_date,
            a.last_event_date,
            a.status,
            a.series,
            a.tenure,
            b.number_months_paid_tenure
          from customer_lvl_analysis_p1 as a
          left join audience_number_months as b
          on a.user_id = b.user_id
          ),

          life_time_value_p1 as
          (
          select
            distinct user_id,
            number_months_paid_tenure,
            series,
            round((number_months_paid_tenure) * 5.99, 2) as ltv
          from customer_lvl_analysis_p2
          ),

          life_time_value_p2 as
          (
          select
            series,
            round(sum(ltv), 0) as total_ltv,
          from life_time_value_p1
          group by series
          )

        select * from life_time_value_p2
        )

        select
          a.*,
          b.number_first_plays,
          c.total_ltv,
          round(100 * a.avg_completion_rate + 0.01 * b.number_first_plays + 0.01 * log(c.total_ltv), 0) as value_score
        from completion_rate_analysis as a
        left join series_first_plays as b
        on a.series = b.series
        inner join life_time_value as c
        on a.series = c.series
        ;;
    }

    filter: date_filter {
      label: "Date Range"
      type: date
    }

    filter: end_date {
      label: "End Date"
      type: date
    }

  parameter: c_name1 {
    label: "Series 1"
    type: string

    default_value: "Hudson & Rex"

    allowed_value: {
      label: "Hudson & Rex"
      value: "Hudson & Rex"
    }

    allowed_value: {
      label: "Touched by an Angel"
      value: "Touched by an Angel"
    }

    allowed_value: {
      label: "Sue Thomas: F.B.Eye"
      value: "Sue Thomas: F.B.Eye"
    }

    allowed_value: {
      label: "Wildfire"
      value: "Wildfire"
    }

    allowed_value: {
      label: "Heartland"
      value: "Heartland"
    }

    allowed_value: {
      label: "Mystic"
      value: "Mystic"
    }
  }

  parameter: c_name2 {
    label: "Series 2"
    type: string

    default_value: "Touched by an Angel"

    allowed_value: {
      label: "Hudson & Rex"
      value: "Hudson & Rex"
    }

    allowed_value: {
      label: "Touched by an Angel"
      value: "Touched by an Angel"
    }

    allowed_value: {
      label: "Sue Thomas: F.B.Eye"
      value: "Sue Thomas: F.B.Eye"
    }

    allowed_value: {
      label: "Wildfire"
      value: "Wildfire"
    }

    allowed_value: {
      label: "Heartland"
      value: "Heartland"
    }

    allowed_value: {
      label: "Mystic"
      value: "Mystic"
    }
  }

  parameter: c_name3 {
    label: "Series 3"
    type: string

    default_value: "Sue Thomas: F.B.Eye"

    allowed_value: {
      label: "Hudson & Rex"
      value: "Hudson & Rex"
    }

    allowed_value: {
      label: "Touched by an Angel"
      value: "Touched by an Angel"
    }

    allowed_value: {
      label: "Sue Thomas: F.B.Eye"
      value: "Sue Thomas: F.B.Eye"
    }

    allowed_value: {
      label: "Wildfire"
      value: "Wildfire"
    }

    allowed_value: {
      label: "Heartland"
      value: "Heartland"
    }

    allowed_value: {
      label: "Mystic"
      value: "Mystic"
    }
  }

  parameter: c_name4 {
    label: "Series 4"
    type: string

    default_value: "Wildfire"

    allowed_value: {
      label: "Hudson & Rex"
      value: "Hudson & Rex"
    }

    allowed_value: {
      label: "Touched by an Angel"
      value: "Touched by an Angel"
    }

    allowed_value: {
      label: "Sue Thomas: F.B.Eye"
      value: "Sue Thomas: F.B.Eye"
    }

    allowed_value: {
      label: "Wildfire"
      value: "Wildfire"
    }

    allowed_value: {
      label: "Heartland"
      value: "Heartland"
    }

    allowed_value: {
      label: "Mystic"
      value: "Mystic"
    }
  }

  parameter: c_name5 {
    label: "Series 5"
    type: string

    default_value: "Heartland"

    allowed_value: {
      label: "Hudson & Rex"
      value: "Hudson & Rex"
    }

    allowed_value: {
      label: "Touched by an Angel"
      value: "Touched by an Angel"
    }

    allowed_value: {
      label: "Sue Thomas: F.B.Eye"
      value: "Sue Thomas: F.B.Eye"
    }

    allowed_value: {
      label: "Wildfire"
      value: "Wildfire"
    }

    allowed_value: {
      label: "Heartland"
      value: "Heartland"
    }

    allowed_value: {
      label: "Mystic"
      value: "Mystic"
    }
  }

  parameter: c_name6 {
    label: "Series 6"
    type: string

    default_value: "Mystic"

    allowed_value: {
      label: "Hudson & Rex"
      value: "Hudson & Rex"
    }

    allowed_value: {
      label: "Touched by an Angel"
      value: "Touched by an Angel"
    }

    allowed_value: {
      label: "Sue Thomas: F.B.Eye"
      value: "Sue Thomas: F.B.Eye"
    }

    allowed_value: {
      label: "Wildfire"
      value: "Wildfire"
    }

    allowed_value: {
      label: "Heartland"
      value: "Heartland"
    }

    allowed_value: {
      label: "Mystic"
      value: "Mystic"
    }
  }

    measure: count {
      type: count
      drill_fields: [detail*]
    }

    dimension: series {
      type: string
      sql: ${TABLE}.series ;;
    }

    measure: avg_completion_rate {
      type: sum
      sql: ${TABLE}.avg_completion_rate ;;
    }

    measure: number_first_plays {
      type: sum
      sql: ${TABLE}.number_first_plays ;;
    }

    measure: total_ltv {
      type: sum
      sql: ${TABLE}.total_ltv ;;
    }

    measure: value_score {
      type: sum
      sql: ${TABLE}.value_score ;;
    }

    set: detail {
      fields: [series, avg_completion_rate, number_first_plays, total_ltv, value_score]
    }
  }
