view: bigquery_timeupdate {
  derived_table: {
    sql:

    select * from bqtimeupdate.p0


;;

  }

  dimension: video_id {
    type: number
    sql: ${TABLE}.video_id ;;
  }

  dimension: hour {
    type: number
    sql: ${TABLE}.hour ;;
  }

  dimension: release {
    type: date
    sql: ${TABLE}.release ;;
  }

  dimension: email {
    type: string
    tags: ["email"]
    sql: ${TABLE}.email ;;
  }

  dimension: Quarter {
    type: string
    sql: ${TABLE}.quarter ;;
  }

  dimension: current_date {
    type: date
    sql: now() ;;
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
    type: number
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


  dimension: timecode {
    type: number
    sql: ${TABLE}.timecode  ;;
  }

  dimension: duration {
    type: number
    sql: ${TABLE}.duration ;;
  }

  dimension: hours_watched {
    type: number
    sql: ${timecode}/3600 ;;
    value_format: "#,##0"
  }

  dimension: minutes_watched {
    type: number
    sql: case when ${duration}< ${timecode} then round(${duration}/60) else round(${timecode}/60) end ;;
    value_format: "#,##0"
  }

  measure: title_count {
    type: count_distinct
    sql: ${video_id} ;;
  }

  dimension: user_id {
    primary_key: yes
    tags: ["user_id"]
    type: string
    sql: ${TABLE}.user_id ;;
  }

  dimension: kids_genre {
    type: string
    sql: case when ${collection} LIKE '%Owlegories%' OR ${collection} LIKE '%Abe & Bruno%' OR ${collection} LIKE '%Seventeen Again%' OR ${collection} LIKE '%Albert: Up, Up and Away!%' OR ${collection} LIKE '%Spirit Bear%' OR ${collection} LIKE '%The Great Mom Swap%' OR ${collection} LIKE '%Gadgetman%' OR ${collection} LIKE '%Zoo Juniors%' OR ${collection} LIKE '%Angels in Training%' OR ${collection} LIKE '%Treasure State%' OR ${collection} LIKE '%The Big Comfy Couch%' OR ${collection} LIKE '%Undercover Kids%' OR ${collection} LIKE '%On the Wings of the Monarch%' OR ${collection} LIKE '%Learning To See: The World of Insects%' OR ${collection} LIKE '%Trailer Made%' OR ${collection} LIKE '%Creeping Things%' OR ${collection} LIKE '%Crimes and Mister Meanors%' OR ${collection} LIKE '%Out of the Wilderness%' OR ${collection} LIKE '%Lost Wilderness%' OR ${collection} LIKE '%The Fix It Boys%' OR ${collection} LIKE '%Gibby%' OR ${collection} LIKE '%Flood Geology Series%' OR ${collection} LIKE '%The Prince and the Pauper%' OR ${collection} LIKE '%Our House: The Puzzle Maker%' OR ${collection} LIKE '%The Torchlighters%' OR ${collection} LIKE '%Meow Manor%' OR ${collection} LIKE '%The Lion of Judah%' OR ${collection} LIKE '%Testament: The Bible in Animation%' OR ${collection} LIKE '%The Passion: A Brickfilm%' OR ${collection} LIKE '%Horse Crazy%' OR ${collection} LIKE '%Kid Cop%' OR ${collection} LIKE '%The Sandman and the Lost Sand of Dreams%' OR ${collection} LIKE '%The Saddle Club%' OR ${collection} LIKE '%Junior\'s Giants%' OR ${collection} LIKE '%Genesis 7%' OR ${collection} LIKE '%The Lost Medallion: The Adventures of Billy Stone%' OR ${collection} LIKE '%Davey & Goliath%' OR ${collection} LIKE '%Legend of The Lost Tomb%' OR ${collection} LIKE '%My Dad\'s a Soccer Mom%' OR ${collection} LIKE '%Little Heroes%' OR ${collection} LIKE '%Ms. Bear%' OR ${collection} LIKE '%Whiskers%' OR ${collection} LIKE '%The Country Mouse and the City Mouse Adventures%' OR ${collection} LIKE '%Monkey Business%' OR ${collection} LIKE '%The Sugar Creek Gang%' OR ${collection} LIKE '%Awesome Science%' OR ${collection} LIKE '%The Ghost Club%' OR ${collection} LIKE '%Animals are People Too!%' OR ${collection} LIKE '%Who\'s Watching the Kids%' OR ${collection} LIKE '%Touched by Grace%' OR ${collection} LIKE '%Mandie And The Cherokee Treasure%' OR ${collection} LIKE '%Mandie and the Secret Tunnel%' OR ${collection} LIKE '%Finding Buck McHenry%' OR ${collection} LIKE '%The Princess Stallion%' OR ${collection} LIKE '%Adventures of Chris Fable%' OR ${collection} LIKE '%The Wild Stallion%' OR ${collection} LIKE '%A Horse Called Bear%' OR ${collection} LIKE '%The Flizbins: Cowboys & Bananas%' OR ${collection} LIKE '%Jacob on the Road%' OR ${collection} LIKE '%Cedarmont Kids%' OR ${collection} LIKE '%Kingdom Under the Sea%' OR ${collection} LIKE '%Uncle Nino%'
         then 'Kids' else 'Non-Kids' end;;
        }


  dimension_group: timestamp {
    type: time
    timeframes: [
      raw,
      time,
      hour_of_day,
      date,
      day_of_week_index,
      day_of_week,
      week,
      month,
      quarter,
      year
    ]
    sql: ${TABLE}.timestamp ;;
  }


  measure: count {
    type: count
    drill_fields: [detail*]
  }


  measure: duration_count {
    type: sum
    sql: ${duration} ;;
  }

  measure: percent_completed {
    type: number
    value_format: "0\%"
    sql: case when ${timecode_count}>${duration_count} then 100.00 else 100.00*${timecode_count}/${duration_count} end ;;
  }

  measure: play_count {
    type: count_distinct
    sql: concat(safe_cast(${video_id} as string),${user_id},cast(${timestamp_date} as string)) ;;
  }

  measure: timecode_count {
    type: sum
    value_format: "0"
    sql: ${timecode} ;;
  }

  measure: hours_count {
    type: sum
    value_format: "#,##0"
    sql: ${hours_watched};;
  }

  measure: minutes_count {
    type: sum
    value_format: "#,##0"
    sql: ${minutes_watched};;
  }

  measure: user_count {
    type: count_distinct
    sql: ${user_id} ;;
  }

  measure: hours_watched_per_user {
    type: number
    sql: 1.0*${hours_count}/${user_count} ;;
    value_format: "0.0"
  }

  measure: minutes_watched_per_user {
    type: number
    sql: 1.00*${minutes_count}/${user_count} ;;
    value_format: "0"
  }

## filter determining time range for all "A" measures
  filter: time_a {
    type: date_time
  }

## flag for "A" measures to only include appropriate time range
  dimension: group_a {
    hidden: no
    type: yesno
    sql: {% condition time_a %} ${timestamp_raw} {% endcondition %}
      ;;
  }

  measure: hours_a {
    type: sum
    filters: {
      field: group_a
      value: "yes"
    }
    sql: ${hours_watched} ;;
    value_format: "#,##0"
  }

## filter determining time range for all "b" measures
  filter: time_b {
    type: date_time
  }

## flag for "B" measures to only include appropriate time range
  dimension: group_b {
    hidden: no
    type: yesno
    sql: {% condition time_b %} ${timestamp_raw} {% endcondition %}
      ;;
  }

  filter: minutes_a {
    type: number
  }

  dimension: sample_a {
    hidden: no
    type: number
    sql: {% condition minutes_a %} ${minutes_watched} {% endcondition %};;
  }

  measure: completion_rate_a {
    type: sum
    filters: {
      field: minutes_a
      value: ">5"
    }
    sql: 100.00*${timecode}/${duration} ;;
  }

  measure: hours_b {
    type: sum
    filters: {
      field: group_b
      value: "yes"
    }
    sql: ${hours_watched} ;;
    value_format: "#,##0"
  }

  measure: user_count_a {
    type: count_distinct
    filters: {
      field: group_a
      value: "yes"
    }
    sql: ${user_id}  ;;
    value_format: "#,##0"
  }

  measure: user_count_b {
    type: count_distinct
    filters: {
      field: group_b
      value: "yes"
    }
    sql: ${user_id} ;;
    value_format: "#,##0"
  }


# ----- Sets of fields for drilling ------
  set: detail {
    fields: [
      platform,
      user_id
    ]
  }
}
