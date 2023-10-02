view: bigquery_allfirstplay {
  derived_table: {
    sql:
/*Begin by building out queries to segment new customer views and winback views*/
    with aa as
(select user_id,email,status_date as churn_date
from http_api.purchase_event
where topic in ('customer.product.cancelled','customer.product.disabled','customer.product.expired')),

bb as
(select user_id, email, max(status_date) as status_date
from http_api.purchase_event
where topic in ('customer.product.created','customer.product.renewed','customer.created','customer.product.free_trial_created')
group by 1,2),

/*Create table with customers who have a status data after churning*/
cc as
(select distinct bb.user_id, bb.email
from aa inner join bb on aa.user_id=bb.user_id and status_date>churn_date),

/*For older dates, we leverage firstplay tables.*/
a1 as
(select sent_at as timestamp,
        user_id,
        (split(title," - ")) as title
from javascript.firstplay),

a2 as
(select timestamp,
        user_id,
        title[safe_ordinal(1)] as title,
        concat(title[safe_ordinal(2)]," - ",title[safe_ordinal(3)]) as collection
 from a1 order by 1),

/*Use php.get_titles table to create title id mapping table that maps video id to title of any given asset*/
a30 as
(select video_id,
       max(loaded_at) as loaded_at
from php.get_titles
group by 1),

titles_id_mapping as
(select distinct
       metadata_series_name  as series,
       case when metadata_season_name in ('Season 1','Season 2','Season 3') then concat(metadata_series_name,'-',metadata_season_name)
            when metadata_season_name is null then metadata_movie_name
            else metadata_season_name end as collection,
       metadata_season_number as season,
       a.title,
       a.video_id as id,
       episode_number as episode,
      coalesce(date(time_available), cast(json_value(release_dates, '$[0].date') as date)) as date,
       --date(time_available) as date,
       date(time_unavailable) as end_date,
       round(duration_seconds/60) as duration,
       promotion
from php.get_titles as a left join svod_titles.titles_id_mapping as b on a.video_id=b.id inner join a30 on a30.video_id=a.video_id and a30.loaded_at=a.loaded_at),
/*call legacy roku firstplay table for old dates*/
a32 as
(select distinct mysql_roku_firstplays_firstplay_date_date as timestamp,
                mysql_roku_firstplays_video_id,
                user_id,
                '' as anonymousId,
                'firstplay' as event_type,
                UNIX_SECONDS(mysql_roku_firstplays_firstplay_date_date) as EPOCH_TIMESTAMP,
                CAST('1111' AS int64) as platform_id
from looker.roku_firstplays),
/*build master dataset for engagement using firstplay tables for older dates and the current video_content_playing tables for
current engagement ingestion source*/
a as
        (select sent_at as timestamp,
                b.date as release_date,
                end_date,
                case when collection in ('Season 1','Season 2','Season 3') then concat(series,' ',collection) else collection end as collection,
                case when (series is null and upper(b.title) like upper(b.collection)) then 'movie'
                     when series is not null then 'series' else 'other' end as type,
                safe_cast(a.video_id as int64) as video_id,
                series,
                trim(b.title) as title,
                user_id,
                anonymous_id,
                event as event_type,
                'Android' as source,
                UNIX_SECONDS(sent_at) as EPOCH_TIMESTAMP,
                CAST(platform_id AS int64) as platform_id,
                episode,
                cast(is_chromecast as int64) as tv_cast,
                promotion
         from android.firstplay as a left join titles_id_mapping as b on a.video_id = b.id

         union all

        select timestamp,
       b.date as release_date,
       end_date,
       case when b.collection in ('Season 1','Season 2','Season 3') then concat(b.series,' ',b.collection) else collection end as collection,
       case when (series is null and upper(b.title) like upper(b.collection)) then 'movie'
                     when b.series is not null then 'series' else 'other' end as type,
       mysql_roku_firstplays_video_id as video_id,
       series,
       trim(b.title) as title,
       user_id,
      'anonymous_id' as anonymous_id,
       'firstplay' as event_type,
      'Roku' as source,
      UNIX_SECONDS(timestamp) as EPOCH_TIMESTAMP,
      CAST('1111' AS int64) as platform_id,
       b.episode,
       null as tv_cast,
       promotion
from a32 as a left join titles_id_mapping as b on mysql_roku_firstplays_video_id=b.id


         union all

         select sent_at as timestamp,
                b.date as release_date,
                end_date,
                case when collection in ('Season 1','Season 2','Season 3') then concat(series,' ',collection) else collection end as collection,
                case when (series is null and upper(b.title) like upper(b.collection)) then 'movie'
                     when series is not null then 'series' else 'other' end as type,
                safe_cast(a.video_id as int64) as video_id,
                series,
                trim(b.title) as title,
                user_id,
                anonymous_id,
                event as event_type,
                'iOS' as source,
                UNIX_SECONDS(sent_at) as EPOCH_TIMESTAMP,
                CAST(platform_id AS int64) as platform_id,
                episode,
                cast(is_chromecast as int64)+cast(is_airplay as int64) as tv_cast,
                promotion
         from ios.firstplay as a left join titles_id_mapping as b on a.video_id = safe_cast(b.id as string)
         union all
         select sent_at as timestamp,
                b.date as release_date,
                end_date,
                case when collection in ('Season 1','Season 2','Season 3') then concat(series,' ',collection) else collection end as collection,
                case when (series is null and upper(b.title) like upper(b.collection)) then 'movie'
                     when series is not null then 'series' else 'other' end as type,
                safe_cast(a.video_id as int64) as video_id,
                series,
                trim(b.title) as title,
                user_id,
                null as anonymous_id,
                event as event_type,
                'Roku' as source,
                UNIX_SECONDS(sent_at) as EPOCH_TIMESTAMP,
                CAST(platform_id AS int64) as platform_id,
                episode,
                cast(is_chromecast as int64)+cast(is_airplay as int64) as tv_cast,
                promotion
         from roku.firstplay as a left join titles_id_mapping as b on a.video_id = b.id
         union all
         select sent_at as timestamp,
                b.date as release_date,
                end_date,
                case when collection in ('Season 1','Season 2','Season 3') then concat(series,' ',collection) else collection end as collection,
                case when (series is null and upper(b.title) like upper(b.collection)) then 'movie'
                     when series is not null then 'series' else 'other' end as type,
                safe_cast(a.video_id as int64) as video_id,
                series,
                trim(b.title) as title,
                user_id,
                anonymous_id,
                'firstplay' as event_type,
                'Web' as source,
                UNIX_SECONDS(sent_at) as EPOCH_TIMESTAMP,
                CAST(platform_id AS int64) as platform_id,
                episode,
                null as tv_cast,
                promotion
         from javascript.loadedmetadata as a left join titles_id_mapping as b on safe_cast(a.video_id as string)= safe_cast(b.id as string)
        union all
        select sent_at as timestamp,
                b.date as release_date,
                end_date,
                case when collection in ('Season 1','Season 2','Season 3') then concat(series,' ',collection) else collection end as collection,
                case when (series is null and upper(b.title) like upper(b.collection)) then 'movie'
                     when series is not null then 'series' else 'other' end as type,
                safe_cast(a.video_id as int64) as video_id,
                series,
                trim(b.title) as title,
                user_id,
                anonymous_id,
                'firstplay' as event_type,
                'Web' as source,
                UNIX_SECONDS(sent_at) as EPOCH_TIMESTAMP,
                CAST(platform_id AS int64) as platform_id,
                episode,
                null as tv_cast,
                promotion
         from javascript.video_content_playing as a left join titles_id_mapping as b on safe_cast(a.video_id as string)= safe_cast(b.id as string)

        union all

select sent_at as timestamp,
                b.date as release_date,
                end_date,
                case when collection in ('Season 1','Season 2','Season 3') then concat(series,' ',collection) else collection end as collection,
                case when (series is null and upper(b.title) like upper(b.collection)) then 'movie'
                     when series is not null then 'series' else 'other' end as type,
                safe_cast(a.video_id as int64) as video_id,
                series,
                trim(b.title) as title,
                user_id,
                anonymous_id,
                'firstplay' as event_type,
                'iOS' as source,
                UNIX_SECONDS(sent_at) as EPOCH_TIMESTAMP,
                CAST(platform_id AS int64) as platform_id,
                episode,
                null as tv_cast,
                promotion
         from ios.video_content_playing as a left join titles_id_mapping as b on safe_cast(a.video_id as string)= safe_cast(b.id as string)

        union all

        select sent_at as timestamp,
                b.date as release_date,
                end_date,
                case when collection in ('Season 1','Season 2','Season 3') then concat(series,' ',collection) else collection end as collection,
                case when (series is null and upper(b.title) like upper(b.collection)) then 'movie'
                     when series is not null then 'series' else 'other' end as type,
                safe_cast(a.video_id as int64) as video_id,
                series,
                trim(b.title) as title,
                user_id,
                anonymous_id,
                'firstplay' as event_type,
                'Android' as source,
                UNIX_SECONDS(sent_at) as EPOCH_TIMESTAMP,
                CAST(platform_id AS int64) as platform_id,
                episode,
                null as tv_cast,
                promotion
         from android.video_content_playing as a left join titles_id_mapping as b on safe_cast(a.video_id as string)= safe_cast(b.id as string)

        union all

        select sent_at as timestamp,
                b.date as release_date,
                end_date,
                case when collection in ('Season 1','Season 2','Season 3') then concat(series,' ',collection) else collection end as collection,
                case when (series is null and upper(b.title) like upper(b.collection)) then 'movie'
                     when series is not null then 'series' else 'other' end as type,
                safe_cast(a.video_id as int64) as video_id,
                series,
                trim(b.title) as title,
                user_id,
                anonymous_id,
                'firstplay' as event_type,
                'FireTV' as source,
                UNIX_SECONDS(sent_at) as EPOCH_TIMESTAMP,
                CAST(platform_id AS int64) as platform_id,
                episode,
                null as tv_cast,
                promotion
         from amazon_fire_tv.video_content_playing as a left join titles_id_mapping as b on safe_cast(a.video_id as string)= safe_cast(b.id as string)

        union all

        select sent_at as timestamp,
                b.date as release_date,
                end_date,
                case when collection in ('Season 1','Season 2','Season 3') then concat(series,' ',collection) else collection end as collection,
                case when (series is null and upper(b.title) like upper(b.collection)) then 'movie'
                     when series is not null then 'series' else 'other' end as type,
                safe_cast(a.video_id as int64) as video_id,
                series,
                trim(b.title) as title,
                user_id,
                a.id as anonymous_id,
                'firstplay' as event_type,
                'Roku' as source,
                UNIX_SECONDS(sent_at) as EPOCH_TIMESTAMP,
                CAST(platform_id AS int64) as platform_id,
                episode,
                null as tv_cast,
                promotion
         from roku.video_content_playing as a left join titles_id_mapping as b on safe_cast(a.video_id as string)= safe_cast(b.id as string)

         union all

        select sent_at as timestamp,
                b.date as release_date,
                end_date,
                case when collection in ('Season 1','Season 2','Season 3') then concat(series,' ',collection) else collection end as collection,
                case when (series is null and upper(b.title) like upper(b.collection)) then 'movie'
                     when series is not null then 'series' else 'other' end as type,
                safe_cast(a.video_id as int64) as video_id,
                series,
                trim(b.title) as title,
                user_id,
                null as anonymous_id,
                'firstplay' as event_type,
                'Tizen' as source,
                UNIX_SECONDS(sent_at) as EPOCH_TIMESTAMP,
                null as platform_id,
                episode,
                null as tv_cast,
                promotion
         from php.get_tizen_views as a left join titles_id_mapping as b on safe_cast(a.video_id as string)= safe_cast(b.id as string)

         union all

         select sent_at as timestamp,
                b.date as release_date,
                end_date,
                case when collection in ('Season 1','Season 2','Season 3') then concat(series,' ',collection) else collection end as collection,
                case when (series is null and upper(b.title) like upper(b.collection)) then 'movie'
                     when series is not null then 'series' else 'other' end as type,
                safe_cast(a.video_id as int64) as video_id,
                series,
                trim(b.title) as title,
                user_id,
                null as anonymous_id,
                'firstplay' as event_type,
                'Xbox' as source,
                UNIX_SECONDS(sent_at) as EPOCH_TIMESTAMP,
                null as platform_id,
                episode,
                null as tv_cast,
                promotion
         from php.get_xbox_views as a left join titles_id_mapping as b on safe_cast(a.video_id as string)= safe_cast(b.id as string)

        union all

        select timestamp,
                b.date as release_date,
                end_date,
                case when b.collection in ('Season 1','Season 2','Season 3') then concat(series,' ',b.collection) else b.collection end as collection,
                case when (series is null and upper(b.title) like upper(b.collection))  then 'movie'
                     when series is not null then 'series' else 'other' end as type,
                safe_cast(b.id as int64) as video_id,
                series,
                trim(b.title) as title,
                user_id,
                '' as anonymous_id,
                'firstplay' as event_type,
                'Web' as source,
                UNIX_SECONDS(timestamp) as EPOCH_TIMESTAMP,
                CAST('33064' AS int64) as platform_id,
                episode,
                null as tv_cast,
                promotion
         from a2 as a left join titles_id_mapping as b on trim(upper(b.title)) = trim(upper(a.title)))
/*join master dataset with winback and first time customers table to finish query*/
select a.user_id,
       a.anonymous_id,
       a.event_type,
       timestamp_sub(a.timestamp,interval 4 hour) as timestamp,
       a.EPOCH_TIMESTAMP,
       a.platform_id,
       a.release_date,
       a.end_date,
       date_diff(date(timestamp_sub(a.timestamp,interval 4 hour)),a.release_date,day) as days_since_release,
       a.collection,
       a.type,
       a.video_id,
       series,
       a.title,
       a.source,
       a.episode,
      email,
      tv_cast,
      c.promotion,
       case when cc.user_id is null then 'first-time customers' else 'reacquisitions' end as winback,
       case when date(a.timestamp) between DATE_SUB(date(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), QUARTER)), INTERVAL 0 QUARTER) and
            DATE_SUB(date(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY)), INTERVAL 0 QUARTER) then "Current Quarter"
            when date(a.timestamp) between DATE_SUB(date(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), QUARTER)), INTERVAL 1 QUARTER) and
            DATE_SUB(date(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY)), INTERVAL 1 QUARTER) then "Prior Quarter"
            when date(a.timestamp) between DATE_SUB(date(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), QUARTER)), INTERVAL 4 QUARTER) and
            DATE_SUB(date(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY)), INTERVAL 4 QUARTER) then "YAGO Quarter"
            else "NA"
            end as Quarter
from a left join cc on a.user_id=cc.user_id left join svod_titles.promos as c on a.video_id=c.video_id
/* where a.user_id<>'0' */

;;

    datagroup_trigger: upff_analytics_datagroup
  }

dimension: winback {
  type: string
  sql: ${TABLE}.winback ;;
}

dimension: days_since_release {
  type: number
  sql: ${TABLE}.days_since_release ;;
}

dimension: tv_cast {
  type: number
  sql: ${TABLE}.tv_cast ;;
}

  dimension: quarter {
    type: string
    sql: ${TABLE}.quarter ;;
  }

  dimension: current_date {
    type: date
    sql: current_date() ;;
  }

  dimension_group: current_date_ {
    type: time
    timeframes: [
      raw,
      time,
      hour_of_day,
      date,
      day_of_week_index,
      week,
      month,
      quarter,
      year
    ]
    sql: current_date() ;;
    }

  dimension: title {
    type: string
    sql: ${TABLE}.title;;
  }

  measure: title_count {
    type: count_distinct
    sql: ${video_id} ;;
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

  measure: episode_count {
    type: max
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

  dimension: platform_id {
    type: number
    sql: ${TABLE}.platform_id ;;
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

  dimension: anonymousId {
    type: string
    #tags: ["segment_anonymous_id"]
    sql: ${TABLE}.anonymous_id ;;
  }

  dimension: event {
    type: string
    sql: ${TABLE}.event_type ;;
  }

  dimension: epoch_timestamp {
    type: number
    value_format: "0"
    sql: ${TABLE}.epoch_timestamp ;;
  }

  dimension: video_id {
    type: number
    sql: ${TABLE}.video_id ;;
  }

  dimension: itemId {
    type: string
    sql: SAFE_CAST(${video_id} AS STRING) ;;
  }

dimension: promotion_date {
  type: date
  sql: ${TABLE}.promotion ;;
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



  dimension: release_date {
    type: date
    datatype: date
    sql: ${TABLE}.release_date ;;
  }

  dimension: kids_genre {
    type: string
    sql: case when ${collection} LIKE '%Owlegories%' OR ${collection} LIKE '%Abe & Bruno%' OR ${collection} LIKE '%Seventeen Again%' OR ${collection} LIKE '%Albert: Up, Up and Away!%' OR ${collection} LIKE '%Spirit Bear%' OR ${collection} LIKE '%The Great Mom Swap%' OR ${collection} LIKE '%Gadgetman%' OR ${collection} LIKE '%Zoo Juniors%' OR ${collection} LIKE '%Angels in Training%' OR ${collection} LIKE '%Treasure State%' OR ${collection} LIKE '%The Big Comfy Couch%' OR ${collection} LIKE '%Undercover Kids%' OR ${collection} LIKE '%On the Wings of the Monarch%' OR ${collection} LIKE '%Learning To See: The World of Insects%' OR ${collection} LIKE '%Trailer Made%' OR ${collection} LIKE '%Creeping Things%' OR ${collection} LIKE '%Crimes and Mister Meanors%' OR ${collection} LIKE '%Out of the Wilderness%' OR ${collection} LIKE '%Lost Wilderness%' OR ${collection} LIKE '%The Fix It Boys%' OR ${collection} LIKE '%Gibby%' OR ${collection} LIKE '%Flood Geology Series%' OR ${collection} LIKE '%The Prince and the Pauper%' OR ${collection} LIKE '%Our House: The Puzzle Maker%' OR ${collection} LIKE '%The Torchlighters%' OR ${collection} LIKE '%Meow Manor%' OR ${collection} LIKE '%The Lion of Judah%' OR ${collection} LIKE '%Testament: The Bible in Animation%' OR ${collection} LIKE '%The Passion: A Brickfilm%' OR ${collection} LIKE '%Horse Crazy%' OR ${collection} LIKE '%Kid Cop%' OR ${collection} LIKE '%The Sandman and the Lost Sand of Dreams%' OR ${collection} LIKE '%The Saddle Club%' OR ${collection} LIKE '%Junior\'s Giants%' OR ${collection} LIKE '%Genesis 7%' OR ${collection} LIKE '%The Lost Medallion: The Adventures of Billy Stone%' OR ${collection} LIKE '%Davey & Goliath%' OR ${collection} LIKE '%Legend of The Lost Tomb%' OR ${collection} LIKE '%My Dad\'s a Soccer Mom%' OR ${collection} LIKE '%Little Heroes%' OR ${collection} LIKE '%Ms. Bear%' OR ${collection} LIKE '%Whiskers%' OR ${collection} LIKE '%The Country Mouse and the City Mouse Adventures%' OR ${collection} LIKE '%Monkey Business%' OR ${collection} LIKE '%The Sugar Creek Gang%' OR ${collection} LIKE '%Awesome Science%' OR ${collection} LIKE '%The Ghost Club%' OR ${collection} LIKE '%Animals are People Too!%' OR ${collection} LIKE '%Who\'s Watching the Kids%' OR ${collection} LIKE '%Touched by Grace%' OR ${collection} LIKE '%Mandie And The Cherokee Treasure%' OR ${collection} LIKE '%Mandie and the Secret Tunnel%' OR ${collection} LIKE '%Finding Buck McHenry%' OR ${collection} LIKE '%The Princess Stallion%' OR ${collection} LIKE '%Adventures of Chris Fable%' OR ${collection} LIKE '%The Wild Stallion%' OR ${collection} LIKE '%A Horse Called Bear%' OR ${collection} LIKE '%The Flizbins: Cowboys & Bananas%' OR ${collection} LIKE '%Jacob on the Road%' OR ${collection} LIKE '%Cedarmont Kids%' OR ${collection} LIKE '%Kingdom Under the Sea%' OR ${collection} LIKE '%Uncle Nino%'
      then 'Kids' else 'Non-Kids' end;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  measure: play_count {
    type: count_distinct
    sql: case when ${user_id} is null or ${user_id}='0' or length(${user_id})=0 then concat(safe_cast(${video_id} as string),${anonymousId},cast(${timestamp_date} as string)) else concat(safe_cast(${video_id} as string),${user_id},cast(${timestamp_date} as string)) end ;;
  }

  measure: number_of_platforms_by_user {
    type: count_distinct
    sql: ${source};;
  }

  measure: user_count {
    type: count_distinct
    sql: ${user_id} ;;
  }

  measure: views_per_user {
    type: number
    sql: 1.0*${play_count}/${user_count} ;;
    value_format: "0.0"
  }

  measure: id_count {
    type: count_distinct
    sql: ${TABLE}.anonymous_id ;;
  }

  measure: views_per_id {
    type: number
    sql: 1.0*${play_count}/${id_count} ;;
    value_format: "0.0"
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


filter: collections_a {
  type: string
}

dimension: collections_group_a {
  hidden: no
  type: yesno
  sql: {% condition collections_a %} ${collection} {% endcondition %};;
}

measure: user_count_collections_a {
  type: count_distinct
  filters: {
  field: collections_group_a
  value: "yes"
  }
  sql: ${user_id} ;;
}

filter: collections_b {
  type: string
}

dimension: collections_group_b {
  hidden: no
  type: yesno
  sql: {% condition collections_b %} ${collection} {% endcondition %};;
}

measure: user_count_collections_b {
  type: count_distinct
  filters: {
  field: collections_group_b
  value: "yes"
  }
  sql: ${user_id} ;;
}

  filter: collections_c {
    type: string
  }

  dimension: collections_group_c {
    hidden: no
    type: yesno
    sql: {% condition collections_c %} ${collection} {% endcondition %};;
  }

  measure: user_count_collections_c {
    type: count_distinct
    filters: {
      field: collections_group_c
      value: "yes"
    }
    sql: ${user_id} ;;
  }

filter: collection_type {
  type: string
}

dimension: collections_type_group {
  hidden: no
  type: yesno
  sql: {% condition collections_b %} ${type}{% endcondition %};;
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



## filter determining time range for all "B" measures
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

  filter: time_c {
    type: date_time
  }
## flag for "C" measures to only include appropriate time range
  dimension: group_c {
    hidden: no
    type: yesno
    sql: {% condition time_c %} ${timestamp_raw} {% endcondition %}
      ;;
  }

  filter: time_d {
    type: date_time
  }

## flag for "D" measures to only include appropriate time range
  dimension: group_d {
    hidden: no
    type: yesno
    sql: {% condition time_d %} ${timestamp_raw} {% endcondition %}
      ;;
  }

  filter: promotion_time {
    type: date
  }

## flag for "D" measures to only include appropriate time range
  dimension: promotion_group {
    hidden: no
    type: yesno
    sql: {% condition promotion_time %} ${promotion_date} {% endcondition %}
      ;;
  }

  measure: promotion_plays {
    type: count_distinct
    filters: {
      field: promotion_group
      value: "yes"
    }
    sql: concat(${title},${user_id},cast(${timestamp_date} as string)) ;;
  }

  measure: promotion_type_plays {
    type: count_distinct
    filters: {
      field: collections_type_group
      value: "yes"
    }
    sql: concat(${title},${user_id},cast(${timestamp_date} as string)) ;;
  }



  measure: plays_a {
    type: count_distinct
    filters: {
      field: group_a
      value: "yes"
    }
    sql: concat(${title},${user_id},cast(${timestamp_date} as string)) ;;
  }

  measure: user_count_a {
    type: count_distinct
    filters: {
      field: group_a
      value: "yes"
    }
    sql: ${user_id} ;;
  }

  measure: plays_b {
    type: count_distinct
    filters: {
      field: group_b
      value: "yes"
    }
    sql: concat(${title},${user_id},cast(${timestamp_date} as string)) ;;
  }

  measure: user_count_b {
    type: count_distinct
    filters: {
      field: group_b
      value: "yes"
    }
    sql: ${user_id} ;;
  }

  measure: plays_c {
    type: count_distinct
    filters: {
      field: group_c
      value: "yes"
    }
    sql: concat(${title},${user_id},cast(${timestamp_date} as string)) ;;
  }

  measure: plays_d {
    type: count_distinct
    filters: {
      field: group_d
      value: "yes"
    }
    sql: concat(${title},${user_id},cast(${timestamp_date} as string)) ;;
  }


  parameter: date_granularity {
    type: string
    allowed_value: { value: "Day" }
    allowed_value: { value: "Week"}
    allowed_value: { value: "Month" }
    allowed_value: { value: "Quarter" }
    allowed_value: { value: "Year" }
  }

  filter:  promotional_id{
    type: number
  }

  dimension: promotional {
    hidden: no
    type: yesno
    sql: {%condition promotional_id%} ${video_id} {%endcondition%};;
  }

  measure: promotional_plays {
    type: count_distinct
    filters: {
      field: promotional
      value: "yes"
    }
    sql: concat(safe_cast(${video_id} as string),${user_id},cast(${timestamp_date} as string))  ;;
  }

  measure: promotional_title_count {
    type: count_distinct
    filters: {
      field: promotional
      value: "yes"
    }
    sql: ${collection} ;;
  }


  filter:  promotional_collection_a{
    type: string
  }

  dimension: promotional_collection_a_{
    hidden: no
    type: yesno
    sql: {%condition promotional_collection_a%} ${collection} {%endcondition%};;
  }

  measure: promotional_collection_plays_a {
    type: count_distinct
    filters: {
      field: promotional_collection_a_
      value: "yes"
    }
    sql: concat(safe_cast(${video_id} as string),${user_id},cast(${timestamp_date} as string))  ;;
  }

  measure: promotional_collection_title_count_a {
    type: count_distinct
    filters: {
      field: promotional_collection_a_
      value: "yes"
    }
    sql: ${video_id} ;;
  }

  filter:  promotional_collection_b{
    type: string
  }

  dimension: promotional_collection_b_{
    hidden: no
    type: yesno
    sql: {%condition promotional_collection_b%} ${collection} {%endcondition%};;
  }

  measure: promotional_collection_plays_b {
    type: count_distinct
    filters: {
      field: promotional_collection_b_
      value: "yes"
    }
    sql: concat(safe_cast(${video_id} as string),${user_id},cast(${timestamp_date} as string))  ;;
  }

  filter:  promotional_collection_c{
    type: string
  }

  dimension: promotional_collection_c_{
    hidden: no
    type: yesno
    sql: {%condition promotional_collection_c%} ${collection} {%endcondition%};;
  }

  measure: promotional_collection_plays_c {
    type: count_distinct
    filters: {
      field: promotional_collection_c_
      value: "yes"
    }
    sql: concat(safe_cast(${video_id} as string),${user_id},cast(${timestamp_date} as string))  ;;
  }

  measure: promotional_collection_title_count_b {
    type: count_distinct
    filters: {
      field: promotional_collection_b_
      value: "yes"
    }
    sql: ${video_id} ;;
  }

  measure: promotional_collection_title_count_c {
    type: count_distinct
    filters: {
      field: promotional_collection_c_
      value: "yes"
    }
    sql: ${video_id} ;;
  }

  dimension: content_type {
    type: string
    sql: case when ${type}='series' then ${type}
              when ${series} like '%Heartland%' then 'Heartland'
              when ${series} like '%Bates%' then 'Bates'
              when (${collection} LIKE '%Letter Never Sent%' OR ${collection} LIKE '%It Had to Be You%' OR ${collection} LIKE '%Instant Nanny%' OR ${collection} LIKE '%Love Throws a Curve%' OR ${collection} LIKE '%Romantically Speaking%' OR ${collection} LIKE '%Wedding Do Over%' OR ${collection} LIKE '%When Duty Calls%' OR ${collection} LIKE '%The Right Girl%' OR ${collection} LIKE '%Brimming With Love%' OR ${collection} LIKE '%Sweet Surrender%' OR ${collection} LIKE '%Hopeless Romantic%' OR ${collection} LIKE '%Secret Summer%' OR ${collection} LIKE '%Late Bloomer%' OR ${collection} LIKE '%Win, Lose or Love%' OR ${collection} LIKE '%Mr. Write%' OR ${collection} LIKE '%Once Upon a Date%' OR ${collection} LIKE '%Twist of Fate%' OR ${collection} LIKE '%Vision of Love (FKA The Michaels)%' OR ${collection} LIKE '%Secret Millionaire%' OR ${collection} LIKE '%Tomboy%' OR ${collection} LIKE '%Advance & Retreat%' OR ${collection} LIKE '%Diagnosis Delicious%' OR ${collection} LIKE '%Undercover Angel%' OR ${collection} LIKE '%Love on the Vines%' OR ${collection} LIKE '%Woman of the House%' OR ${collection} LIKE '%A Moving Romance%' OR ${collection} LIKE '%Change of Heart%' OR ${collection} LIKE '%Touched by Love (FKA Touched)%' OR ${collection} LIKE '%Buy My Love%' OR ${collection} LIKE '%Groomzilla%' OR ${collection} LIKE '%How Not to Propose%' OR ${collection} LIKE '%Sisters of the Groom%' OR ${collection} LIKE '%Mechanics of Love%' OR ${collection} LIKE '%Accidentally in Love%' OR ${collection} LIKE '%Class%' OR ${collection} LIKE '%Keeping Up with the Randalls%' OR ${collection} LIKE '%Rock the House%' OR ${collection} LIKE '%Place Called Home, A%' OR ${collection} LIKE '%Out of the Woods%' OR ${collection} LIKE '%Thicker Than Water%' OR ${collection} LIKE '%Long Shot, The%' OR ${collection} LIKE '%After the Fall%' OR ${collection} LIKE '%Soldier Love Story%' OR ${collection} LIKE '%Mending Fences%' OR ${collection} LIKE '%A Time to Remember%' OR ${collection} LIKE '%Expecting a Miracle%' OR ${collection} LIKE '%Annie’s Point%' OR ${collection} LIKE '%Bound by a Secret%' OR ${collection} LIKE '%Ladies of the House%' OR ${collection} LIKE '%Where There’s a Will%' OR ${collection} LIKE '%Family Plan%' OR ${collection} LIKE '%Backyard Wedding%' OR ${collection} LIKE '%Honeymoon for One%' OR ${collection} LIKE '%Wedding Daze%' OR ${collection} LIKE '%The Nanny Express%' OR ${collection} LIKE '%Fielder’s Choice%' OR ${collection} LIKE '%Generation Gap%' OR ${collection} LIKE '%Love Will Keep Us Together%' OR ${collection} LIKE '%Relative Stranger%' OR ${collection} LIKE '%Working Miracles (aka Healing Hands)%' OR ${collection} LIKE '%Vickery’s Wild Ride%' OR ${collection} LIKE '%The Reading Room%' OR ${collection} LIKE '%Miles From Nowhere (aka Chasing A Dream)%' OR ${collection} LIKE '%Ordinary Miracles%' OR ${collection} LIKE '%Home Fires Burning%' OR ${collection} LIKE '%Vision of Love%' OR ${collection} LIKE '%The Michaels%' OR ${collection} LIKE '%Touched by Love%' OR ${collection} LIKE '%Touched%' OR ${collection} LIKE '%Working Miracles%' OR ${collection} LIKE '%Healing Hands%' OR ${collection} LIKE '%Miracles From Nowhere%' OR ${collection} LIKE '%Chasing A Dream%') then 'LLP'
              when ${type}='movie' and (${collection} NOT LIKE '%Letter Never Sent%' OR ${collection} NOT LIKE '%It Had to Be You%' OR ${collection} NOT LIKE '%Instant Nanny%' OR ${collection} NOT LIKE '%Love Throws a Curve%' OR ${collection} NOT LIKE '%Romantically Speaking%' OR ${collection} NOT LIKE '%Wedding Do Over%' OR ${collection} NOT LIKE '%When Duty Calls%' OR ${collection} NOT LIKE '%The Right Girl%' OR ${collection} NOT LIKE '%Brimming With Love%' OR ${collection} NOT LIKE '%Sweet Surrender%' OR ${collection} NOT LIKE '%Hopeless Romantic%' OR ${collection} NOT LIKE '%Secret Summer%' OR ${collection} NOT LIKE '%Late Bloomer%' OR ${collection} NOT LIKE '%Win, Lose or Love%' OR ${collection} NOT LIKE '%Mr. Write%' OR ${collection} NOT LIKE '%Once Upon a Date%' OR ${collection} NOT LIKE '%Twist of Fate%' OR ${collection} NOT LIKE '%Vision of Love (FKA The Michaels)%' OR ${collection} NOT LIKE '%Secret Millionaire%' OR ${collection} NOT LIKE '%Tomboy%' OR ${collection} NOT LIKE '%Advance & Retreat%' OR ${collection} NOT LIKE '%Diagnosis Delicious%' OR ${collection} NOT LIKE '%Undercover Angel%' OR ${collection} NOT LIKE '%Love on the Vines%' OR ${collection} NOT LIKE '%Woman of the House%' OR ${collection} NOT LIKE '%A Moving Romance%' OR ${collection} NOT LIKE '%Change of Heart%' OR ${collection} NOT LIKE '%Touched by Love (FKA Touched)%' OR ${collection} NOT LIKE '%Can\'t Buy My Love%' OR ${collection} NOT LIKE '%Groomzilla%' OR ${collection} NOT LIKE '%How Not to Propose%' OR ${collection} NOT LIKE '%Sisters of the Groom%' OR ${collection} NOT LIKE '%Mechanics of Love%' OR ${collection} NOT LIKE '%Accidentally in Love%' OR ${collection} NOT LIKE '%Class%' OR ${collection} NOT LIKE '%Keeping Up with the Randalls%' OR ${collection} NOT LIKE '%Rock the House%' OR ${collection} NOT LIKE '%Place Called Home, A%' OR ${collection} NOT LIKE '%Out of the Woods%' OR ${collection} NOT LIKE '%Thicker Than Water%' OR ${collection} NOT LIKE '%Long Shot, The%' OR ${collection} NOT LIKE '%After the Fall%' OR ${collection} NOT LIKE '%Soldier Love Story%' OR ${collection} NOT LIKE '%Mending Fences%' OR ${collection} NOT LIKE '%A Time to Remember%' OR ${collection} NOT LIKE '%Expecting a Miracle%' OR ${collection} NOT LIKE '%Annie’s Point%' OR ${collection} NOT LIKE '%Bound by a Secret%' OR ${collection} NOT LIKE '%Ladies of the House%' OR ${collection} NOT LIKE '%Where There’s a Will%' OR ${collection} NOT LIKE '%Family Plan%' OR ${collection} NOT LIKE '%Backyard Wedding%' OR ${collection} NOT LIKE '%Honeymoon for One%' OR ${collection} NOT LIKE '%Wedding Daze%' OR ${collection} NOT LIKE '%The Nanny Express%' OR ${collection} NOT LIKE '%Fielder’s Choice%' OR ${collection} NOT LIKE '%Generation Gap%' OR ${collection} NOT LIKE '%Love Will Keep Us Together%' OR ${collection} NOT LIKE '%Relative Stranger%' OR ${collection} NOT LIKE '%Working Miracles (aka Healing Hands)%' OR ${collection} NOT LIKE '%Vickery’s Wild Ride%' OR ${collection} NOT LIKE '%The Reading Room%' OR ${collection} NOT LIKE '%Miles From Nowhere (aka Chasing A Dream)%' OR ${collection} NOT LIKE '%Ordinary Miracles%' OR ${collection} NOT LIKE '%Home Fires Burning%' OR ${collection} NOT LIKE '%Vision of Love%' OR ${collection} NOT LIKE '%The Michaels%' OR ${collection} NOT LIKE '%Touched by Love%' OR ${collection} NOT LIKE '%Touched%' OR ${collection} NOT LIKE '%Working Miracles%' OR ${collection} NOT LIKE '%Healing Hands%' OR ${collection} NOT LIKE '%Miracles From Nowhere%' OR ${collection} NOT LIKE '%Chasing A Dream%') then 'movie' end;;
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

  dimension: streamathon {
    type: string
    sql: case when ${TABLE}.email in ('gregorys0@att.net',
      'kimmiwhite03@gmail.com',
      'lynnp54jsj@gmail.com',
      'bri.lafond2014@gmail.com',
      'tasrn7@yahoo.com',
      'rhodes.carlie16@gmail.com',
      'karibenesh@hotmail.com',
      'funlbarb65@gmail.com',
      'donna444444@yahoo.com',
      'charlottewoodsmith10@gmail.com',
      'carroll0605@yahoo.com',
      'jodilchristensen@hotmail.com',
      'sarah_walley@hotmail.com',
      'whitedeadgoats@gmail.com',
      'meredithblock@hotmail.com',
      'keri.barhorst@gmail.com',
      'jessica.rosian@yahoo.com',
      'Kcchristin@aol.com',
      'deesmedellin@aol.com',
      'pooper123444@gmail.com',
      'lisagreen11@yahoo.com',
      'aechapman@gmail.com',
      'tdreis@windstream.net',
      'seyma_bennett@hotmail.com',
      'lilmse73@gmail.com',
      'shavonne.waltman75@yahoo.com',
      'ralyson27@gmail.com',
      'telon.weathington@gmail.com',
      'srippon11@aol.com',
      'punky.easthom0208@gmail.com',
      'mcelroynancy@hotmail.com',
      'bhjustice@carolina.rr.com',
      'ltmzprincess15@aol.com',
      'luckmck@gmail.com',
      'maldonadoirma96@yahoo.com',
      'aubreydaniels84@yahoo.com',
      'maryjosh.8979@gmail.com',
      'bevazazz@aol.com',
      'lindsayfairbrother@gmail.com',
      'rodrick2@comcast.net',
      'thunderbird_797@yahoo.com',
      'EMCHELLY77@AOL.COM',
      'valerieahrens02@gmail.com',
      'buggaboo338@gmail.com',
      'lawsonsmaidservice@gmail.com',
      'Jalescia23@gmail.com',
      'marymeadows63@yahoo.com',
      'qm25@yahoo.com',
      'rpmorgan1954@yahoo.com',
      'patriciamedina2014@yahoo.com',
      'kathyandron2013@hotmail.com',
      'leahsthomas@live.com',
      'cas_kitkat@hotmail.com',
      'carrieburks23@yahoo.com',
      'cjcompton38@yahoo.com',
      'brianeberts1720@yahoo.com',
      'anitastephens1720@yahoo.com',
      'superdayday7@yahoo.com',
      'adecorso25@gmail.com',
      'pokey_ton@icloud.com',
      'rboots84@gmail.com',
      'kayleecrab95@gmail.com',
      'nancynelson90@gmail.com',
      'srmetcalfe@att.net',
      'christyhayward99@gmail.com',
      'robynbyrd1973@rocketmail.com',
      'shannonlvitvitsky@yahoo.com',
      'kinpa28@aol.com',
      'lunsfordkrystal@yahoo.com',
      'mamamoose2015@gmail.com',
      'paulinasaylor23@gmail.com',
      'hotalin22@yahoo.com',
      'pinkyirwin@aol.com',
      'holdncole90@icloud.com',
      'bchandler2x3@gmail.com',
      'pheetphancy@gmail.com',
      'west.180@wright.edu',
      'crystinrousey84@gmail.com',
      'alese.robinson@yahoo.com',
      'kristinac8813@gmail.com',
      'kelly.freeman@cox.net',
      'rayneehaze@yahoo.com',
      'savannah2nduhs@gmail.com',
      'maribethbreidel@gmail.com',
      'cherie08753@aol.com',
      'amandacarter0809@gmail.com',
      'jesusmama07@yahoo.com',
      'pismodunes1@aol.com',
      'griffeytiffany@yahoo.com',
      'madisonwenninger@gmail.com',
      'kimberly.botts@att.net',
      'lamonicaandholly@gmail.com',
      'miss.samanthaaa@gmail.com',
      'annettemdecker@yahoo.com',
      'Most3nvied89@gmail.com',
      'oma7132012@yahoo.com',
      'pjmill4@aol.com',
      'rhonda3840@hotmail.com',
      'cmbjesupga@aol.com',
      'jlynch0509@gmail.com',
      'Electrikis77@aol.com',
      'angieemt5@gmail.com',
      't.karina85@yahoo.com',
      'teraculverwell@gmail.com',
      'pamela_smith1959@yahoo.com',
      'annedeaton44@gmail.com',
      'kacyanngraves0723@gmail.com',
      'daniellepreston995@gmail.com',
      'isabellatugman@yahoo.com',
      'amandajbrenee@gmail.com',
      'sisterofcountry@yahoo.com',
      'deckhoff1970@yahoo.com',
      'Sarahkittredge6112@gmail.com',
      'dianemorris0561@aol.com',
      'astewart0120@hotmail.com',
      'sherb91@yahoo.com',
      'domingo_avigale@yahoo.com',
      'dchagdes@hotmail.com',
      'Samanthamack713@gmail.com',
      'amberalise19870105@gmail.com',
      'Itsmeeedm86@gmail.com',
      'jc731106@gmail.com',
      'mommy007of3@aol.com',
      'tigger_lover16701@yahoo.com',
      'Kathydeal@verizon.net',
      'mjkeller55@aol.com',
      'alliegirl00@gmail.com',
      'hickstina2009@gmail.com',
      'marci-lyn@hotmail.com',
      'crazy4boxtops@aol.com',
      'stacy0479@yahoo.com',
      'nease.jessica@yahoo.com',
      'kayley_christine@yahoo.com',
      'wldbfort98@yahoo.com',
      'trinajsnow@gmail.com',
      'debbie_slaven@hotmail.com',
      'owenschristley.d@gmail.com',
      'Kimmyzoey3@gmail.com',
      'medicwife06@yahoo.com',
      'hackerkimberly69@gmail.com',
      'nickells.emily@yahoo.com',
      'jgaskill26@gmail.com',
      'nurseblondie28@yahoo.com',
      '8vette@gmail.com',
      'MJGork@yahoo.com',
      'supamomof4@gmail.com',
      'bushwood44@yahoo.com',
      'henson1488@gmail.com',
      'jessicammillers@gmail.com',
      'Killacam5690@gmail.com',
      'mochiesmom@yahoo.com',
      'jobell13@aol.com',
      'tkthornton06@gmail.com',
      'carvizu17@gmail.com',
      'kswiney1@gmail.com',
      'chassy59@msn.com',
      'sdeel78@hotmail.com',
      'debbie5deb67@yahoo.com',
      'Roxanne_redifer@hotmail.com',
      'sweeps687@gmail.com',
      'lmw21491@gmail.com',
      'aroure3991@gmail.com',
      'tclockett81@gmail.com',
      'shaunanderton336@yahoo.com',
      'bluewolfmm@hotmail.com',
      'dimarispapaleo@gmail.com',
      'araemomma2014@outlook.com',
      'MYCASANOVAS123@GMAIL.COM',
      'Karagill54@icloud.com',
      'krissi91881@yahoo.com',
      'alanachika1127@gmail.com',
      'daultonsmom94@gmail.com',
      'wingard.christina@gmail.com',
      'vickizagami@gmail.com',
      'robin.lohman@gmail.com',
      'melaina_phillips@aol.com',
      'vickijo840@yahoo.com',
      'shonnie74@yahoo.com',
      'honeybunny360@gmail.com',
      'kelndan11@yahoo.com',
      'care3868@cox.net',
      'staceyblazer@yahoo.com',
      'shelly9667@icloud.com',
      'rainsmom19@gmail.com',
      'nicholz07@yahoo.com',
      'ashleynicole689@gmail.com',
      'Sherrym780@gmail.com',
      'coolnan109@yahoo.com',
      'jlleone2013@gmail.com',
      'lxltrue_babylxl@hotmail.com',
      'valeriebalochney69@gmail.com',
      'jasfields@yahoo.com',
      'tinaleejone@gmail.com',
      'yeleybunch@gmail.com',
      'jennday79@gmail.com',
      'lisa_govreau@yahoo.com',
      'twytie127@yahoo.com',
      'danijoe2018@yahoo.com',
      'fvl.coupon@gmail.com',
      'cocoaanderson@hotmail.com',
      'cassiepenley@yahoo.com',
      'libbysmom0309@icloud.com',
      'l0isj@yahoo.com',
      'aagray89@gmail.com',
      'jodymcastro@gmail.com',
      'laurajacobsen74@gmail.com',
      'kittycat.sweeps03@gmail.com',
      'mginpa@gmail.com',
      'Good2go58@yahoo.com',
      'missysue16@gmail.com',
      'elptrck1973@gmail.com',
      'kenzie316@msn.com',
      'angelwings2319@gmail.com',
      'mrs.chelseajones2018@gmail.com',
      'katiegrewell@yahoo.com',
      'laurasouthern36@gmail.com',
      'Dkrodgers84@yahoo.com',
      'kaydawl42@aol.com',
      'Acon83ac@gmail.com',
      'nfprudden313@gmail.com',
      'djaggers82@gmail.com',
      'kkk8737@yahoo.com',
      'nicole0177@hotmail.com',
      'beautifulangel3636@gmail.com',
      'chey@btconline.net',
      'irjas1976@gmail.com',
      'dilseydust@gmail.com',
      'carmenejacobs@gmail.com',
      'vallen4142018@gmail.com',
      'ashleyh8584@yahoo.com',
      'Chrissyj1014@gmail.com',
      'terrieepps36@gmail.com',
      'kenabublitz19@gmail.com',
      'cbetourne@cox.net',
      'franknsense22@gmail.com',
      'tnyltrs@juno.com',
      'tclouters@gmail.com',
      'crystalgrey36@gmail.com',
      'halotater@gmail.com',
      'ajones2028@yahoo.com',
      'melynn2369@gmail.com',
      'nmiller419@outlook.com',
      'suehumph@yahoo.com',
      'florencerobbins38@gmail.com',
      'hanersarahjo@gmail.con',
      'kbrooks722@gmail.com',
      'chanceuse23@yahoo.com',
      'marlenamulloy83@gmail.com',
      'Lauren_1978@hotmail.com',
      'tlewis@olemac.net',
      'The1stelf@gmail.com',
      'paulaeverill@gmail.com',
      'Taffy5202002@yahoo.com',
      'jbrunscheon@gmail.com',
      'waterfallgirl17@gmail.com',
      'gailward1954@gmail.com',
      'Teelyn69@gmail.com',
      'freshstart388@gmail.com',
      'ksarahelizabeth@gmail.com',
      'lgsigers@yahoo.com',
      'jkams3@sbcglobal.net',
      'paulburkey@me.com',
      'defevertammy@yahoo.com',
      'ashleysilvers0917@gmail.com',
      'jpringle@sstelco.com',
      'jodypalama808@gmail.com',
      'ltaylor86550@gmail.com',
      'matkinson077@yahoo.com',
      'grannym58@aol.com',
      'Crystal_knight2003@yahoo.com',
      'kasnjim@gmail.com',
      'sapp_beverly@yahoo.com',
      'maurstad@wiktel.com',
      'Ishiahelmer@yahoo.com',
      'maxwellgramigna2@yahoo.com',
      'barbara.wall61@gmail.com',
      'kourtneymcintosh@gmail.com',
      'pennypam59@gmail.com',
      'karenelisanford@gmail.com',
      'moejacks53@gmail.com',
      'info@guidingsteps.com',
      'caw_67449@yahoo.com',
      'kneecree@gmail.com',
      'firenice1979@yahoo.com',
      'donna@donnamaukonen.com',
      'pvazok@gmail.com',
      'Ohanaforever33@gmail.com',
      'novaleeregister2015@gmail.com',
      'devfam@ptd.net',
      'moranatephanie57@gmail.com',
      'sweetlilmo@aol.com',
      'ec1015@hotmail.com',
      'sblmmtj@roadrunner.com',
      'jangarza06@gmail.co',
      'corenemama@gmail.com',
      'sclanton243@gmail.com',
      'fuzzyvincent615@gmail.com',
      'luv2ridein85@yahoo.com',
      'Hudsonrena26@gmail.com',
      'mcpritts@verizon.net',
      'cleaning@stignatius.net',
      'tonyoswald@comcast.net',
      'mjbnkl@gmail.com',
      'Rockntrina@gmail.com',
      'jeaniemarie37@yahoo.com',
      'pienzi2000@gmail.com',
      'maryacole3@yahoo.com',
      'lecorataylor@hotmail.com',
      'sprkle_iii@sbcglobal.net',
      'NovaNew4@outlook.com',
      'wagron2@yahoo.com',
      'louisegreen711@yahoo.com',
      'adiva43@gmail.com',
      'bkziesmer@gmail.com',
      'truelovedove7@aol.com',
      'lmckenzie1967@yahoo.com',
      'leeandronda33@gmail.com',
      'jenningsdanielle34@gmail.com',
      'barkss@sbcglobal.net',
      'eileenhiggins4@gmail.com',
      'thompkinsorita3@gmail.com',
      'irishmom968@msn.com',
      'ajscountrygirl@yahoo.com',
      'software@tlovec.com',
      'grannaelf@me.com',
      'enaausbrooks83@gmail.com',
      'Tonygordon1738@gmail.com',
      'gatorgirl1511@gmail.com',
      'pkmk8488@yahoo.com',
      'slvargas7@outlook.com',
      'duranmommy@gmail.com',
      'starls1@comcast.net',
      'tatertot3741@gmail.com',
      'eknutson@yahoo.com',
      'jerkins.malinda@icloud.com',
      'roni_c@sbcglobal.net',
      'cathehuson@gmail.com',
      'stollsl09@gmail.com',
      'shaub13@gmail.com',
      'Jenna.m.powley@gmail.com',
      'jennifer1025.jf@gmail.com',
      'wilcox7599@gmail.com',
      'squeakysmiles@frontier.com',
      'ahugejeffgordonfan@yahoo.com',
      'missreichl@gmail.com',
      'mayria21@icloud.com',
      'tyouse1968@gmail.com',
      'sun2brite1@yahoo.com',
      'danitag39@gmail.com',
      'Mariahartman58@gmail.com',
      'mpurple805@gmail.com',
      'little1bandita@gmail.com',
      'kt.perry@yahoo.com',
      'avon4michelle@yahoo.com',
      'tkm12734@yahoo.com',
      'kmatthews2011@live.com',
      'jtrevor20@msn.com',
      'christyp0625@gmail.com',
      'kassabma@charter.net',
      'honeylynn2@yahoo.com',
      'moegia12@gmail.com',
      'ginagilpin@hotmail.com',
      'miss_michelle_rn@yahoo.com',
      'katathom4now@yahoo.com',
      'allredpro2@gmail.com',
      'smkao@hotmail.com',
      'rfunk@nemr.net',
      'bamac89@gmail.com',
      'christyshatley41@gmail.com',
      'greaseballs80@gmail.com',
      'bambiholladay2@gmail.com',
      'hydiebean2017@gmail.com',
      'mirnaprobst6@gmail.com',
      'kristine.hornsby@gmail.com',
      'Mlslshell@hotmail.com',
      'mglgeorge@yahoo.com',
      'Mommyofa6pack@aol.com',
      'groogruxking40@gmail.com',
      '81380lawson@gmail.com',
      'joshuanana3205@yahoo.com',
      'joannamarielarson@gmail.com',
      'dachkc19@aol.com',
      'simplyme6226@gmail.com',
      'mmarsh14@student.atlantatech.edu',
      'vstambolia@gmail.com',
      'starrlee18@yahoo.com',
      'roakinin@gmail.com',
      'pjle60@frontier.com',
      'ambiebac@aol.com',
      'miannemalberg@gmail.com',
      'dr7657@bellsouth.net',
      'crystalcapelle@gmail.com',
      'amandadominguez2926@gmail.com',
      'pastornoahfarmer@eastviewwesleyan.com',
      'lrothenstein@gmail.com',
      'mariak.willis@gmail.com',
      'ladyjoop72@gmail.com',
      'Lena.Bartell604@gmail.com',
      'gigilvskevin@gmail.com',
      'realmccoy581@gmail.com',
      'zoomlorizoom@yahoo.com',
      'jenniferstars04@yahoo.com',
      'jenaramz@yahoo.com',
      'donna.thebaglady@yahoo.com',
      'Vtvanlen@ncsu.edu',
      'courtneyfaulkner1@hotmail.com',
      'ginnyloy@yahoo.com',
      'estelamedina1518@gmail.com',
      'crosson47@hotmail.com',
      'sobeatifullove51@gmail.com',
      'bfocusonhim@aol.com',
      '1mattie21566@gmail.com',
      'mzglover68@aol.com',
      'ssherm89@gmail.com',
      'amber.inesta@icloud.com',
      'jnolen@tampabay.rr.com',
      'cruisingal1967@gmail.com',
      'camcd3@comcast.net',
      'rdftsmith@me.com',
      'kerifreeburg@hotmail.com',
      'Hartwellmj@gmail.com',
      'hairdresser1408@yahoo.com',
      'connersmimi35@yahoo.com',
      'hauptman.sarah@gmail.com',
      'audraparker71@yahoo.com',
      'psa0504@sbcglobal.net',
      'coolnana109@yahoo.com',
      'nalogirl@outlook.com',
      'lisag_2006@hotmail.com',
      'tlahr44@yahoo.com',
      'danielsdeborah449@gmail.com',
      'marssp51@yahoo.com',
      'talonesi@live.com',
      'docmo1979@gmail.com',
      'brandyc187@gmail.com',
      'lisaostrow100@gmail.com',
      'mommy.hernandez53@gmail.com',
      'rajeespot@gmail.com',
      'tannasc@yahoo.com',
      'rebecca.brantner64@yahoo.com',
      'sleepee55@gmail.com',
      'lisamarieloranger@gmail.com',
      'eeyore707070@aol.com',
      'babygirl78362@yahoo.com',
      'greensheryl169@gmail.com',
      'bellasmommy21@gmail.com',
      'whiteheadlacy03@gmail.com',
      'lilb546502@yahoo.com',
      'countrymom9360@gmail.com',
      'Owllover197641@gmail.com',
      'jessykabennett@gmail.com',
      'amysteigman@gmail.com',
      'fesnow1191@gmail.com',
      'Lindsay_klotz@hotmail.com',
      'kathymedicus@yahoo.com',
      'staceytindell@yahoo.com',
      'daydreamer0404@aol.com',
      'texasketochick@gmail.com',
      'alysweep@outlook.com',
      'sadisticsam0328@gmail.com',
      'tamistrader6@gmail.com',
      'folletted343@gmail.com',
      'kitts381@gmail.com',
      'dogmom195050@gmail.com',
      'mandermoore@yahoo.com',
      'mamabear32217@gmail.com',
      'crissysimpson05@aol.com',
      'jbridges7664@gmail.com',
      'Jparks0329@gmail.com',
      'Huntress3711@yahoo.com',
      'candyeduncan@icloud.com',
      'shacquelb@yahoo.com',
      'm.helton75@yahoo.com',
      'stasydeal@gmail.com',
      'Alexantcord@aol.com',
      'demadeal@gmail.com',
      'Bsgaway@gmail.com',
      'mademarina@gmail.com',
      'ddriz76@gmail.com',
      'mclee062007@comcast.net',
      'rlhintze@msn.com',
      'ashleymariah5554@gmail.com',
      'iris.m.caballero@gmail.com',
      'eamar1967@gmail.com',
      'Kendall63012@aol.com',
      'sherri71268@hotmail.com',
      'am94jm@yahoo.com',
      'amanda691987@aol.com',
      'Sharkweek74@hotmail.com',
      'Lawaw1974@att.net',
      'lynnettesabot@gmail.com',
      'ejones37096@gmail.com',
      'heerdeangela@yahoo.com',
      'vitacosmopolita@yahoo.com',
      'cgc_mommy@hotmail.com',
      'amberlynne1184@yahoo.com',
      'jmullins2068@gmail.com',
      'rey2527@gmail.com',
      'lynde.tomd@verizon.net',
      'holdenjoycelin92@gmail.com',
      'marsha3338@gmail.com',
      'zippys0801@gmail.com',
      'tjscooters@aol.com',
      'mary.zwahr@yahoo.com',
      'masonpriscilla49@yahoo.com',
      'twinsplusaprincess@gmail.com',
      'DALILANGELS@HOTMAIL.COM',
      'stephjcarlson@hotmail.com',
      'merjenta88@gmail.com',
      'Cyn.alv0520@gmail.com',
      'blj140@icloud.com',
      'bcruzruiz1@gmail.com',
      'gdhmch971@gmail.com',
      'sara.richie13@hotmail.com',
      'audrey.cross26@yahoo.com',
      'shelmer80@gmail.com',
      'kluvst@roadrunner.com',
      'hillari_brooks@yahoo.com',
      'Betty.vanderen61@gmail.com',
      'Sanddancer2000@yahoo.com',
      'sissy2648@comcast.net',
      'brandsellsco@gmail.com',
      'jackieking550@Yahoo.com',
      'laurelbuttars@gmail.com',
      'shaynagriffith23@gmail.com',
      'meganne23@yahoo.com',
      'botticelliandrew@gmail.com',
      'carolscorner1962@gmail.com',
      'dsamson1204@yahoo.com',
      'gman4061@yahoo.com',
      'midgemarkus@hotmail.com',
      'john61868@hotmail.com',
      'cowgirlway1@gmail.com',
      'scascio81@yahoo.com',
      'linda.carbajal@att.net',
      'hsmith0519@gmail.com',
      'briellesmom16@aol.com',
      'melissa.mehlhoff@gmail.com',
      'zuberbuehlersarah@hotmail.com',
      'girlhere11@gmail.com',
      'susansplace1@gmail.com',
      'tapango1@aol.com',
      'Moe7982006@yahoo.com',
      'ann47@yahoo.com',
      'linb212@yahoo.com',
      'christielombardo@yahoo.com',
      'kvalletta@gmail.com',
      'lfarcich85@yahoo.com',
      'laydeebuggangel@hotmail.com',
      'mdcdski1@comcast.net',
      'magicsnowflake89@gmail.com',
      'nanac07788@gmail.com',
      'proudauntie82@gmail.com',
      'Vicki.patrick@yahoo.com',
      'villalt1@live.com',
      'tiffanynichole89@gmail.com',
      'bkt11610@gmail.com',
      'Pantoja.angelica@yahoo.com',
      'chawkins631985@gmail.com',
      'cnwboyle@yahoo.com',
      'caligal293@gmail.com',
      'loganjanelx0@gmail.com',
      'terriwaites58@gmail.com',
      'winstowin123@gmail.com',
      'billy_sherrell@yahoo.com',
      'mccarronjedi@hotmail.com',
      'mommylovescooper@gmail.com',
      'Delrahim69@gmail.com',
      'mbarnum3@tampabay.rr.com',
      'jeani7075@Yahoo.com',
      'paulawilkins36@gmail.com',
      'jovanb77@comcast.net',
      'maresilver@aol.com',
      'caassie3@gmail.com',
      'marlene_butler@yahoo.com',
      'pjfeasel@yahoo.com',
      'rabssb@att.net',
      'jenniferellenbryan@gmail.com',
      'nanaof3a@yahoo.com',
      'dshermansr@yahoo.com',
      'missyann0921@gmail.com',
      'diamondcarol27@hotmail.com',
      'cindyd@metrocast.net',
      'jessicagaither@gmx.com',
      'barham2206@gmail.com',
      'connie.y58@gmail.com',
      'markandbevw@gmail.com',
      'dayanawade55@hotmail.com',
      'onlybyfaith2@yahoo.com',
      'bulmertheresa@gmail.com',
      'marcylang6@gmail.com',
      'autumn3036@gmail.com',
      'zahndrattina@yahoo.com',
      'wildtiger1956@yahoo.com',
      'totkendrick@live.com',
      'abe.liandro@yahoo.com',
      'sschafer2008@yahoo.com',
      'darlenedaycare@gmail.com',
      'kathyshelley1960@yahoo.com',
      'mrshodg@gmail.com',
      'bunnyfur@aol.com',
      'bigphatdaddy2@gmail.com',
      'yardart606@yahoo.com',
      'valoriesimons@yahoo.com',
      'flowerpow20@gmail.com',
      'sunnydaysmiles@gmail.com',
      'audranikle@gmail.com',
      'sandralopez87@yahoo.com',
      'pgosain413@gmail.com',
      'ruckmanfam@yahoo.com',
      'Bobsiglow@msn.com',
      'kkriefall@yahoo.com',
      'tavila70@aol.com',
      'ikaxuk@gmail.com',
      'amy.broadie@yahoo.com',
      'sparmeland@optonline.net',
      'Pat1937@hotmail.com',
      'anisa.chandler93@gmail.com',
      'turstadt@charter.net',
      'lfjones@lisd.org',
      'millerfam0519@aol.com',
      'mukluk03@yahoo.com',
      'crcorey@gmail.com',
      'horse_lover77@hotmail.com',
      'paul.middleton@comcast.net',
      'shobe65@gmail.com',
      'DALMATIAN3585@YAHOO.COM',
      'NGOCCHAUBUI@YAHOO.COM',
      'cleanerose@aol.com',
      'm_buresh2@hotmail.com',
      'darnita.nettles83@gmail.com',
      'annienurse22@gmail.com',
      'fan8racer@yahoo.com',
      'crawfordnancy345678@gmail.com',
      'don.oettinger@gmail.com',
      'meelees@hotmail.com',
      'alyssa_ronco@hotmail.com',
      'skbradfield@gmail.com',
      'tennischick15@gmail.com',
      'mclarvoe@aol.com',
      'mollyhubenschmidt@yahoo.com',
      'marycmann.jenders@gmaim.com',
      'cind7678@yahoo.com',
      'kaiserfam5@gmail.com',
      'sjadams0510@yahoo.com',
      'dyfincham@icloud.com',
      'kathynorris7@gmail.com',
      'sunshinetryde@gmail.com',
      'cr_unlimited@charter.net',
      'judybwork@bellsouth.net',
      'sknose90@sbcglobal.net',
      'dscalfw2@gmail.com',
      'terryripley@ymail.com',
      'jkamp002@gmail.com',
      'lenasomerville@icloud.com',
      '062902@gmail.com',
      'lmhallas@yahoo.com',
      'tinajay_1999@yahoo.com',
      'babyblueeyes968@gmail.com',
      'vidlnoc1@gmail.com',
      'tomsager33@yahoo.com',
      'cvb91855@aol.com',
      'cristina_cottingham@yahoo.com',
      'kks93074@yahoo.com',
      'cdarling6@austin.rr.com',
      'zachzook31@gmail.com',
      'robin55sal2017@icloud.com',
      'judymills50@yahoo.com',
      'mbovin1@att.net',
      'Kimberlytolliver769@yahoo.com',
      'mwdstar12@gmail.com',
      'scottnva34@yahoo.com',
      'cyndb123@hotmail.com',
      'pamy51355@sbcglobal.net',
      'lar58eeeves@yahoo.com',
      'mustang251982@yahoo.com',
      'conirod76@gmail.com',
      'thais2276@gmail.com',
      'bevielight@yahoo.com',
      'nonnaholly@yahoo.com',
      'bethe95@yahoo.com',
      'khtrowell@aol.com',
      'meloneywh1966@gmail.com',
      'sonshinekidsmi@gmail.com',
      'kelleywheeler1962@gmail.com',
      'beaniebear@me.com',
      'cassieorr519@hotmail.com',
      'maureenriley82@gmail.com',
      'Jimscc@gmail.com',
      'bjsoldchicken@yahoo.com',
      'sarahe.walker38@gmail.com',
      'sawalters@yahoo.com',
      'theresaturnbo@gmail.com',
      'thomasearlene5@gmail.com',
      'istone0223@gmail.com',
      'xgrammyapplesx@runbox.com',
      '1mac62638@gmail.com',
      'shennawright21@yahoo.com',
      'becca2potter@yahoo.com',
      'dianened2011@gmail.com',
      'llantz4@verizon.net',
      'loripeterson33@gmail.com',
      'gdhb227@aol.com',
      'jonettesg@gmail.com',
      'sunniewoodyplus2@msn.com',
      'barnecut@sbcglobal.net',
      'dcuchine@gmail.com',
      'rachaelpalmer-101@hotmail.com',
      'meh92116@gmail.com',
      'marqs4me@comcast.net',
      'c.daddario@twc.com',
      'jsdvis1954@gmail.com',
      'clogrady50@gmail.com',
      'naturalcontext@comcast.net',
      'ef_parker@yahoo.com',
      'reneeslockets@hot.rr.com',
      'binb43@hotmail.com',
      'oldmangray71@gmail.com',
      'joann527@yahoo.com',
      'kelly_abbott8@yahoo.com',
      'nancystoddard@aol.com',
      'bwstoughton@gmail.com',
      'chinarain7@att.net',
      'victom@sbcglobal.net',
      'dcljjlove@gmail.com',
      'lukylady09@aol.com',
      'sarahblumb@gmail.com',
      'scowles9@aol.com',
      'crawford2311@windstream.net',
      'm.s.tahtinen@charter.net',
      'rclewisburg@outlook.com',
      'tinkerbell@prodigy.net',
      'Reitz220@gmail.com',
      'Lynmicaletti@gmail.com',
      'toniad3434@yahoo.com',
      'mygirlkat74@gmail.com',
      'eflynn@me.com',
      'bushtrotter83@gmail.com',
      'kitrygirl557@gmail.com',
      'briafarren@mac.com',
      'lgabriel4@aol.com',
      'ozoneman@comcast.net',
      'debby@ldtms.org',
      'lesgonz@msn.com',
      'marysorenson11@gmail.com',
      'janetmesser2011@gmail.com',
      'ledpod@comcast.net',
      'Georgialamb264@yahoo.com',
      'debbierock21@yahoo.com',
      'emcdonald0928@gmail.com',
      'tholman@yadtel.net',
      'jeanneyoung79@yahoo.com',
      'pthompson2457@gmail.com',
      'kldusty@gmail.com',
      'jockywanab@aol.com',
      'vamumc@aol.com',
      'horsecrazymary@yahoo.com',
      'php403@att.net',
      'smithscm@gmail.com',
      'dawnbrown428@yahoo.com',
      'fishingbears92@yahoo.com',
      'lauravalitsky@yahoo.com',
      'karensebesta1953@yahoo.com',
      'mleesteve@yahoo.com',
      'ag9jetvw@gmail.com',
      'oktwitch74@gmail.com',
      'bpherg@msn.com',
      'aussimdeals@gmail.com',
      'adajackson8@gmail.com',
      'shannon215.se@gmail.com',
      'jannyjo1117@gmail.com',
      'dwoodruff@unitedwaygenesee.org',
      'skbaker1961@gmail.com',
      'azarcher61@gmail.com',
      'Trhurt@gmail.com',
      'tricyclewagontrain@yahoo.com',
      'biggin_little@yahoo.com',
      'jilessa@comcast.net',
      'nikki7_94@yahoo.com',
      'barb.shoemaker@yahoo.com',
      'maryanncampbell84@gmail.com',
      'sophiedarlin.js@gmail.com',
      'cnapihaa24@gmail.com',
      'saltedcornbread@gmail.com',
      'mike.pierce@charter.net',
      'dhdd-mcguire@sbcglobal.net',
      'quiltwithfriends@windstream.net',
      'mepaw72@yahoo.com',
      'chelbynow@gmail.com',
      'beverlyallen77@yahoo.com',
      'karenspencer956@gmail.com',
      'hollyanderson1980@gmail.com',
      'laurel1152@msn.com',
      'lady_b_raven@yahoo.com',
      'DbackBob@gmail.com',
      'kgtabes02@yahoo.com',
      'ablair1018@gmail.com',
      'gwellsey1968@gmail.com',
      'hoagjrjohn@gmail.com',
      'philly0081@gmail.com',
      'leaannarcher@gmail.com',
      'kiiomgps@gmail.com',
      'chuot56@aol.com',
      'cynthiablevins1124@gmail.com',
      'lasims54@hotmail.com',
      'jack.means@gmail.com',
      'ra22krause@gmail.com',
      'tomvickireg@gmail.com',
      'bjmowdy@gmail.com',
      'Ashleymcclelland25@gmail.com',
      'rboze0310@yahoo.com',
      'jwchoctawindian@live.com',
      'zanybell@yahoo.com',
      'terrann55@gmail.com',
      'tommuyjoe9121971@gmail.com',
      'mewhyte42@gmail.com',
      'medcod15@gmail.com',
      'keberst@aol.com',
      'brendamaxime@hotmail.com',
      'marcia.ramie@yahoo.com',
      'khockaday@aol.com',
      'sherrysiebert@gmail.com',
      'bonniesaba99@gmail.com',
      'pmoose22@gmail.com',
      'bkim93@yahoo.com',
      'gallegosmax1941@gmail.com',
      'jonessteven@att.net',
      'tomika.anderdon@gmail.com',
      'rsallee07@gmail.com',
      'pattyearl3@gmail.com',
      'lionesst31@yahoo.com',
      'faithann2458@gmail.com',
      'robertmartinezrm388@gmail.com',
      'lmf0517@frontier.com',
      'toribugg46TR@gmail.com',
      'Debbiecallison@ymail.com',
      'gene.leo.murri@gmail.com',
      'rr31bags@gmail.com',
      'mom2jebs@gmail.com',
      'yosam118@yahoo.com',
      'sheltv_86@yahoo.com',
      'rmdroach@sbcglobal.net',
      'tervsue@gmail.com',
      'timothyhagermaker@icloud.com',
      'rohloff1980@hotmail.com',
      'mshis4ev@yahoo.com',
      'Kaileacastle20@gmail.com',
      'miranda.ribinson84@yahoo.com',
      'rkr4JC@gmail.com',
      'julie.e.passarelli@gmail.com',
      'Cassandra65@gmail.com',
      'angelandamelia@aol.com',
      'tjg510@hotmail.com',
      'pdcagle@yahoo.com',
      'paultilliefranks@gmail.com',
      'bricope03111979@yahoo.com',
      'raca96@yahoo.com',
      'dlcurrent@gmail.com',
      'psteineke@gmail.com',
      'sabrina50edwards@gmail.com',
      'braveheartfreedom@gmail.com',
      'kimi@kimiquick.com',
      'gethigpen78@hotmail.com',
      'buffy0200@gmail.com',
      'mygubyek3@gmail.com',
      'elizabethgilliland007@gmail.com',
      'rachlyn62@yahoo.com',
      'Tsirrine@ymail.com',
      'calgarymouse84@gmail.com',
      'sheilasmithmyfaith07@yahoo.com',
      'catmom628@yahoo.com',
      'katydid_85201@yahoo.com',
      'a.forbeswatkins@gmail.com',
      'quickdraw117@hotmail.com',
      'marcelino.aviles@yahoo.com',
      'bethbarsness@gmail.com',
      'rmueller2@cox.net',
      'pegroebuck@yahoo.com',
      'Tesserific1@gmail.com',
      'lamerex4@gmail.com',
      'Snewman.622@comcast.net',
      'snuggles7071985@aol.com',
      'ljk041563@gmail.com',
      'susandunn1948@aol.com',
      'Marissamartini81@gmail.com',
      'grannymag@cfl.rr.com',
      'binnettetammy@gmail.com',
      'soccerchic327@gmail.com',
      'dreece1961@windstream.net',
      'rebeccaloska@gmail.com',
      'mimi103066@gmail.com',
      'raginmomm@aol.com',
      'dixiedamsel@gmail.com',
      'ktyler@ptd.net',
      'Jes4Jesus@yahoo.com',
      'cutekurly@hotmail.com',
      'dbwampler52@gmail.com',
      'tlbenes60@gmail.com',
      'betsyelaine816@aol.com',
      'psaltis@comcast.net',
      'genentexas@att.net',
      'seasonalgirl24@gmail.com',
      'talktoclark@gmail.com',
      'sripanti@gmail.com',
      'vanessasouthern@hotmail.com',
      'mylittlebus@hotmail.com',
      'kublerfamily7@gmail.com',
      'tyouse68@gmail.com',
      'willardkiser1@gmail.com',
      'werewithchrist@yahoo.com',
      'arivera84@gmail.com',
      'danyell1965@gmail.com',
      'tembucktwo@gmail.com',
      'lindseymcook22@yahoo.com',
      'benlashonda@gmail.com',
      'radjim7@gmail.com',
      'teresahensley49@gmail.com',
      'rmitzel71@gmail.com',
      'greeneyes0128@yahoo.com',
      'Ajlepak@hotmail.com',
      'marystru74@yahoo.com',
      'jmkeefer71@gmail.com',
      'plbarnes95@yahoo.com',
      'tammy.galloway@yahoo.com',
      'Hope.Thorpe1973@gmail.com',
      'megan.stough@yahoo.com',
      'ckwheeler@cableone.net',
      'sweetjules226@gmail.com',
      'Thomaca122lm70@gmail.com',
      'glmcaleese@yahoo.com',
      'Mollieleanne@gmail.com',
      'Zibadoo1@gmail.com',
      'stcmclp@me.com',
      'rebekah.mesa@gmail.com',
      'fayem526@gmaul.com',
      'mukismama@yahoo.com',
      'jcline49@comcast.net',
      'Maliequartero@gmail.com',
      'bhaugum62197@gmail.com',
      'tater0729@yahoo.com',
      'Kristyn.perez82@gmail.com',
      'drtalimoni@gmail.com',
      'srbs4@msn.com',
      'sandymatuse@bellsouth.net',
      'stuartbenoff@gmail.com',
      'blndesunshine@gmail.com',
      'vickyras1953@gmail.com',
      '41.morrison.12@gmail.com',
      'lasdougb@gmail.com',
      'daphne@featherhaven.com',
      'tjohnson73099@att.net',
      'jsheila1962@yahoo.com',
      'ranora.cosselmon@gmail.com',
      'jckay31@gmail.com',
      'blentz@me.com',
      'tonygirl4ever16@yahoo.com',
      'djscows_2@yahoo.com',
      'bethfullwood@yahoo.com',
      'Sueharts@yahoo.com',
      'brendaeacker@yahoo.com',
      'charafarmtx@yahoo.com',
      'patties1118@gmail.com',
      'brittfamilysc@gmail.com',
      'fhewkin1955@gmail.com',
      'dfallis3744@gmail.com',
      'tammyiglesias67@gmail.com',
      'TRICIA.STARK24@LIVE.COM',
      'rmeb4ever@gmail.com',
      'kathygarcia12@yahoo.com',
      'ashmbyron@gmail.com',
      'oldjakeljiii@aol.com',
      'mduhon72@yahoo.com',
      'lundd1@comcast.net',
      'rharbour90@gmail.com',
      'mgkrieg@att.net',
      'vivanamendoza1966@yahoo.com',
      'jimandchriscastor@gmail.com',
      'carolcomer.cc@gmail.com',
      'stephensAlaurene@yahoo.com',
      'meredithmorris85@gmail.com',
      'tracynord71@gmail.com',
      'rhonda.harvey@centerpointenergy.com',
      'burkholder505@gmail.com',
      'bobbyprice233@gmail.com',
      'denisepfeifer@yahoo.com',
      'sherry_wardlow@yahoo.com',
      'tgriz72@comcast.net',
      'sweetlilmiss88@yahoo.com',
      'miranda.robinson84@yahoo.com',
      'mrs.woodsey_2u@yahoo.com',
      'jim44052@yahoo.com',
      'blessed_valley@yahoo.com',
      'cklesner63@gmail.com',
      'dodgecountry09@gmail.com',
      'day.day1968@yahoo.com',
      'anthompson@cerner.com',
      'ashleysimonet@yahoo.com',
      'samwaggs53@yahoo.com',
      'jwolf0813@gmail.com',
      'kbjsalinas31@gmail.com',
      'scottbyrd@sevier.org',
      'annade1117@yahoo.com',
      'sherrinewman@aol.com',
      'kimdowns12016@gmail.com',
      'mrogier2007@hotmail.com',
      'raynasims06@gmail.com',
      'joycebug@gmail.com',
      'my4floyds@gmail.com',
      'tamarabusterud1990@gmail.com',
      'dblount@sbcglobal.net',
      'inna.titenok@gmail.com',
      'wire1946@comcast.net',
      'kmarshall1497@yahoo.com',
      'griffinaprill@gmail.com',
      'charlottecarlson74@aol.com',
      'ellenmas1@att.net',
      'sexyonotorcyle@gmail.com',
      'sunsetvirgo913@aol.com',
      'karenrevis3756@gmail.com',
      'nanapell@yahoo.com',
      'Lacieandrews81@gmail.com',
      'txbluebonnet6613@yahoo.com',
      'bevey56@frontier.com',
      'senoraclean@gmail.com',
      'PaulChurch38@Gmail.Com',
      'rca.tb@hotmail.com',
      'anthompson@comcast.net',
      'lindaharris11119@gmil.com',
      'scsnana@aol.com',
      'jennifer.kluender@gmail.com',
      'debj.lockwood@gmail.com',
      'diannaculpepper@yahoo.com',
      'jesspouliot@live.com',
      'cheryld10@gmail.com',
      'ddilly@tds.net',
      'PKADILBECK@GMAIL.COM',
      'Ms.Nita45@yahoo.com',
      'PatsyPoggle@gmail.com',
      'adawnmiller02@gmail.com',
      '66jandmozz@gmail.com',
      'carriefife@icloud.com',
      'mbrausa@ivytech.edu',
      'hondagirl14@comcast.net',
      'pytluvsu@aol.com',
      'afulmer52@icloud.com',
      'donandnorma28@att.net',
      'aburnside2535@gmail.com',
      'msonny6@gmail.com',
      'jepinokc@yahoo.com',
      'Blackrosetwilight12@gmail.com',
      'ltate8@yahoo.com',
      'masemaya1@gmail.com',
      'pittmandeborah69@yahoo.com',
      'babygirl198096@gmail.com',
      'sipegirl4366@gmail.com',
      'denisemanagault@gmail.com',
      'melvalpn@yahoo.com',
      'mariscalgloria9@gmail.com',
      'DOAK317@YAHOO.COM',
      'patricia.moyer@yahoo.com',
      'hudgins.haley@yahoo.com',
      'mynanaty@yahoo.com',
      'taylermccollum1@gmail.com',
      'tvattic23@gmail.com',
      'booshee2002@yahoo.com',
      'cgepford@hotmail.com',
      'allwood48@yahoo.com',
      'aldalton@aldns.com',
      'tmsfruitytooty@hormail.com',
      'ronni135@yahoo.com',
      'tpeterson69@ymail.com',
      'hawkinman@gmail.com',
      'broadr@uah.edu',
      'NELSONFAMILY506@gMAIL.COM',
      'pcheesman05@yahoo.com',
      'niecer_la@yahoo.com',
      'smithheather154@gmail.com',
      'moe_laura@hotmail.com',
      'mzmari379@gmail.com',
      'ufaf@scotteggleston.com',
      'ajervin77@gmail.com',
      'rhonda.smyth@gmail.com',
      'wwjdilovegod1234@yahoo.com',
      'neshemkatherine@yahoo.com',
      'strasburg5@yahoo.com',
      'mallorymillsphotography@gmail.com',
      'nlburr@windstream.net',
      'chgingcolors@aol.com',
      'artist_with_passion@yahoo.com',
      'AngWalls25@yahoo.com',
      'vbamanda@hotmail.com',
      'horsr_lover77@hotmail.com',
      'snyderm67@gmail.com',
      'unicorn12@windstream.net',
      'judymilstead903@gmail.com',
      'MissGummy@yahoo.com',
      'mblount1977@gmail.com',
      'laura_blackwell@hotmail.com',
      'gigiboo1987@gmail.com',
      'simshoward742@gmail.com',
      'jennandmalakye216@gmail.com',
      'jamesgrimes12260@gmail.com',
      'wimberlyw84@gamil.com',
      'ecatarina.grant@gmail.com',
      'islandmouse07@yahoo.com',
      'debpetzold@yahoo.com',
      'capturedbykristina@gmail.com',
      'jenniferlynn8581@gmail.com',
      'faitele04@gmail.com',
      'shezywicke@yahoo.com',
      'ljpoz254@yahoo.com',
      'lammrowe@comcast.net',
      'jennarteaga78@gmail.com',
      'missmosrite@yahoo.com',
      'babywolf4.na@gmail.com',
      'april.wrightnoe@gmail.com',
      'maricastro1992@icloud.com',
      'karenelupton@gmail.com',
      'myminpins5@gmail.com',
      'rainier_services@comcast.net',
      'mutterbear@verizon.net',
      'debbiesearson@yahoo.com',
      'stevenusaf@hotmail.com',
      'taz1984ghs@yahoo.com',
      'okinslow@gmail.com',
      'hr061006@yahoo.com',
      'stampcz3@gmail.com',
      'smssimmons@gmail.com',
      'mommyoftwo08.cm@gmail.com',
      'aprobertucci247@yahoo.com',
      'taylorj1998@nycap.rr.com',
      'sportegan212@gmail.com',
      'georgespeidel46@gmail.com',
      'chniwalker@gmail.com',
      'kristendavis@embarqmail.com',
      'pookorama1@yahoo.com',
      'cmcstylist87@yahoo.com',
      'nhaley20@gmail.com',
      'tammy.81@yahoo.com',
      'brettmc93@gmail.com',
      'clunde1221@gmail.com',
      'crainsd1@gmail.com',
      'bdsmom75@gmail.com',
      'sweetzhunny66@gmail.com',
      'alyssakiwi9@gmail.com',
      'bjwillow65@gmail.com',
      'venesafalcon@gmail.com',
      'brandy_miles@aol.com',
      'rebeccawaltrip8564@gmail.com',
      'brassman_1999@yahoo.com',
      'atmbroke@yahoo.com',
      'olgasurm@hotmail.com',
      'keribarnette3@yahoo.com',
      'kjwereb@gmail.com',
      'sherryrus6@aol.com',
      'ana-marth@uiowa.edu',
      'tkdixieglory@yahoo.com',
      'jts0727@gmail.com',
      'williamkorte@yahoo.com',
      'annamarie.robert@yahoo.com',
      'yatesr821@gmail.com',
      'winfieldjulia52@yahoo.com',
      'jennifercoker1991@gmail.com',
      'palomo69graciela.gp@gmail.com',
      'irishviaemail@gmail.com',
      'vcabrenda@gmail.com',
      'knighthawkeminiatures@gmail.com',
      't.magaly40@gmail.com',
      'jeng817@gmail.com',
      'caco11@comcast.net',
      'nanadianne61@yahoo.com',
      'feathernetter@yahoo.com',
      'musiclady3852@yahoo.com',
      'lorna.a.c9876@hotmail.com',
      '1grandmakikki@gmail.com',
      'clmay1961@yahoo.com',
      'beckyssalon@hotmail.com',
      'bhibicki@gmail.com',
      'rlgraham@lakemac.net',
      'Karaokecloud@yahoo.com',
      'bwymer@kent.edu',
      'lesliejfleisher@gmail.com',
      'johnsonvermona@gmail.com',
      'jessica.kiddo@gmail.com',
      'tinaheaton97@gmail.com',
      'tashadavis68@icloud.com',
      'lorriespano@frontier.com',
      'jrrej1950@yahoo.com',
      'becky.chapoy@gmail.com',
      'portega@franklinisd.net',
      'ysanford87@gmail.com',
      'myiaperry24@gmail.com',
      'stitcher3@hotmail.com',
      'westcottgloria@yahoo.com',
      'rena.lunsford@aol.com',
      'jeanmeljess123@outlook.com',
      'brubakerk.22@gmail.com',
      'lgalbraith@new.rr.com',
      'mthorpefamily@att.net',
      'rickandcheyennesmom@gmail.com',
      'mosslisa47@yahoo.com',
      'clbsrobinson@yahoo.com',
      'carrie@turansky.com',
      'chults02@gmail.com',
      'montes.inc@netzero.net',
      'billiejeaneddy@gmail.com',
      'piston2293@yahoo.com',
      'melissamurley68@gmail.com',
      'carissamathews08@gmail.com',
      'jmschlesselman@windstream.net',
      'jnabbas@icloud.com',
      'sampson20@msn.com',
      'payton.farley@gmail.com',
      'cassieann1517@gmail.com',
      'blessedmommaforlife@gmail.com',
      'hale.katrinad@gmail.com',
      'PJBrake@gmail.com',
      'nanasbabys222@gmail.com',
      'juliakatecurls78@yahoo.com',
      'mpalmer49@suddenlink.net',
      'cortezlord13@gmail.com',
      'soniaybarra2@gmail.com',
      'bduncans1@gmail.com',
      'judy.arrowood@charter.net',
      'snperry@hotmail.com',
      'suzysoulian60@gmail.com',
      'sheilajo5@gmail.com',
      'reelmusic.mh@gmail.com',
      'smitty1michelle@gmail.com',
      'csaby@centurylink.net',
      'Karen-Michaels@utc.edu',
      'oneofgodspreciouschild@gmail.com',
      'social@toriclose.com',
      'adriennedaniels66@gamil.com',
      'harmonymarty@yahoo.com',
      'terrydmore@gmail.com',
      'pradeep.yelamanti@gmail.com',
      'annie.may1958@yahoo.com',
      'ivgillen1@yahoo.com',
      'thehomemadetreasures@hushmail.com',
      'thehopelighthouse@hushmail.com',
      'beliawildcat56@yahoo.com',
      'lsmoody@outlook.com',
      'roseannsomm@gmail.com',
      'reedusandrea827@gmail.com',
      'charlesgaither357@gmail.com',
      'jonesjones303@yahoo.com',
      'meira0311@gmail.com',
      'oompas1974@yahoo.com',
      'lilkirk30421@gmail.com',
      'simplypamm@gmail.com',
      'jb20144336@gmail.com',
      'welovearizonasun@yahoo.com',
      'mcdowell.carla20@gmail.com',
      'mayirsi@yahoo.com',
      'pattmccullou@bellsouth.net',
      'Teela828@gmail.com',
      'Lennette.r.mclaurin@gmail.com',
      'diaperangel@yahoo.com',
      'amacoloniallife.pete@gmail.com',
      'joolzbohr@gmail.com',
      'nancyjengle@gmail.com',
      'alnsonya@msn.com',
      'hwebber360@gmail.com',
      'robbinsdebbie50@yahoo.com',
      'nchwg2@yahoo.com',
      'amyj1107@gmail.com',
      'daughterofpurity1987@gmail.com',
      'marieguy83@yahoo.com',
      'jamesgary846@gmail.com',
      'bishop55ej@yahoo.com',
      'dicktwi@gmail.com',
      'lesleystunkel39@gmail.com',
      'megreatgal26@yahoo.com',
      'juliajones1995.jj@gmail.com',
      'pammywhammy871@gmail.com',
      'acole4265@gmail.com',
      'pmhcj38@gmail.com',
      'tna6504@gmail.com',
      'mkucsma@suddenlink.net',
      'dre@drebarnes.com',
      'wwrdj5@outlook.com',
      'baringerblast@yahoo.com',
      'aimee@lisa-law.com',
      'suannecrockett@gmail.com',
      'rgmoore33159@gmail.com',
      'tiny@shtc.net',
      'iamkaitlynking@gmail.com',
      'mendy_hansen@yahoo.com',
      'akmcmurtrey@gmail.com',
      'nurse4him77@gmail.com',
      'angcannon@gmail.com',
      'snbzellner@gmail.com',
      'tracy.tarver75@yahoo.com',
      'theresaharlow3@yahoo.com',
      'jth3334@outlook.com',
      'dp_ink@hotmail.com',
      'cecilia.durand@ad.ndus',
      'alroloff@gmail.com',
      'tammydorris@gmail.com',
      'jacquendalenae@gmail.com',
      'debijhowe@aol.com',
      'debcollins8264@hotmail.com',
      'diamoniquewilliams1@gmail.com',
      'mcjfut444@gmail.com',
      'djoann24@yahoo.com',
      'carmen07032@hotmail.com',
      'tawnyalowrance@gmail.com',
      'cheryl.nims@gmail.com',
      'virginia_dubois55@yahoo.com',
      'tabithajones06@yahoo.com',
      'gorlando2005@charter.net',
      'wmccoy@uptv.com',
      'Northamanda189@gmail.com',
      'country1991girl@hotmail.com',
      'teamhall907@gmail.com',
      'brendataylor783@gmail.com',
      'craine2001@hotmail.com',
      'brndycd@gmail.com',
      'reyes5519@sbcglobal.net') then 'Stream-a-thon' else 'Control' end;;
  }

}
