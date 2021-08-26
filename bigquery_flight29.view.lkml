view: bigquery_flight29 {
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
           season_number as season,
           a.title,
           a.video_id as id,
           episode_number as episode,
           date(time_available) as date,
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
             from a2 as a left join titles_id_mapping as b on trim(upper(b.title)) = trim(upper(a.title))),
    /*join master dataset with winback and first time customers table to finish query*/

    master as
    (select a.user_id,
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
    from a left join cc on a.user_id=cc.user_id left join svod_titles.promos as c on a.video_id=c.video_id),

  /* creates viewership flags for each episode per user_id */
    user as(
    select
    user_id, collection, title, episode,
    case when episode=1 then 1 else 0 end as ep01_flag,
    case when episode=2 then 1 else 0 end as ep02_flag,
    case when episode=3 then 1 else 0 end as ep03_flag,
    case when episode=4 then 1 else 0 end as ep04_flag,
    case when episode=5 then 1 else 0 end as ep05_flag,
    case when episode=6 then 1 else 0 end as ep06_flag,
    case when episode=7 then 1 else 0 end as ep07_flag,
    case when episode=8 then 1 else 0 end as ep08_flag,
    case when episode=9 then 1 else 0 end as ep09_flag,
    case when episode=10 then 1 else 0 end as ep10_flag,
    case when episode=11 then 1 else 0 end as ep11_flag,
    case when episode=12 then 1 else 0 end as ep12_flag,
    case when episode=13 then 1 else 0 end as ep13_flag,
    (ep01_flag+ep02_flag+ep03_flag+ep04_flag+ep05_flag+ep06_flag+ep07_flag+ep08_flag+ep09_flag+ep10_flag+ep11_flag+ep12_flag+ep13_flag) as total_eps
  from master
  group by user_id)

    select * from user
  ;;
}

  dimension: total_eps {
    type: number
    sql: {TABLE}.total_eps ;;
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

  # ------
  # Ad Hoc Work
  # ------

  dimension: eps1 {
     sql: CASE
    WHEN ${TABLE}.episode = 1 THEN 1
    ELSE 0
    END ;;
  }

  dimension: eps2 {
    sql: CASE
          WHEN ${TABLE}.episode = 2 THEN 1
          ELSE 0
          END ;;
  }

  dimension: eps3 {
    sql: CASE
          WHEN ${TABLE}.episode = 3 THEN 1
          ELSE 0
          END ;;
  }

  dimension: eps4 {
    sql: CASE
          WHEN ${TABLE}.episode = 4 THEN 1
          ELSE 0
          END ;;
  }

  dimension: eps5 {
    sql: CASE
          WHEN ${TABLE}.episode = 5 THEN 1
          ELSE 0
          END ;;
  }

  dimension: eps6 {
    sql: CASE
          WHEN ${TABLE}.episode = 6 THEN 1
          ELSE 0
          END ;;
  }

  dimension: eps7 {
    sql: CASE
          WHEN ${TABLE}.episode = 7 THEN 1
          ELSE 0
          END ;;
  }

  dimension: eps8 {
    sql: CASE
          WHEN ${TABLE}.episode = 8 THEN 1
          ELSE 0
          END ;;
  }

  dimension: eps9 {
    sql: CASE
          WHEN ${TABLE}.episode = 9 THEN 1
          ELSE 0
          END ;;
  }

  dimension: eps10 {
    sql: CASE
          WHEN ${TABLE}.episode = 10 THEN 1
          ELSE 0
          END ;;
  }

  dimension: eps11 {
    sql: CASE
          WHEN ${TABLE}.episode = 11 THEN 1
          ELSE 0
          END ;;
  }

  dimension: eps12 {
    sql: CASE
          WHEN ${TABLE}.episode = 12 THEN 1
          ELSE 0
          END ;;
  }

  dimension: eps13 {
    sql: CASE
          WHEN ${TABLE}.episode = 13 THEN 1
          ELSE 0
          END ;;
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

  filter: collection_type {
    type: string
  }

  dimension: collections_type_group {
    hidden: no
    type: yesno
    sql: {% condition collections_b %} ${type}{% endcondition %};;
  }



  measure: user_count_collections_b {
    type: count_distinct
    filters: {
      field: collections_group_b
      value: "yes"
    }
    sql: ${user_id} ;;
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

  dimension: promotional_collection_c_{
    hidden: no
    type: yesno
    sql: {%condition promotional_collection_a%} ${collection} {%endcondition%};;
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

}
