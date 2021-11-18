view: retention {
    derived_table: {
      sql: with

              all_first_play as (
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
              ),

              paid_campaign as (
              select
                context_campaign_source,
                context_campaign_medium,
                anonymous_id,
                context_ip
              from javascript_upff_home.pages
              where context_campaign_source in ('fb','bing_ads','ig')
              ),

              order_completed as (
              select
                user_id,
                anonymous_id,
                context_ip
                from javascript.order_completed
              ),

              order_completed_by_campaign_p1 as (
              select oc.user_id
              from order_completed as oc
              left join paid_campaign as pc
              on oc.anonymous_id = pc.anonymous_id
              ),

              order_completed_by_campaign_p2 as (
              select oc.user_id
              from order_completed as oc
              left join paid_campaign as pc
              on oc.context_ip = pc.context_ip
              ),

              order_completed_by_campaign_full as (
              select * from order_completed_by_campaign_p1
              union all
              select * from order_completed_by_campaign_p2
              ),

              order_completed_by_campaign as (
              select
                distinct user_id,
                1 as paid_sub
              from order_completed_by_campaign_full
              ),

              purchase_event as ( /* a */
              select * from http_api.purchase_event
              ),

              purchase_event_analytic as ( /* b */
              select
                user_id,
                row_number() over (partition by user_id order by timestamp) as event_num,
                topic,
                platform,
                subscription_frequency,
                date(created_at) as create_dt,
                date(timestamp) as status_dt,
                date_diff(date(created_at), date(timestamp), day) as tenure,
                extract(dayofweek from date(created_at)) as create_day,
                case when topic in ('customer.product.free_trial_created') then 1 else 0 end as trialist
              from purchase_event
              where topic is not null
              order by user_id
              ),

              paid_media_users_for_churn as (
              select pe.*, oc.paid_sub
              from purchase_event_analytic as pe
              left join order_completed_by_campaign as oc
              on pe.user_id = oc.user_id
              ),

              free_trial_date as (
              select
                user_id,
                case when topic = 'customer.product.free_trial_created' then status_dt else null end as trial_dt
              from purchase_event_analytic
              ),

              free_trial_date_lookup as (
              select
                user_id,
                min(trial_dt) as min_trial_dt
              from free_trial_date
              where trial_dt is not null
              group by user_id
              ),

              free_trial_subs_aggregate as (
              select
                user_id,
                trialist
                from purchase_event_analytic
                group by 1,2
              ),

              free_trial_subs_lookup as ( /*** creates lookup table for trialists ***/
              select user_id, trialist from free_trial_subs_aggregate where trialist = 1
              ),

              last_event as ( /* c */
              select
                user_id,
                max(event_num) as max_event,
                max(status_dt) as max_status
              from purchase_event_analytic
              group by 1
              order by 1
              ),

              analytic_table as ( /* d */
              select b.*, c.max_event, c.max_status, f.min_trial_dt
              from paid_media_users_for_churn as b
              inner join last_event as c
              on b.user_id = c.user_id and b.event_num = c.max_event
              inner join free_trial_date_lookup as f
              on b.user_id = f.user_id
              where b.user_id in (select user_id from free_trial_subs_lookup)
              order by b.user_id
              ),

              pre_flags as (
              select *, date_diff(max_status, date(min_trial_dt), day) as tenure2
              from analytic_table
              ),

              flags as ( /*** e ***/
              select *,
                case
                  when topic in ('customer.deleted', 'customer.product.free_trial_expired', 'customer.product.cancelled', 'customer.product.expired') then 1 else 0 end as churn_status,
                case
                  when tenure2 between 0 and 30 then 1
                  when tenure2 between 30 and 60 then 2
                  when tenure2 between 60 and 90 then 3
                  when tenure2 between 90 and 120 then 4
                  when tenure2 between 120 and 150 then 5
                  when tenure2 between 150 and 180 then 6
                  when tenure2 between 180 and 210 then 7
                  when tenure2 between 210 and 240 then 8
                  when tenure2 between 240 and 270 then 9
                  when tenure2 between 270 and 300 then 10
                  when tenure2 between 300 and 330 then 11
                  when tenure2 between 330 and 360 then 12
                  when tenure2 between 360 and 390 then 13
                  when tenure2 between 390 and 420 then 14
                  when tenure2 between 420 and 450 then 15
                  when tenure2 between 450 and 480 then 16
                  when tenure2 between 480 and 510 then 17
                  when tenure2 between 510 and 540 then 18
                  when tenure2 between 540 and 570 then 19
                  when tenure2 between 570 and 600 then 20
                  when tenure2 between 600 and 630 then 21
                  when tenure2 between 630 and 660 then 22
                  when tenure2 between 660 and 690 then 23
                  when tenure2 between 690 and 720 then 24
                  when tenure2 between 720 and 750 then 25
                  when tenure2 between 750 and 780 then 26
                  when tenure2 between 780 and 810 then 27
                  when tenure2 between 810 and 840 then 28
                  when tenure2 between 840 and 870 then 29
                  when tenure2 between 870 and 900 then 30
                  when tenure2 between 900 and 930 then 31
                  when tenure2 between 930 and 960 then 32
                  when tenure2 between 960 and 990 then 33
                  when tenure2 between 990 and 1020 then 34
                  when tenure2 between 1020 and 1050 then 35
                  else 36 end as num_months,
                extract(dayofweek from min_trial_dt) as trial_day,
                case
                  when date(min_trial_dt) between '2019-01-01' and '2019-03-31' then '2019-Q1'
                  when date(min_trial_dt) between '2019-04-01' and '2019-06-30' then '2019-Q2'
                  when date(min_trial_dt) between '2019-07-01' and '2019-09-30' then '2019-Q3'
                  when date(min_trial_dt) between '2019-10-01' and '2019-12-31' then '2019-Q4'
                  when date(min_trial_dt) between '2020-01-01' and '2020-03-31' then '2020-Q1'
                  when date(min_trial_dt) between '2020-04-01' and '2020-06-30' then '2020-Q2'
                  when date(min_trial_dt) between '2020-07-01' and '2020-09-30' then '2020-Q3'
                  when date(min_trial_dt) between '2020-10-01' and '2020-12-31' then '2020-Q4'
                  when date(min_trial_dt) between '2021-01-01' and '2021-03-31' then '2021-Q1'
                  when date(min_trial_dt) between '2021-04-01' and '2021-06-30' then '2021-Q2'
                  when date(min_trial_dt) between '2021-07-01' and '2021-09-30' then '2021-Q3'
                else 'Missing' end as quarter,
                case
                  when date(min_trial_dt) between '2019-01-01' and '2019-01-31' then '2019-01'
                  when date(min_trial_dt) between '2019-02-01' and '2019-02-28' then '2019-02'
                  when date(min_trial_dt) between '2019-03-01' and '2019-03-31' then '2019-03'
                  when date(min_trial_dt) between '2019-04-01' and '2019-04-30' then '2019-04'
                  when date(min_trial_dt) between '2019-05-01' and '2019-05-31' then '2019-05'
                  when date(min_trial_dt) between '2019-06-01' and '2019-06-30' then '2019-06'
                  when date(min_trial_dt) between '2019-07-01' and '2019-07-31' then '2019-07'
                  when date(min_trial_dt) between '2019-08-01' and '2019-08-31' then '2019-08'
                  when date(min_trial_dt) between '2019-09-01' and '2019-09-30' then '2019-09'
                  when date(min_trial_dt) between '2019-10-01' and '2019-10-31' then '2019-10'
                  when date(min_trial_dt) between '2019-11-01' and '2019-11-30' then '2019-11'
                  when date(min_trial_dt) between '2019-12-01' and '2019-12-31' then '2019-12'
                  when date(min_trial_dt) between '2020-01-01' and '2020-01-31' then '2020-01'
                  when date(min_trial_dt) between '2020-02-01' and '2020-02-29' then '2020-02'
                  when date(min_trial_dt) between '2020-03-01' and '2020-03-31' then '2020-03'
                  when date(min_trial_dt) between '2020-04-01' and '2020-04-30' then '2020-04'
                  when date(min_trial_dt) between '2020-05-01' and '2020-05-31' then '2020-05'
                  when date(min_trial_dt) between '2020-06-01' and '2020-06-30' then '2020-06'
                  when date(min_trial_dt) between '2020-07-01' and '2020-07-31' then '2020-07'
                  when date(min_trial_dt) between '2020-08-01' and '2020-08-31' then '2020-08'
                  when date(min_trial_dt) between '2020-09-01' and '2020-09-30' then '2020-09'
                  when date(min_trial_dt) between '2020-10-01' and '2020-10-31' then '2020-10'
                  when date(min_trial_dt) between '2020-11-01' and '2020-11-30' then '2020-11'
                  when date(min_trial_dt) between '2020-12-01' and '2020-12-31' then '2020-12'
                  when date(min_trial_dt) between '2021-01-01' and '2021-01-31' then '2021-01'
                  when date(min_trial_dt) between '2021-02-01' and '2021-02-28' then '2021-02'
                  when date(min_trial_dt) between '2021-03-01' and '2021-03-31' then '2021-03'
                  when date(min_trial_dt) between '2021-04-01' and '2021-04-30' then '2021-04'
                  when date(min_trial_dt) between '2021-05-01' and '2021-05-31' then '2021-05'
                  when date(min_trial_dt) between '2021-06-01' and '2021-06-30' then '2021-06'
                  when date(min_trial_dt) between '2021-07-01' and '2021-07-31' then '2021-07'
                  when date(min_trial_dt) between '2021-08-01' and '2021-08-31' then '2021-08'
                  when date(min_trial_dt) between '2021-09-01' and '2021-09-30' then '2021-09'
                else 'Missing' end as month
              from pre_flags
              ),

              lkup as (
              select user_id, min_trial_dt from flags where month in (
              '2021-01',
              '2021-02',
              '2021-03',
              '2021-04',
              '2021-05',
              '2021-06',
              '2021-07',
              '2021-08')
              ),

              lkup_firstplay as (
              select
                cast(user_id as string) as user_id,
                seriesname_watched_1st as series,
                case
                  when seriesname_watched_1st = 'Heartland' then 'Heartland'
                  when seriesname_watched_1st = 'Bringing Up Bates' then 'Bates'
                  when seriesname_watched_1st not in ('Heartland','Bringing Up Bates') then 'Other Serials'
                  when seriesname_watched_1st is null then 'Movie'
                else 'Null' end as first_play
                from ad_hoc.first_watched
              ),

              missing_user as (
              select
                user_id from lkup where user_id not in (select user_id from lkup_firstplay)
              ),

              post_flags as (
              select a.*, b.*
              from flags as a
              left join lkup_firstplay as b
              on a.user_id = b.user_id
              ),

              aggregate as (
              select /*** final query calculates churn rate and aggregates by days of week ***/
                trial_day,
                num_months,
                case when paid_sub = 1 then 1 else 0 end as paid_sub_indicator,
                platform,
                subscription_frequency,
                sum(churn_status) as num_churners,
                count(*) as total_pop,
                quarter,
                month,
                topic,
                first_play
              from post_flags
              where create_day is not null
              group by 1,2,3,4,5,8,9,10,11
              order by 1,2
              ),

              counts as (
              select 'paid_campaign' as table_name, count(*) as size from paid_campaign
              union all
              select 'order_completed' as table_name, count(*) as size  from order_completed as order_completed_counts
              union all
              select 'order_completed_by_campaign_p1' as table_name, count(*) as size  from order_completed_by_campaign_p1 as order_completed_by_campaign_p1_counts
              union all
              select 'order_completed_by_campaign_p2' as table_name, count(*) as size  from order_completed_by_campaign_p2 as order_completed_by_campaign_p2_counts
              union all
              select 'order_completed_by_campaign_full' as table_name, count(*) as size  from order_completed_by_campaign_full as order_completed_by_campaign_full_counts
              union all
              select 'order_completed_by_campaign' as table_name, count(*) as size  from order_completed_by_campaign as order_completed_by_campaign_counts
              union all
              select 'purchase_event' as table_name, count(*) as size  from purchase_event as purchase_event_counts
              union all
              select 'purchase_event_analytic' as table_name, count(*) as size  from purchase_event_analytic as purchase_event_analytic_counts
              union all
              select 'paid_media_users_for_churn' as table_name, count(*) as size  from paid_media_users_for_churn as paid_media_users_for_churn_counts
              union all
              select 'free_trial_date' as table_name, count(*) as size  from free_trial_date as free_trial_date_counts
              union all
              select 'free_trial_date_lookup' as table_name, count(*) as size  from free_trial_date_lookup as free_trial_date_lookup_counts
              union all
              select 'free_trial_subs_aggregate' as table_name, count(*) as size  from free_trial_subs_aggregate as free_trial_subs_aggregate_counts
              union all
              select 'free_trial_subs_lookup' as table_name, count(*) as size  from free_trial_subs_lookup as free_trial_subs_lookup_counts
              union all
              select 'last_event_' as table_name, count(*) as size  from last_event as last_event_counts
              union all
              select 'analytic_table' as table_name, count(*) as size from analytic_table as analytic_table_counts
              union all
              select 'flags' as table_name, count(*) as size  from flags as flags_counts
              union all
              select 'aggregate' as table_name, count(*) as size  from aggregate as aggregate_counts
              ),

              tenure_mismatch as (
              select * from flags where month = '2021-09' and tenure > 30
              )

              select * from aggregate where month in
              (
              '2021-01',
              '2021-02',
              '2021-03',
              '2021-04',
              '2021-05',
              '2021-06',
              '2021-07',
              '2021-08')

              /*
              select count(*) as n from flags
              union all
              select count(*) as n from post_flags
              */

              /*
              select
                count(*) as n,
                heartland_flg,
                bates_flg,
                other_flg,
                movie_flg
              from lkup_firstplay
              group by 2,3,4,5
              */

              /*
              select count(*) as n, heartland_flg, bates_flg, other_flg, null_flg from lkup_firstplay group by 2,3,4,5 order by 2
              */

              /*
              select * from ad_hoc.first_watched
              */

              /*
              select * from aggregate where month in
              (
              '2021-01',
              '2021-02',
              '2021-03',
              '2021-04',
              '2021-05',
              '2021-06',
              '2021-07',
              '2021-08')
              */

              /*
              select month, tenure2, count(*) as n from flags where month in
              (
              '2020-04',
              '2020-05',
              '2020-06',
              '2020-07',
              '2020-08')
              group by 1,2
              order by 1,2
              */
               ;;
    }

    measure: count {
      type: count
      drill_fields: [detail*]
    }

    dimension: trial_day {
      type: number
      sql: ${TABLE}.trial_day ;;
    }

    dimension: num_months {
      type: number
      sql: ${TABLE}.num_months ;;
    }

    dimension: paid_sub_indicator {
      type: number
      sql: ${TABLE}.paid_sub_indicator ;;
    }

    dimension: platform {
      type: string
      sql: ${TABLE}.platform ;;
    }

    dimension: subscription_frequency {
      type: string
      sql: ${TABLE}.subscription_frequency ;;
    }

    dimension: num_churners {
      type: number
      sql: ${TABLE}.num_churners ;;
    }

    dimension: total_pop {
      type: number
      sql: ${TABLE}.total_pop ;;
    }

    dimension: quarter {
      type: string
      sql: ${TABLE}.quarter ;;
    }

    dimension: month {
      type: string
      sql: ${TABLE}.month ;;
    }

    dimension: topic {
      type: string
      sql: ${TABLE}.topic ;;
    }

    dimension: first_play {
      type: string
      sql: ${TABLE}.first_play ;;
    }

    set: detail {
      fields: [
        trial_day,
        num_months,
        paid_sub_indicator,
        platform,
        subscription_frequency,
        num_churners,
        total_pop,
        quarter,
        month,
        topic,
        first_play
      ]
    }
  }
