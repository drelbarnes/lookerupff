view: sat {
  derived_table: {
    sql: WITH bigquery_timeupdate AS (((with a30 as
      (select video_id,
             max(ingest_at) as ingest_at
      from php.get_titles
      group by 1),

      a3 as
      (select distinct
             metadata_series_name as series,
              case when metadata_season_name in ('Season 1','Season 2','Season 3') then concat(metadata_series_name,'-',metadata_season_name)
                  when metadata_season_name is null then metadata_movie_name
                  when metadata_season_name is null and metadata_movie_name is null and a.duration_seconds>2700 then a.title
                  else metadata_season_name end as collection,
             season_number as season,
             a.title,
             a.video_id as id,
             episode_number as episode,
             date(time_available) as date,
             round(duration_seconds/60) as duration,
             promotion
      from php.get_titles as a left join svod_titles.titles_id_mapping as b on a.video_id=b.id inner join a30 on a30.video_id=a.video_id and a30.ingest_at=a.ingest_at
       where date(a.loaded_at)>='2020-02-13'  ),

      a31 as
      (select mysql_roku_firstplays_firstplay_date_date as timestamp,
                      mysql_roku_firstplays_video_id,
                      user_id,
                      max(loaded_at) as maxloaded
      from looker.roku_firstplays
      group by 1,2,3),

      a32 as
      (select a31.timestamp,
             a31.mysql_roku_firstplays_video_id,
             a31.user_id,
             count(*) as numcount,
             sum(mysql_roku_firstplays_total_minutes_watched) as mysql_roku_firstplays_total_minutes_watched
      from looker.roku_firstplays as a inner join a31 on a.loaded_at=maxloaded and mysql_roku_firstplays_firstplay_date_date=a31.timestamp and a31.mysql_roku_firstplays_video_id=a.mysql_roku_firstplays_video_id and a.user_id=a31.user_id
      group by 1,2,3),

      a4 as
      ((SELECT
          a3.title,
          a.user_id,
          email,
          cast(video_id as int64) as video_id,
          case when collection in ('Season 1','Season 2','Season 3') then concat(series,' ',collection) else collection end as collection,
          series,
          season,
          episode,
          case when series is null and upper(collection)=upper(a3.title) then 'movie'
                           when series is not null then 'series' else 'other' end as type,
          safe_cast(date(a.sent_at) as timestamp) as timestamp,
          a3.duration*60 as duration,
          max(timecode) as timecode,
         'Web' AS source
        FROM
          javascript.durationchange as a inner join a3 on safe_cast(a.video_id as int64)=a3.id inner join http_api.purchase_event as p on a.user_id=p.user_id
        WHERE
          a.user_id IS NOT NULL /*and safe_cast(a.user_id as string)!='0'*/ and a3.duration>0
        GROUP BY 1,2,3,4,5,6,7,8,9,10,11)

      union all

      (SELECT
          title,
          a.user_id,
          email,
          cast(video_id as int64) as video_id,
          case when collection in ('Season 1','Season 2','Season 3') then concat(series,' ',collection) else collection end as collection,
          series,
          season,
          episode,
          case when series is null and upper(collection)=upper(title) then 'movie'
                           when series is not null then 'series' else 'other' end as type,
          safe_cast(date(a.sent_at) as timestamp) as timestamp,
          a3.duration*60 as duration,
          max(timecode) as timecode,
         'iOS' AS source
        FROM
          ios.timeupdate as a inner join a3 on safe_cast(a.video_id as int64)=a3.id inner join http_api.purchase_event as p on a.user_id=p.user_id
        WHERE
          a.user_id IS NOT NULL /*and safe_cast(a.user_id as string)!='0'*/ and a3.duration>0
        GROUP BY 1,2,3,4,5,6,7,8,9,10,11)

        union all

        (SELECT
          title,
          a.user_id,
          email,
          cast(video_id as int64) as video_id,
          case when collection in ('Season 1','Season 2','Season 3') then concat(series,' ',collection) else collection end as collection,
          series,
          season,
          episode,
          case when series is null and upper(collection)=upper(title) then 'movie'
                           when series is not null then 'series' else 'other' end as type,
          safe_cast(date(a.sent_at) as timestamp) as timestamp,
          a3.duration*60 as duration,
          max(timecode) as timecode,
         'iOS' AS source
        FROM
          ios.video_content_playing as a inner join a3 on safe_cast(a.video_id as int64)=a3.id inner join http_api.purchase_event as p on a.user_id=p.user_id
        WHERE
          a.user_id IS NOT NULL /*and safe_cast(a.user_id as string)!='0'*/ and a3.duration>0
        GROUP BY 1,2,3,4,5,6,7,8,9,10,11)

        union all

        (SELECT
          title,
          a.user_id,
          email,
          cast(video_id as int64) as video_id,
          case when collection in ('Season 1','Season 2','Season 3') then concat(series,' ',collection) else collection end as collection,
          series,
          season,
          episode,
          case when series is null and upper(collection)=upper(title) then 'movie'
                           when series is not null then 'series' else 'other' end as type,
          safe_cast(date(a.sent_at) as timestamp) as timestamp,
          a3.duration*60 as duration,
          max(timecode) as timecode,
         'Roku' AS source
        FROM
          roku.video_content_playing as a inner join a3 on safe_cast(a.video_id as int64)=a3.id inner join http_api.purchase_event as p on a.user_id=p.user_id
        WHERE
          a.user_id IS NOT NULL /*and safe_cast(a.user_id as string)!='0'*/ and a3.duration>0
        GROUP BY 1,2,3,4,5,6,7,8,9,10,11)

        union all

        (SELECT
          title,
          a.user_id,
          email,
          cast(video_id as int64) as video_id,
          case when collection in ('Season 1','Season 2','Season 3') then concat(series,' ',collection) else collection end as collection,
          series,
          season,
          episode,
          case when series is null and upper(collection)=upper(title) then 'movie'
                           when series is not null then 'series' else 'other' end as type,
          safe_cast(date(a.sent_at) as timestamp) as timestamp,
          a3.duration*60 as duration,
          max(timecode) as timecode,
         'Android' AS source
        FROM
          android.video_content_playing as a inner join a3 on safe_cast(a.video_id as int64)=a3.id inner join http_api.purchase_event as p on a.user_id=p.user_id
        WHERE
          a.user_id IS NOT NULL /*and safe_cast(a.user_id as string)!='0'*/ and a3.duration>0
        GROUP BY 1,2,3,4,5,6,7,8,9,10,11)

        union all

        (SELECT
          a3.title,
          a.user_id,
          email,
          cast(video_id as int64) as video_id,
          case when collection in ('Season 1','Season 2','Season 3') then concat(series,' ',collection) else collection end as collection,
          series,
          season,
          episode,
          case when series is null and upper(collection)=upper(a3.title) then 'movie'
                           when series is not null then 'series' else 'other' end as type,
          safe_cast(date(a.sent_at) as timestamp) as timestamp,
          a3.duration*60 as duration,
          max(timecode) as timecode,
         'Web' AS source
        FROM
          javascript.video_content_playing as a inner join a3 on safe_cast(a.video_id as int64)=a3.id inner join http_api.purchase_event as p on a.user_id=p.user_id
        WHERE
          a.user_id IS NOT NULL /*and safe_cast(a.user_id as string)!='0'*/ and a3.duration>0
        GROUP BY 1,2,3,4,5,6,7,8,9,10,11)

        union all

        (SELECT
          title,
          a.user_id,
          email,
          cast(video_id as int64) as video_id,
          case when collection in ('Season 1','Season 2','Season 3') then concat(series,' ',collection) else collection end as collection,
          series,
          season,
          episode,
          case when series is null and upper(collection)=upper(title) then 'movie'
                           when series is not null then 'series' else 'other' end as type,
          safe_cast(date(a.sent_at) as timestamp) as timestamp,
          a3.duration*60 as duration,
          max(timecode) as timecode,
         'Roku' AS source
        FROM
          roku.timeupdate as a inner join a3 on safe_cast(a.video_id as int64)=a3.id inner join http_api.purchase_event as p on a.user_id=p.user_id
        WHERE
          a.user_id IS NOT NULL /*and safe_cast(a.user_id as string)!='0'*/ and a3.duration>0
        GROUP BY 1,2,3,4,5,6,7,8,9,10,11)

        union all

      (SELECT
          title,
          a.user_id,
          email,
          video_id,
          case when collection in ('Season 1','Season 2','Season 3') then concat(series,' ',collection) else collection end as collection,
          series,
          season,
          episode,
          case when series is null and upper(collection)=upper(title) then 'movie'
                           when series is not null then 'series' else 'other' end as type,
          safe_cast(date(a.sent_at) as timestamp) as timestamp,
          a3.duration*60 as duration,
          max(timecode) as timecode,
         'Android' AS source
        FROM
          android.timeupdate as a inner join a3 on a.video_id=a3.id inner join http_api.purchase_event as p on a.user_id=p.user_id
        WHERE
          a.user_id IS NOT NULL /*and safe_cast(a.user_id as string)!='0'*/ and a3.duration>0
        GROUP BY 1,2,3,4,5,6,7,8,9,10,11)

        union all

        (SELECT
          distinct
          a3.title,
          a.user_id,
          email,
           mysql_roku_firstplays_video_id as video_id,
          case when collection in ('Season 1','Season 2','Season 3') then concat(series,' ',collection) else collection end as collection,
          series,
          season,
          episode,
          case when series is null and upper(collection)=upper(title) then 'movie'
                           when series is not null then 'series' else 'other' end as type,
          a.timestamp,
          a3.duration*60 as duration,
          mysql_roku_firstplays_total_minutes_watched*60 as timecode,
         'Roku' AS source
        FROM
          a32 as a inner join a3 on  mysql_roku_firstplays_video_id=a3.id inner join http_api.purchase_event as p on a.user_id=p.user_id
        WHERE
          a.user_id IS NOT NULL /*and a.user_id<>'0'*/ and a3.duration>0))

      select *,
             case when date(a.timestamp) between DATE_SUB(date(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), QUARTER)), INTERVAL 0 QUARTER) and
                  DATE_SUB(date(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY)), INTERVAL 0 QUARTER) then "Current Quarter"
                  when date(a.timestamp) between DATE_SUB(date(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), QUARTER)), INTERVAL 1 QUARTER) and
                  DATE_SUB(date(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY)), INTERVAL 1 QUARTER) then "Prior Quarter"
                  when date(a.timestamp) between DATE_SUB(date(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), QUARTER)), INTERVAL 4 QUARTER) and
                  DATE_SUB(date(TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), DAY)), INTERVAL 4 QUARTER) then "YAGO Quarter"
                  else "NA"
                  end as Quarter
      from a4 as a))),

      sat0 as
      (SELECT
        bigquery_timeupdate.email  AS email,
        bigquery_timeupdate.user_id  AS user_id,
        bigquery_timeupdate.collection  AS collection,
        bigquery_timeupdate.title AS title,
        case when (COALESCE(SUM(bigquery_timeupdate.timecode ), 0))>(COALESCE(SUM(bigquery_timeupdate.duration ), 0)) then 100.00 else 100.00*(COALESCE(SUM(bigquery_timeupdate.timecode ), 0))/(COALESCE(SUM(bigquery_timeupdate.duration ), 0)) end as percent_completed,
        case when collection in ('Once Upon A Date','New Life','Barry Brewer: Chicago I\'m Home','All Good Things','The Furnace') and (case when (COALESCE(SUM(bigquery_timeupdate.timecode ), 0))>(COALESCE(SUM(bigquery_timeupdate.duration ), 0)) then 100.00 else 100.00*(COALESCE(SUM(bigquery_timeupdate.timecode ), 0))/(COALESCE(SUM(bigquery_timeupdate.duration ), 0)) end)>70 then 2
        when (case when (COALESCE(SUM(bigquery_timeupdate.timecode ), 0))>(COALESCE(SUM(bigquery_timeupdate.duration ), 0)) then 100.00 else 100.00*(COALESCE(SUM(bigquery_timeupdate.timecode ), 0))/(COALESCE(SUM(bigquery_timeupdate.duration ), 0)) end)>70 then 1 else 0 end  AS points
      FROM bigquery_timeupdate

      WHERE ((bigquery_timeupdate.email  IN ('greysgirl@live.com',
      'ttooks@uptv.com',
      'kathisempf@yahoo.com',
      'sarachamberlain@msn.com',
      'rodriguezrachelle97@yahoo.com',
      'erica34du@gmail.com',
      'rachelledford79@gmail.com',
      'markesefamily@yahoo.com',
      'lorijayejohnson@gmail.com',
      'jacklineknight19@gmail.com',
      'musickconnie71@gmail.com',
      'jaykub@hotmail.com',
      'kirstens451@gmail.com',
      'sipe47102@gmail.com',
      'jkf61@mail.com',
      'ezellp@windstream.net',
      'daveshop18@yahoo.com',
      'teresahusc@gmail.com',
      'kublerfamily7@gmail.com',
      'ptzwlkr@gmail.com',
      'kkosmenko@gmail.com',
      'jerrysmart74@gmail.com',
      'knmiskimen1@gmail.com',
      'jjkchamilton@gmail.com',
      'eahawes@gmail.com',
      'chancehaugen@hotmail.com',
      'billi24@yahoo.com',
      'mdstrong76@yahoo.com',
      'ag2guinness@gmail.com',
      'owenssheri63@gmail.com',
      'hamdeville@yahoo.com',
      'frogs113@hotmail.com',
      'billywill60@yahoo.com',
      'northamanda189@gmail.com',
      'jamienhoyt@aol.com',
      'martamcsween@gmail.com',
      'valentineteena@yahoo.com',
      'pafljess@yahoo.com',
      'northernbeans@charter.net',
      'navynekline@gmail.com',
      'aliyaconner42@gmail.com',
      '1daegli@gmail.com',
      'cjherrington58@gmail.com',
      'cindylstock@yahoo.com',
      'swalesb@att.net',
      'aechapman@gmail.com',
      'motherhubbard_2000@yahoo.com',
      'pbj7958@comcast.net',
      'ablair1018@gmail.com',
      'a.forbeswatkins@gmail.com',
      'sarahmcburney96@gmail.com',
      'christine.sutherly@yahoo.com',
      'cdstaggs@msn.com',
      'slange828@gmail.com',
      'pdcagle@yahoo.com',
      'newcreation4861@yahoo.com',
      'jrluvar1966@gmail.com',
      'epgoad@hotmail.com',
      'thomasgfloyd@outlook.com',
      'tom@kickinksoap.com',
      'cleiby8292@yahoo.com',
      'hstephanie871@gmail.com',
      'cloud_child33@hotmail.com',
      'phylichiacook1123@gmail.com',
      'dwilloughby928@gmail.com',
      'wonderbar57@gmail.com',
      'brchris71@gmail.com',
      'christal.bushey@yahoo.com',
      'maryboyea68.mb@gmail.com',
      'nikki5787@yahoo.com',
      'conetoo@gmail.com',
      'akgyrl@gmail.com',
      'azwhiskeygirl0526@gmail.com',
      'mccrickard_april@yahoo.com',
      'jackiefroeming@gmail.com',
      'mom12195@yahoo.com',
      'coniknits@gmail.com',
      'sleepee55@gmail.com',
      's.keller6363@live.com',
      'mandyeric2@gmail.com',
      'kellyl691969@gmail.com',
      'garylb1968@hotmail.com',
      'welovearizonasun@yahoo.com',
      'melissabasham123@gmail.com',
      'hootencharlene@gmail.com',
      'derry-s@hotmail.com',
      'papabear3111@aol.com',
      'scar9284@gmail.com',
      'laolson66@gmail.com',
      'kjwereb@gmail.com',
      'lilavie59@yahoo.com',
      'sandymatuse@bellsouth.net',
      'callie1506@yahoo.com',
      'mirnaprobst6@gmail.com',
      'mollyhubenschmidt@yahoo.com',
      'bapruden@yahoo.com',
      'joanncreid@gmail.com',
      'resatfowler@triad.rr.com',
      'carola@gvtel.com',
      'rdrowe46@gmail.com',
      'ashleyns83@bellsouth.net',
      'jmcmahan2007@gmail.com',
      'lawrencehclark@gmail.com',
      'daughterofsam2000@yahoo.com',
      'sbell74@yahoo.com',
      'aly8810@gmail.com',
      'yvonnecastellanos33@gmail.com',
      'cconnie14@ymail.com',
      'mystylist.trish@gmail.com',
      'angemarie20@gmail.com',
      'mini_z_2000@yahoo.com',
      'ncmusicman3@yahoo.com',
      'webwolf2002@gmail.com',
      'jokerspeck@hotmail.com',
      'victom@sbcglobal.net',
      'jrspeed27@yahoo.com',
      'lynn6903@yahoo.com',
      'grace9@ptd.net',
      'sapp_beverly@yahoo.com',
      'mrs.peterman88@gmail.com',
      'ashleynicolewatkins4@icloud.com',
      'atypicalgurl@gmail.com',
      'suannecrockett@gmail.com',
      'jonim232@gmail.com',
      'lguldner@gmail.com',
      'swculotta@gmail.com',
      'bgidget05@gmail.com',
      'bricope03111979@yahoo.com',
      'ckonfederath@yahoo.com',
      'hesaved5@hotmail.com',
      'gslzavala@gmail.com',
      'bcappsmidwife@sbcglobal.net',
      'angeleyz34@yahoo.com',
      'eke614@gmail.com',
      'suedavis3@gmail.com',
      'rosycheeks_83@yahoo.com',
      'hrhpch@embarqmail.com',
      'jmitchell24@cfl.rr.com',
      'tbd@omahaweb.net',
      'mciszak2013@gmail.com',
      'markeecandis@yahoo.com',
      'shannon215.se@gmail.com',
      'lilangel711@live.com',
      'chermex1@yahoo.com',
      'mshotrod711@gmail.com',
      'kingsdaughter76@hotmail.com',
      'charri730@aol.com',
      'brian_n_crystal.gorski@yahoo.com',
      'mary.noble46@outlook.com',
      'tinker.phillips@yahoo.com',
      'Montoyak42@yahoo.com',
      'doug@reitmeier.org',
      'janet.joyner0425@gmail.com',
      'theresa.bird@sbcglobal.net',
      'lin.sanchez80@yahoo.com',
      'dalong@rochester.rr.com',
      'varnerbetty.bv@gmail.com',
      'Latoyaramer@gmail.com',
      'Cowgirljeffes1@yahoo.com',
      'kttodd_2000@yahoo.com',
      'serinarichert@gmail.com',
      'dbbro662004@msn.com',
      'charmainekern@gmail.com',
      'pattyreith49@gmail.com',
      'JOYNLIFE@AOL.COM',
      'Kaitlynralexander08@gmail.com',
      'RWAGNER@PTA-CRM.COM',
      'Retailbabe@gmail.com',
      'room306orange@outlook.com',
      'Mkcausey5@gmail.com',
      'sowell57@msn.com',
      'mirmmy@gmail.com',
      'Teribauguess@yahoo.com',
      'mbarnhart2@comcast.net',
      'ljp4563@frontier.com',
      '2017rivergirl@gmail.com',
      'lisaroberts62@gmail.com',
      'tiggergirl1004@gmail.com',
      'raymcclain@yahoo.com',
      'jclark3379@yahoo.com',
      'misner.jolene@yahoo.com',
      'spc_reed@yahoo.com',
      'Tpbarras@outlook.com',
      'ajines@mtnhome.com',
      'yoerger86@gmail.com',
      'lynettekennard@yahoo.com',
      'lynn1king@yahoo.com',
      'dtrudeau79@yahoo.com',
      'angeliaparsons@yahoo.com',
      'aprilleigh74@gmail.com',
      'daw816@gmail.com',
      'cmckelvey1011@aol.com',
      'lindalitch2@gmail.com',
      'jma998@sbcglobal.net',
      'henri.leonard@frontier.com',
      'delaine.watts55@gmail.com',
      'mpmig@yahoo.com',
      'bcdkayak@gmail.com',
      'june.momto8@gmail.com',
      'cgosweiler@gmail.com',
      'Ellagraceg18@gmail.com',
      'rosalynnsoland@sbcglobal.net',
      'dragonflygtr@gmail.com',
      'taylobc@hotmail.com',
      'donaldregle38@gmail.com',
      'vickiwh329@gmail.com',
      'naomapotter@hotmail.com',
      'highlandscv@gmail.com',
      'marshaargue7778@gmail.com',
      'conniedupont94@yahoo.com',
      'Richardmbayle@yahoo.com',
      'cherylmcbride777@gmail.com',
      'Katepappas65@gmail.com',
      'nautadavid@gmail.com',
      'awhitaker1963@yahoo.com',
      'doyralaniz@gmail.com',
      'marysteely@gmail.com',
      'Pdthierry@gmail.com',
      'phelandotti72@gmail.com',
      'melissa77us@yahoo.com',
      'jeshckgsk@yahoo.com',
      'nicolekraeger@yahoo.com',
      'Staceystricklin@hotmail.com',
      'claricebone@att.net',
      'sierrahanners@gmail.com',
      'sutherland.susie@yahoo.com',
      'patt.hansen1970@gmail.com',
      'Clevieelee68@gmail.com',
      'bjcoffey09@gmail.com',
      'cmatthews10@yahoo.com',
      'gracepc@tampabay.rr.com',
      'marrina92@hotmail.com',
      'abauler718@gmail.com',
      'kerihoz@yahoo.com',
      'jaynanevans@yahoo.com',
      'dgoad2011@hotmail.com',
      'Jenn62501@aol.com',
      'mjkoes@gmail.com',
      'billiejo52470@gmail.com',
      'mjonnada67@gmail.com',
      'blspaeth@hotmail.com',
      'michaelfaith7@yahoo.com',
      'buffdaddy5059@hotmail.com',
      'foxtrotrn79@yahoo.com',
      'dgronau8@gmail.com',
      'mannmarie97@yahoo.com',
      'noahsark220@gmail.com',
      'SHARWOOD@IOLAMAIL.COM',
      'Mommy_memories@yahoo.com',
      'ann.collins47@yahoo.com',
      'suzeqscraps@gmail.com',
      'vicky.smhp@gmail.com',
      'rebekahvera87@gmail.com',
      'wendihatfield.wh@gmail.com',
      'curlylocks856@yahoo.com',
      'Plstarcher@yahoo.com',
      'Kiwiemonkeys@gmail.com',
      'martinezmikaela752@gmail.com',
      'barenz@truevine.net',
      'tginocchetti@gmail.com',
      'liz.burhans10@gmail.com',
      'jeanettec.ross@yahoo.com',
      'jle946@comcast.net',
      'debbiewoodruff7@gmail.com',
      'hennessy1017@gmail.com',
      'msjjmid@verizon.net',
      'cramern70@gmail.com',
      'Dingalls@yahoo.com',
      'ramonamannstucson@msn.com',
      'erin_posch@yahoo.com',
      'gem4him52@aol.com',
      'Rad64okie@gmail.com',
      'blklabel77@gmail.com',
      'sararoot45@gmail.com',
      'momonstrike10101@aol.com',
      'leasetfree@gmail.com',
      'thezeeks@sbcglobal.net',
      'eric@theinfinigroup.com',
      'Lfaro@sbcglobal.net',
      'bnimsky@gmail.com',
      'medina.andy9696@gmail.com',
      'irwally63@gmail.com',
      'elidonnelly20@gmail.com',
      'mlaupp221@gmail.com',
      'oshields322@gmail.com',
      'myred4651@yahoo.com',
      'abeandmichelle@gmail.com',
      'rickd32@comcast.net',
      'hollyh.0723@gmail.com',
      'ndogtester@charter.net',
      'dtierno@att.net',
      'pheerthis@yahoo.com',
      'snoopy10yr@yahoo.com',
      'jacobsdonna222@gmail.com',
      'tommydsr@charter.net',
      'caseygigi02@gmail.com',
      'greggarcia6@gmail.com',
      'emilymarie.be@gmail.com',
      'culleenman@gmail.com',
      'rcmaier@venturecomm.net',
      'sillygirl729k@yahoo.com',
      'rochelann@yahoo.com',
      'dorothyclark0726@gmail.com',
      'ielifoglu@gmail.com',
      'marsharinehart1974@gmail.com',
      'josepedraza479@yahoo.com',
      'susanlavish@yahoo.com',
      'sessib@mail.lcc.edu',
      'shergonzales06@yahoo.com',
      'antesmarilyn8@gmail.com',
      'jenniferscanlonjs@gmail.com',
      'jackiesparks50@sbcglobal.net',
      'bearlyecho@yahoo.com',
      'tamikabacchus@bellsouth.net',
      'joydakota2004@yahoo.com',
      'skress@cox.net',
      'dpteneyck@comcast.net',
      'arlenecannon@prodigy.net',
      'peggeo1970@gmail.com',
      'iloveacf@gmail.com',
      'jeffrey.moore7716@my.sinclair.edu',
      'maaefiaampiaw@gmail.com',
      'tuesday_white@yahoo.com',
      'btilley@craftsmennwf.com',
      'rrc201026@gmail.com',
      'harvarddeb@gmail.com',
      'twhgroopie@yahoo.com',
      'dnuncio3177@gmail.com',
      'barbherz1@gmail.com',
      'ronahill@gmail.com',
      '143kimbo@gmail.com',
      'katbabyblu@yahoo.com',
      'katherine.rand@yahoo.com',
      'surggyn@gmail.com',
      'sachleenjellybean@gmail.com',
      'dltilden@gmail.com',
      'jadavidson1023@gmail.com',
      'tsrodda@aol.com',
      'echevarriald@gmail.com',
      'jillybean_107@yahoo.com',
      'malinda147@gmail.com',
      'ngilson@gocai.com',
      'abdielandries@gmail.com',
      'mhhastings82@gmail.com',
      'louissafroehlich@hotmail.com',
      'debbimixon@yahoo.com',
      'lprichard61@gmail.com',
      'wrangred@aol.com',
      'prj1@live.com',
      'nsettimo17@gmail.com',
      'dfgcdg521@gmail.com',
      'leahkarl@yahoo.com',
      'jessica.bont@yahoo.com',
      'ashleywquinn@gmail.com',
      '77holliesearle@gmail.com',
      'nanasprecious9@hotmail.com',
      'photopi2002@yahoo.com',
      'jrtass@hotmail.com',
      'phylicia_fisher@aol.com',
      'vexed73@hotmail.com',
      'trudyrusch@gmail.com',
      'stacyc2010@yahoo.com',
      'kathifiveonefive@gmail.com',
      'akb924@hotmail.com',
      'harmanx5@outlook.com',
      'jfreaks22@hotmail.com',
      'hwebb1950@yahoo.com',
      'tonyawade024@gmail.com',
      'peacefulimagesjen@gmail.com',
      'lathompson@nckcn.com',
      'jippingcindy@yahoo.com',
      'avellanedaedith24@gmail.com',
      'lcwiley@epbfi.com',
      'spolanco1979@gmail.com',
      'mikeand.kelly@yahoo.com',
      'anndriva@yahoo.com',
      'peggychitwood@aol.com',
      'jhochstetler03@yahoo.com',
      'quintinaacres@hotmail.com',
      'aidillevitt@aol.com',
      'cynthia18m@yahoo.com',
      'briceline@yahoo.com',
      'mamalinn@hotmail.com',
      'godisgreatmjm@hotmail.com',
      'morlenmo@gmail.com',
      'weserveabiggod@yahoo.com',
      'blogparadice@gmail.com',
      'blue8791@aol.com',
      'awilliams275@yahoo.com',
      'curtisduluth@gmail.com',
      'elizabeth.freedmandoherty@verizon.net',
      'jenn@barefootlifestyle.org',
      'april.visscher12@gmail.com',
      'cubanprincess69@gmail.com',
      'rvschristina9369@gmail.com',
      'clydesdale824@gmail.com',
      'reneenew7@yahoo.com',
      'amosduke2@gmail.com',
      'littleone9502@gmail.com',
      'clarisal15@gmail.com',
      'af757capt@yahoo.com',
      'bklinkey@hotmail.com',
      'Jrmh2911jr@gmail.com',
      'weatherman1776@gmail.com',
      'chauk67330@aol.com',
      'martinjohn87@gmail.com',
      'mizzteelady83@gmail.com',
      'melinda.j.maier@gmail.com',
      'eallen2013@hotmail.com',
      'bethcaley@gmail.com',
      'wpollema@verizon.net',
      'mamasgirl1974@gmail.com',
      'briethmamaw@yahoo.com',
      'johnapidalove@yahoo.com',
      'adkins1st@yahoo.com',
      'nancybridges1125@gmail.com',
      'youknowwho169@hotmail.com',
      'ashlee520727@hotmail.com',
      'jrhame2@att.net',
      'diana1106@yahoo.com',
      'sharonk.mauger@gmail.com',
      'nancyplushpup@yahoo.com',
      'nicolek924@gmail.com',
      'kmcentire78@gmail.com',
      'gl18401926@att.net',
      'pudding_thing@hotmail.com',
      'mediwoman2000@yahoo.com',
      'jesusfreak-45@hotmail.com',
      'cbatterton66@yahoo.com',
      'michelled_06@hotmail.com',
      'beccarita@live.com',
      'pbranham02@yahoo.com',
      'codydonnally@yahoo.com',
      'car4622@gmail.com',
      'elliscarlasue@gmail.com',
      'peyton15p@gmail.com',
      'jassy7243@yahoo.com',
      'calvinflowers18@yahoo.com',
      'laursen@comcast.net',
      'mlongie@hotmail.com',
      'mrstiffanycondren@yahoo.com',
      'ptracy61@gmail.com',
      'selah7@outlook.com',
      'truiz@startmail.com',
      'debrael777@new.rr.com',
      'elsagurrola@ymail.com',
      'aaleeiyah13@gmail.com',
      'bonitapj@yahoo.com',
      'victoriebreeze@gmail.com',
      '6464puffy@gmail.com',
      'magee1030@yahoo.com',
      'wchaffee@stny.rr.com',
      'dobesfirst@yahoo.com',
      'asc810@gmail.com',
      'pjjawilson@gmail.com',
      'yaitzagil@yahoo.com',
      'kprkr06@gmail.com',
      'rdyck05@yahoo.com',
      'anitarabon72@gmail.com',
      'jaimiherring@gmail.com',
      'specialfew@gmail.com',
      'leighsnow@yahoo.com',
      'amtolbert1999@yahoo.com',
      'sherwhitaker2015@yahoo.com',
      'angeltc705@gmail.com',
      'Shockeypaula48@gmail.com',
      'melisback@comcast.net',
      'tiffanyeakinrn@gmail.com',
      'catherinebaldelomar@yahoo.com',
      'glove_8@msn.com',
      'cissy_neely@yahoo.com',
      'whalenlr49@yahoo.com',
      'jezameeka@gmail.com',
      'haleyfae0811@gmail.com',
      'venissajean@gmail.com',
      'denise_schwingle@yahoo.com',
      'mega_wat29@hotmail.com',
      'lalatham06@gmail.com',
      'smarolson@yahoo.com',
      'sharris509@gmail.com',
      'inoneaccord16@hotmail.com',
      'amber.shirley2001@gmail.com',
      'jalgaard@yahoo.com',
      'mahaynes26@gmail.com',
      'ermeikle@gmail.com',
      'shereeh@charter.net',
      'mal8635@yahoo.com',
      'crazyshelbykrystle@yahoo.com',
      'catherinedconnolly@gmail.com',
      'jviesmorgan@hotmail.com',
      'jacksonkimm@gmail.com',
      '16suchm@gmail.com'))) AND
      (bigquery_timeupdate.collection  IN ('A Father\'s Choice','A Fine Step',
      'A Gift Horse',
      'A Horse Called Bear',
      'A Horse for Summer',
      'A Very Country Wedding',
      'Amazing Racer',
      'Cowboy Indiana',
      'Forgiven',
      'Grace Unplugged',
      'Healed by Grace',
      'Horse Crazy',
      'Love Finds You in Sugarcreek',
      'Love Finds You in Valentine',
      'Midnight Stallion',
      'Our Wild Hearts',
      'Painted Horses',
      'Pure Country 2: The Gift',
      'Queens of Nashville ',
      'Race to Win',
      'Rodeo Girl',
      'Running Forever',
      'Running Wild',
      'Second Chances',
      'Steps of Faith',
      'The Legend of Longwood',
      'The Princess Stallion',
      'The Wild Stallion',
      'The Winter Stallion',
      'Heart of the Country',
      'Wild Faith') OR collection like '%Heartland%' OR collection like '%Keeping Up with the Kaimanawas%' or collection like '%Morgan Family Strong%' or collection like '%Neon Rider%' or collection like '%Saddle Club%' or collection like '%Wild at Heart%' or (collection='Once Upon a Date' and date(timestamp)='2020-04-17') or (collection='New Life' and date(timestamp)='2020-04-18') or (collection='Barry Brewer: Chicago I\'m Home' AND date(timestamp)='2020-04-19') or (collection='All Good Things' and date(timestamp)='2020-04-21') or (collection='The Furnace' and date(timestamp)='2020-04-23')) AND date(timestamp)>='2020-04-17'
      GROUP BY 1,2,3,4)

      select email,
             user_id,
             sum(points) as points
      from sat0
      group by 1,2
       ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: email {
    type: string
    sql: ${TABLE}.email ;;
  }

  dimension: user_id {
    type: string
    sql: ${TABLE}.user_id ;;
  }

  dimension: points {
    type: number
    sql: ${TABLE}.points ;;
  }

  set: detail {
    fields: [email, user_id, points]
  }
}
