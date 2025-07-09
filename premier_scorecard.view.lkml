view: premier_scorecard {
    derived_table: {
      sql: with

                  play_data_global as
                  (
                  select * from allfirstplay.p0
                  where user_id <> '0'
                  and regexp_contains(user_id, r'^[0-9]*$')
                  and user_id is not null
                  and type = 'movie'
                  and date(timestamp) >= '2023-01-01'
                  ),

                  plays_most_granular as
                  (
                  select
                  user_id
                  , row_number() over (partition by user_id, date(timestamp), video_id order by date(timestamp)) as min_count
                  , timestamp
                  , collection
                  , type
                  , video_id
                  , series
                  , title
                  , source
                  , episode
                  , email
                  , winback
                  from play_data_global
                  order by
                  user_id
                  , date(timestamp)
                  , video_id
                  , min_count
                  ),

                  plays_max_duration as
                  (
                  select
                  user_id
                  , video_id
                  , date(timestamp) as date
                  , max(min_count) as min_count
                  from plays_most_granular
                  group by 1,2,3
                  ),

                  plays_less_granular as
                  (
                  select
                  a.*
                  , row_number() over (partition by a.user_id order by a.timestamp) as play_number
                  from plays_most_granular as a
                  inner join plays_max_duration as b
                  on a.user_id = b.user_id
                  and a.video_id = b.video_id
                  and date(a.timestamp) = b.date
                  and a.min_count = b.min_count
                  ),

                  movie_play_counts as
                  (
                  select
                  collection
                  , min(date(timestamp)) as dt_first_view
                  , count(distinct user_id) as n
                  from plays_less_granular
                  group by 1
                  ),

                  n_day_views AS (
                  select
                  m.collection
                  , m.dt_first_view
                  , count(/*distinct*/ if(p.timestamp between timestamp(m.dt_first_view) and timestamp(m.dt_first_view) + interval 7 day, p.user_id, null)) as views_7_days
                  , count(/*distinct*/ if(p.timestamp between timestamp(m.dt_first_view) and timestamp(m.dt_first_view) + interval 30 day, p.user_id, null)) as views_30_days
                  , count(/*distinct*/ if(p.timestamp between timestamp(m.dt_first_view) and timestamp(m.dt_first_view) + interval 90 day, p.user_id, null)) as views_90_days
                  from movie_play_counts as m
                  inner join plays_less_granular as p
                  on m.collection = p.collection
                  group by 1, 2
                  )

                  select * from n_day_views where collection in
                  (
              "Far Haven",
              "Discovering Love",
              "A Carpenter's Prayer",
              "Grace By Night",
              "Identity Crisis",
              "Festival of Trees",
              "A Bluegrass Christmas",
              "Dial S for Santa",
              "A Novel Christmas",
              "A Country Music Christmas",
              "Jingle Smells",
              "Return to Sender",
              "Love Club Moms: Tory",
              "Love Club Moms: Jo",
              "Love Club Moms: Harper",
              "Love Club Moms: Nila",
              "The Thorn",
              "What We Find on the Road",
              "A Bestselling Kind of Love",
              "Mr. Pawsitively Perfect",
              "The Wedding Contest",
              "Sugar Creek Amish Mysteries",
              "Two Chef's and a Wedding Cake",
              "Heart of a Champion",
              "The Single's Guidebook",
              "The Soulmate Search",
              "Romantic Friction",
              "Sweetly Salted",
              "May the Best Wedding Win",
              "The Wedding Rule",
              "A Match for the Prince",
              "The Love Advisor",
              "Love on Retreat",
              "A Royal Makeover",
              "I Can",
              "Summer at Charlotte's",
              "Fragile Heart",
              "Counter Column",
              "Writing a Love Song",
              "Lucky Louie",
              "Silent Night in Algona",
              "Christmas by the Book",
              "Happy Camper",
              "Luckless in Love",
              "Infamously in Love",
              "Princess and the Bodyguard",
              "Finding Love in Big Sky",
              "The Confession: The Musical",
              "Plus One at an Amish Wedding",
              "When Love Blooms",
              "Star-Crossed Romance",
              "Southern Gospel",
              "Mixed Baggage",
              "Just Jake",
              "Sweet on You",
              "The Wedding Wish",
              "A Royal Christmas Match",
              "An Unperfect Christmas Wish",
              "The Christmas Retreat",
              "Something's Brewing",
              "Lucky Hearts",
              "Love at the Lodge",
              "Romance in Hawaii",
              "Cowboy And Movie Star",
              "Baked with a Kiss",
              "Love by Design",
              "We're Scrooged",
              "Christmas Time Capsule",
              "Yuletide the Knot",
              "A Tiny Home Christmas",
              "Mistletoe Connection",
              "Country Hearts",
              "Christmas at the Amish Bakery",
              "Christmas on the Rocks",
              "The Holiday Swap",
              "A Christmas Masquerade",
              "An Eclectic Christmas",
              "Country Hearts Christmas"


                  ) ;;
    }

    measure: count {
      type: count
      drill_fields: [detail*]
    }

    dimension: collection {
      type: string
      sql: ${TABLE}.collection ;;
    }

    dimension: dt_first_view {
      type: date
      datatype: date
      sql: ${TABLE}.dt_first_view ;;
    }

    dimension: views_7_days {
      type: number
      sql: ${TABLE}.views_7_days ;;
    }

    dimension: views_30_days {
      type: number
      sql: ${TABLE}.views_30_days ;;
    }

    dimension: views_90_days {
      type: number
      sql: ${TABLE}.views_90_days ;;
    }

    set: detail {
      fields: [
        collection,
        dt_first_view,
        views_7_days,
        views_30_days,
        views_90_days
      ]
    }


  # # You can specify the table name if it's different from the view name:
  # sql_table_name: my_schema_name.tester ;;
  #
  # # Define your dimensions and measures here, like this:
  # dimension: user_id {
  #   description: "Unique ID for each user that has ordered"
  #   type: number
  #   sql: ${TABLE}.user_id ;;
  # }
  #
  # dimension: lifetime_orders {
  #   description: "The total number of orders for each user"
  #   type: number
  #   sql: ${TABLE}.lifetime_orders ;;
  # }
  #
  # dimension_group: most_recent_purchase {
  #   description: "The date when each user last ordered"
  #   type: time
  #   timeframes: [date, week, month, year]
  #   sql: ${TABLE}.most_recent_purchase_at ;;
  # }
  #
  # measure: total_lifetime_orders {
  #   description: "Use this for counting lifetime orders across many users"
  #   type: sum
  #   sql: ${lifetime_orders} ;;
  # }
}

# view: premier_scorecard {
#   # Or, you could make this view a derived table, like this:
#   derived_table: {
#     sql: SELECT
#         user_id as user_id
#         , COUNT(*) as lifetime_orders
#         , MAX(orders.created_at) as most_recent_purchase_at
#       FROM orders
#       GROUP BY user_id
#       ;;
#   }
#
#   # Define your dimensions and measures here, like this:
#   dimension: user_id {
#     description: "Unique ID for each user that has ordered"
#     type: number
#     sql: ${TABLE}.user_id ;;
#   }
#
#   dimension: lifetime_orders {
#     description: "The total number of orders for each user"
#     type: number
#     sql: ${TABLE}.lifetime_orders ;;
#   }
#
#   dimension_group: most_recent_purchase {
#     description: "The date when each user last ordered"
#     type: time
#     timeframes: [date, week, month, year]
#     sql: ${TABLE}.most_recent_purchase_at ;;
#   }
#
#   measure: total_lifetime_orders {
#     description: "Use this for counting lifetime orders across many users"
#     type: sum
#     sql: ${lifetime_orders} ;;
#   }
# }
