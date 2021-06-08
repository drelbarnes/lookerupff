- dashboard: svod_monthly_v2_dashboard
  title: SVOD Monthly Dashboard
  layout: newspaper
  embed_style:
    background_color: "#f6f8fa"
    show_title: false
    title_color: "#3a4245"
    show_filters_bar: false
    tile_text_color: "#3a4245"
    text_tile_text_color: ''
  elements:
  - title: Top Series Performance by Season
    name: Top Series Performance by Season
    model: upff_google
    explore: bigquery_titles
    type: table
    fields: [bigquery_titles.franchise, bigquery_titles.season, bigquery_titles.episode_count, bigquery_titles.total_views,
      bigquery_titles.avg_views_per_episode]
    filters:
      bigquery_titles.content_type: Series
      bigquery_titles.platform: "-NULL"
    sorts: [bigquery_titles.total_views desc]
    limit: 500
    show_view_names: false
    show_row_numbers: true
    truncate_column_names: false
    subtotals_at_bottom: false
    hide_totals: false
    hide_row_totals: false
    table_theme: white
    limit_displayed_rows: false
    limit_displayed_rows_values:
      show_hide: show
      first_last: first
      num_rows: '20'
    enable_conditional_formatting: false
    conditional_formatting: [{type: low to high, value: !!null '', background_color: !!null '',
        font_color: !!null '', palette: {name: Red to Yellow to Green, colors: ["#F36254",
            "#FCF758", "#4FBC89"]}, bold: false, italic: false, strikethrough: false,
        fields: [bigquery_titles.total_views]}, {type: low to high, value: !!null '', background_color: !!null '',
        font_color: !!null '', palette: {name: Red to Yellow to Green, colors: ["#F36254",
            "#FCF758", "#4FBC89"]}, bold: false, italic: false, strikethrough: false,
        fields: []}]
    conditional_formatting_include_totals: false
    conditional_formatting_include_nulls: false
    stacking: ''
    show_value_labels: false
    label_density: 25
    legend_position: center
    x_axis_gridlines: false
    y_axis_gridlines: true
    y_axis_combined: true
    show_y_axis_labels: true
    show_y_axis_ticks: true
    y_axis_tick_density: default
    y_axis_tick_density_custom: 5
    show_x_axis_label: true
    show_x_axis_ticks: true
    x_axis_scale: auto
    y_axis_scale_mode: linear
    x_axis_reversed: false
    y_axis_reversed: false
    show_null_points: true
    point_style: none
    interpolation: linear
    show_totals_labels: false
    show_silhouette: false
    totals_color: "#808080"
    color_range: ["#dd3333", "#80ce5d", "#f78131", "#369dc1", "#c572d3", "#36c1b3",
      "#b57052", "#ed69af"]
    ordering: none
    show_null_labels: false
    series_types: {}
    listen:
      Time Period for Views: bigquery_titles.timestamp_month
    row: 38
    col: 0
    width: 12
    height: 11
  - title: Views by Platform
    name: Views by Platform
    model: upff_google
    explore: bigquery_titles
    type: looker_column
    fields: [bigquery_titles.timestamp_month, bigquery_titles.total_views, bigquery_titles.platform_]
    pivots: [bigquery_titles.platform_]
    filters:
      bigquery_titles.platform: "-NULL,-App"
      bigquery_titles.total_views: NOT NULL
    sorts: [bigquery_titles.timestamp_month, bigquery_titles.platform_ desc]
    limit: 500
    trellis: ''
    stacking: normal
    colors: ["#5245ed", "#ed6168", "#1ea8df", "#353b49", "#49cec1", "#b3a0dd", "#db7f2a",
      "#706080", "#a2dcf3", "#776fdf", "#e9b404", "#635189"]
    show_value_labels: true
    label_density: 25
    legend_position: center
    x_axis_gridlines: false
    y_axis_gridlines: true
    show_view_names: false
    point_style: none
    series_colors:
      Vimeo - bigquery_titles.total_views: "#b3a0dd"
      Comcast - bigquery_titles.total_views: "#ed6168"
      Amazon - bigquery_titles.total_views: "#5245ed"
      All Others - bigquery_titles.total_views: "#1ea8df"
    series_labels:
      Comcast SVOD - bigquery_titles.total_views: Comcast
    limit_displayed_rows: false
    y_axis_combined: true
    show_y_axis_labels: true
    show_y_axis_ticks: true
    y_axis_tick_density: default
    y_axis_tick_density_custom: 5
    show_x_axis_label: false
    show_x_axis_ticks: true
    x_axis_datetime_label: "%b' %y"
    x_axis_scale: auto
    y_axis_scale_mode: linear
    x_axis_reversed: false
    y_axis_reversed: false
    plot_size_by_field: false
    ordering: desc
    show_null_labels: false
    show_totals_labels: true
    show_silhouette: false
    totals_color: "#808080"
    listen:
      Time Period for Subs: bigquery_titles.timestamp_month
    row: 4
    col: 12
    width: 12
    height: 10
  - title: Subs by Platforms
    name: Subs by Platforms
    model: upff_google
    explore: bigquery_mvpd_subs
    type: looker_column
    fields: [bigquery_mvpd_subs.date, bigquery_mvpd_subs.amazon_, bigquery_mvpd_subs.comcast_, bigquery_mvpd_subs.d2c_,
      bigquery_mvpd_subs.all_others_]
    sorts: [bigquery_mvpd_subs.date]
    limit: 500
    trellis: ''
    stacking: normal
    colors: ["#5245ed", "#ed6168", "#1ea8df", "#353b49", "#49cec1", "#b3a0dd", "#db7f2a",
      "#706080", "#a2dcf3", "#776fdf", "#e9b404", "#635189"]
    show_value_labels: true
    label_density: 25
    legend_position: center
    x_axis_gridlines: false
    y_axis_gridlines: true
    show_view_names: false
    point_style: none
    series_colors:
      bigquery_mvpd_subs.d2c_: "#b3a0dd"
      bigquery_mvpd_subs.all_others_: "#1ea8df"
    series_labels:
      bigquery_mvpd_subs.d2c_: Vimeo
      bigquery_mvpd_subs.dish_: Dish_sling
    series_types: {}
    limit_displayed_rows: false
    y_axis_combined: true
    show_y_axis_labels: true
    show_y_axis_ticks: true
    y_axis_tick_density: default
    y_axis_tick_density_custom: 5
    show_x_axis_label: false
    show_x_axis_ticks: true
    x_axis_datetime_label: "%b' %y"
    x_axis_scale: auto
    y_axis_scale_mode: linear
    x_axis_reversed: false
    y_axis_reversed: false
    plot_size_by_field: false
    ordering: desc
    show_null_labels: false
    show_totals_labels: true
    show_silhouette: false
    totals_color: "#808080"
    note_state: collapsed
    note_display: above
    note_text: "*Reporting"
    listen:
      Time Period for Subs: bigquery_mvpd_subs.date
    row: 4
    col: 0
    width: 12
    height: 10
  - name: Top Franchises by Platform
    title: Top Franchises by Platform
    model: upff_google
    explore: bigquery_titles
    type: looker_bar
    fields: [bigquery_titles.total_views, bigquery_titles.franchise, bigquery_titles.platform_]
    pivots: [bigquery_titles.platform_]
    filters:
      bigquery_titles.platform: Amazon,Comcast,Cox,Sling,Vimeo
      bigquery_titles.total_views: ">1000"
    sorts: [bigquery_titles.total_views desc 4, bigquery_titles.platform_ 0]
    limit: 5000
    column_limit: 50
    row_total: right
    x_axis_gridlines: false
    y_axis_gridlines: true
    show_view_names: false
    y_axes: [{label: '', orientation: bottom, series: [{id: Amazon - bigquery_titles.total_views,
            name: Amazon, axisId: bigquery_titles.total_views}, {id: Comcast - bigquery_titles.total_views,
            name: Comcast, axisId: bigquery_titles.total_views}, {id: Cox - bigquery_titles.total_views,
            name: Cox, axisId: bigquery_titles.total_views}, {id: Sling - bigquery_titles.total_views,
            name: Sling, axisId: bigquery_titles.total_views}, {id: Vimeo - bigquery_titles.total_views,
            name: Vimeo, axisId: bigquery_titles.total_views}], showLabels: true, showValues: true,
        minValue: !!null '', unpinAxis: false, tickDensity: custom, tickDensityCustom: '4',
        type: linear}]
    show_y_axis_labels: true
    show_y_axis_ticks: true
    y_axis_tick_density: default
    y_axis_tick_density_custom: 5
    show_x_axis_label: true
    show_x_axis_ticks: true
    y_axis_scale_mode: linear
    x_axis_reversed: false
    y_axis_reversed: false
    plot_size_by_field: false
    trellis: ''
    stacking: normal
    limit_displayed_rows: true
    limit_displayed_rows_values:
      show_hide: show
      first_last: first
      num_rows: '10'
    legend_position: center
    colors: ["#5245ed", "#ed6168", "#1ea8df", "#353b49", "#49cec1", "#b3a0dd", "#db7f2a",
      "#706080", "#a2dcf3", "#776fdf", "#e9b404", "#635189"]
    series_types: {}
    point_style: circle
    series_colors:
      Sling - bigquery_titles.total_views: "#49cec1"
      Vimeo - bigquery_titles.total_views: "#b3a0dd"
      Amazon - bigquery_titles.total_views: "#5245ed"
      Comcast - bigquery_titles.total_views: "#ed6168"
      All Others - bigquery_titles.total_views: "#1ea8df"
    series_labels:
      Comcast SVOD - bigquery_titles.total_views: Comcast
    show_value_labels: true
    label_density: 25
    x_axis_scale: auto
    y_axis_combined: true
    x_axis_datetime_tick_count: 2
    ordering: desc
    show_null_labels: false
    show_totals_labels: true
    show_silhouette: false
    totals_color: "#808080"
    show_null_points: true
    interpolation: linear
    color_range: ["#dd3333", "#80ce5d", "#f78131", "#369dc1", "#c572d3", "#36c1b3",
      "#b57052", "#ed69af"]
    listen:
      Time Period for Views: bigquery_titles.timestamp_month
    row: 14
    col: 0
    width: 12
    height: 9
  - title: Views by Content Type
    name: Views by Content Type
    model: upff_google
    explore: bigquery_titles
    type: looker_column
    fields: [bigquery_titles.timestamp_month, bigquery_titles.content_type, bigquery_titles.total_views]
    pivots: [bigquery_titles.content_type]
    filters:
      bigquery_titles.content_type: Movie,Series
      bigquery_titles.total_views: NOT NULL
    sorts: [bigquery_titles.timestamp_month desc, bigquery_titles.content_type 0]
    limit: 500
    stacking: percent
    show_value_labels: true
    label_density: 25
    legend_position: center
    x_axis_gridlines: false
    y_axis_gridlines: true
    show_view_names: true
    point_style: none
    limit_displayed_rows: false
    y_axis_combined: true
    show_y_axis_labels: true
    show_y_axis_ticks: true
    y_axis_tick_density: default
    y_axis_tick_density_custom: 5
    show_x_axis_label: false
    show_x_axis_ticks: true
    x_axis_scale: auto
    y_axis_scale_mode: linear
    x_axis_reversed: false
    y_axis_reversed: false
    ordering: desc
    show_null_labels: false
    show_totals_labels: false
    show_silhouette: false
    totals_color: "#808080"
    colors: ['palette: Santa Cruz']
    series_colors: {}
    y_axes: [{label: '', orientation: left, series: [{id: Movie - bigquery_titles.total_views,
            name: Movie, axisId: bigquery_titles.total_views}, {id: Series - bigquery_titles.total_views,
            name: Series, axisId: bigquery_titles.total_views}], showLabels: false, showValues: true,
        unpinAxis: false, tickDensity: default, tickDensityCustom: 5, type: linear}]
    series_types: {}
    listen:
      Time Period for Subs: bigquery_titles.timestamp_month
    row: 14
    col: 12
    width: 12
    height: 9
  - title: Top Movie Views
    name: Top Movie Views
    model: upff_google
    explore: bigquery_titles
    type: table
    fields: [bigquery_titles.up_title, bigquery_titles.total_views]
    filters:
      bigquery_titles.content_type: Movie
    sorts: [bigquery_titles.total_views desc]
    limit: 500
    show_view_names: false
    show_row_numbers: true
    truncate_column_names: false
    hide_totals: false
    hide_row_totals: false
    table_theme: white
    limit_displayed_rows: false
    enable_conditional_formatting: false
    conditional_formatting_include_totals: false
    conditional_formatting_include_nulls: false
    series_labels:
      bigquery_titles.total_views: Views
    limit_displayed_rows_values:
      show_hide: show
      first_last: first
      num_rows: '10'
    conditional_formatting: [{type: low to high, value: !!null '', background_color: !!null '',
        font_color: !!null '', palette: {name: Red to Yellow to Green, colors: ["#F36254",
            "#FCF758", "#4FBC89"]}, bold: false, italic: false, strikethrough: false,
        fields: !!null ''}]
    stacking: ''
    show_value_labels: false
    label_density: 25
    legend_position: center
    x_axis_gridlines: false
    y_axis_gridlines: true
    point_style: none
    y_axis_combined: true
    show_y_axis_labels: true
    show_y_axis_ticks: true
    y_axis_tick_density: default
    y_axis_tick_density_custom: 5
    show_x_axis_label: true
    show_x_axis_ticks: true
    x_axis_scale: auto
    y_axis_scale_mode: linear
    x_axis_reversed: false
    y_axis_reversed: false
    ordering: none
    show_null_labels: false
    show_totals_labels: false
    show_silhouette: false
    totals_color: "#808080"
    font_size: 12
    value_labels: legend
    label_type: labPer
    show_null_points: true
    series_types: {}
    defaults_version: 1
    listen:
      Time Period for Views: bigquery_titles.timestamp_month
    row: 38
    col: 12
    width: 6
    height: 11
  - title: Top Series Views
    name: Top Series Views
    model: upff_google
    explore: bigquery_titles
    type: table
    fields: [bigquery_titles.up_title, bigquery_titles.total_views]
    filters:
      bigquery_titles.content_type: Series
    sorts: [bigquery_titles.total_views desc]
    limit: 500
    show_view_names: false
    show_row_numbers: true
    truncate_column_names: false
    hide_totals: false
    hide_row_totals: false
    series_labels:
      bigquery_titles.total_views: Views
    table_theme: white
    limit_displayed_rows: false
    limit_displayed_rows_values:
      show_hide: show
      first_last: first
      num_rows: '10'
    enable_conditional_formatting: false
    conditional_formatting_include_totals: false
    conditional_formatting_include_nulls: false
    stacking: ''
    show_value_labels: false
    label_density: 25
    legend_position: center
    x_axis_gridlines: false
    y_axis_gridlines: true
    point_style: none
    y_axis_combined: true
    show_y_axis_labels: true
    show_y_axis_ticks: true
    y_axis_tick_density: default
    y_axis_tick_density_custom: 5
    show_x_axis_label: true
    show_x_axis_ticks: true
    x_axis_scale: auto
    y_axis_scale_mode: linear
    x_axis_reversed: false
    y_axis_reversed: false
    ordering: none
    show_null_labels: false
    show_totals_labels: false
    show_silhouette: false
    totals_color: "#808080"
    font_size: 12
    value_labels: legend
    label_type: labPer
    show_null_points: true
    series_types: {}
    listen:
      Time Period for Views: bigquery_titles.timestamp_month
    row: 38
    col: 18
    width: 6
    height: 11
  - title: Looker
    name: Looker
    model: upff_google
    explore: bigquery_titles
    type: single_value
    fields: [bigquery_titles.timestamp_month]
    fill_fields: [bigquery_titles.timestamp_month]
    sorts: [bigquery_titles.timestamp_month desc]
    limit: 500
    custom_color_enabled: false
    custom_color: forestgreen
    show_single_value_title: true
    single_value_title: Reporting Month
    show_comparison: false
    comparison_type: value
    comparison_reverse_colors: false
    show_comparison_label: true
    stacking: ''
    show_value_labels: false
    label_density: 25
    legend_position: center
    x_axis_gridlines: false
    y_axis_gridlines: true
    show_view_names: true
    point_style: none
    limit_displayed_rows: false
    y_axis_combined: true
    show_y_axis_labels: true
    show_y_axis_ticks: true
    y_axis_tick_density: default
    y_axis_tick_density_custom: 5
    show_x_axis_label: true
    show_x_axis_ticks: true
    x_axis_scale: auto
    y_axis_scale_mode: linear
    x_axis_reversed: false
    y_axis_reversed: false
    plot_size_by_field: false
    ordering: none
    show_null_labels: false
    show_totals_labels: false
    show_silhouette: false
    totals_color: "#808080"
    series_types: {}
    listen:
      Time Period for Subs: bigquery_titles.timestamp_month
    row: 0
    col: 0
    width: 24
    height: 2
  - title: Reporting Month Total Subs
    name: Reporting Month Total Subs
    model: upff_google
    explore: bigquery_mvpd_subs
    type: single_value
    fields: [bigquery_mvpd_subs.comcast_, bigquery_mvpd_subs.amazon_, bigquery_mvpd_subs.all_others_, bigquery_mvpd_subs.d2c_]
    limit: 500
    dynamic_fields: [{table_calculation: total_subs, label: Total Subs, expression: "${bigquery_mvpd_subs.comcast_}+${bigquery_mvpd_subs.amazon_}+${bigquery_mvpd_subs.all_others_}+${bigquery_mvpd_subs.d2c_}",
        value_format: !!null '', value_format_name: decimal_0, _kind_hint: measure,
        _type_hint: number}]
    hidden_fields: [bigquery_mvpd_subs.all_others_, bigquery_mvpd_subs.comcast_, bigquery_mvpd_subs.amazon_,
      bigquery_mvpd_subs.d2c_]
    listen:
      Time Period for Views: bigquery_mvpd_subs.date
    row: 2
    col: 0
    width: 12
    height: 2
  - title: Reporting Month Total Views
    name: Reporting Month Total Views
    model: upff_google
    explore: bigquery_titles
    type: single_value
    fields: [bigquery_titles.total_views]
    limit: 500
    listen:
      Time Period for Views: bigquery_titles.date
    row: 2
    col: 12
    width: 12
    height: 2
  - title: Top Bigquery_titles by Platform
    name: Top Bigquery_titles by Platform
    model: upff_google
    explore: bigquery_titles
    type: table
    fields: [bigquery_titles.up_title, bigquery_titles.total_views, bigquery_titles.platform_]
    pivots: [bigquery_titles.platform_]
    sorts: [bigquery_titles.total_views desc 5, bigquery_titles.platform_ desc 0]
    row_total: right
    dynamic_fields: [{table_calculation: platform_index, label: Platform Index, expression: '100*${platform_total}/${avg_share}',
        value_format: !!null '', value_format_name: decimal_0, is_disabled: false,
        _kind_hint: measure, _type_hint: number}, {table_calculation: platform_total,
        label: Platform Total, expression: "${bigquery_titles.total_views}/sum(${bigquery_titles.total_views})",
        value_format: !!null '', value_format_name: !!null '', is_disabled: false,
        _kind_hint: measure, _type_hint: number}, {table_calculation: avg_share, label: Avg
          Share, expression: "(pivot_index(if(is_null(${platform_total}),0,${platform_total}),1)+pivot_index(if(is_null(${platform_total}),0,${platform_total}),2)+pivot_index(if(is_null(${platform_total}),0,${platform_total}),3)+pivot_index(if(is_null(${platform_total}),0,${platform_total}),4))/4 ",
        value_format: !!null '', value_format_name: !!null '', is_disabled: false,
        _kind_hint: supermeasure, _type_hint: number}]
    query_timezone: America/New_York
    show_view_names: false
    show_row_numbers: true
    truncate_column_names: false
    hide_totals: false
    hide_row_totals: false
    table_theme: white
    limit_displayed_rows: false
    enable_conditional_formatting: true
    conditional_formatting_include_totals: true
    conditional_formatting_include_nulls: true
    series_labels:
      bigquery_titles.total_views: Views
    limit_displayed_rows_values:
      show_hide: show
      first_last: first
      num_rows: '20'
    conditional_formatting: [{type: along a scale..., value: !!null '', background_color: "#62bad4",
        font_color: !!null '', color_application: {collection_id: legacy, palette_id: legacy_diverging1,
          options: {steps: 5, constraints: {min: {type: minimum}, mid: {type: number,
                value: 100}, max: {type: maximum}}, mirror: true, reverse: false,
            stepped: false}}, bold: false, italic: false, strikethrough: false, fields: [
          platform_index]}, {type: along a scale..., value: !!null '', background_color: "#62bad4",
        font_color: !!null '', color_application: {collection_id: legacy, palette_id: legacy_diverging1,
          options: {steps: 5, constraints: {min: {type: minimum}, mid: {type: average},
              max: {type: maximum}}, mirror: true, reverse: false, stepped: false}},
        bold: false, italic: false, strikethrough: false, fields: [bigquery_titles.total_views]}]
    color_range: ["#dd3333", "#80ce5d", "#f78131", "#369dc1", "#c572d3", "#36c1b3",
      "#b57052", "#ed69af"]
    bins: 5
    stacking: ''
    show_value_labels: false
    label_density: 25
    legend_position: center
    x_axis_gridlines: false
    y_axis_gridlines: true
    y_axis_combined: true
    show_y_axis_labels: true
    show_y_axis_ticks: true
    y_axis_tick_density: default
    y_axis_tick_density_custom: 5
    show_x_axis_label: true
    show_x_axis_ticks: true
    x_axis_scale: auto
    y_axis_scale_mode: linear
    x_axis_reversed: false
    y_axis_reversed: false
    ordering: none
    show_null_labels: false
    show_totals_labels: false
    show_silhouette: false
    totals_color: "#808080"
    series_types: {}
    defaults_version: 1
    hidden_fields: [platform_total, avg_share]
    listen:
      Time Period for Views: bigquery_titles.timestamp_month
    row: 23
    col: 0
    width: 24
    height: 15
  filters:
  - name: Time Period for Subs
    title: Time Period for Subs
    type: date_filter
    default_value: after 1 years ago
    allow_multiple_values: true
    required: false
  - name: Time Period for Views
    title: Time Period for Views
    type: date_filter
    default_value: after 2 months ago
    allow_multiple_values: true
    required: false
