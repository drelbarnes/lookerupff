- dashboard: churn_model_performance
  title: Churn Model Performance
  layout: newspaper
  elements:
  - title: Loss Curve
    name: Loss Curve
    model: upff_google
    explore: churn_model_training_info
    type: looker_area
    fields:
    - churn_model_training_info.loss
    - churn_model_training_info.iteration
    sorts:
    - churn_model_training_info.iteration
    limit: 500
    query_timezone: America/New_York
    stacking: ''
    show_value_labels: false
    label_density: 25
    legend_position: center
    x_axis_gridlines: false
    y_axis_gridlines: true
    show_view_names: false
    point_style: circle_outline
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
    show_null_points: true
    interpolation: monotone
    show_totals_labels: false
    show_silhouette: false
    totals_color: "#808080"
    series_types: {}
    listen: {}
    row: 27
    col: 14
    width: 10
    height: 6
  - title: Total Training Time (sec)
    name: Total Training Time (sec)
    model: upff_google
    explore: churn_model_training_info
    type: single_value
    fields:
    - churn_model_training_info.total_training_time
    limit: 500
    query_timezone: America/New_York
    custom_color_enabled: false
    custom_color: forestgreen
    show_single_value_title: true
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
    show_null_points: true
    interpolation: linear
    show_totals_labels: false
    show_silhouette: false
    totals_color: "#808080"
    series_types: {}
    listen: {}
    row: 27
    col: 0
    width: 6
    height: 2
  - title: Average Iteration Duration (sec)
    name: Average Iteration Duration (sec)
    model: upff_google
    explore: churn_model_training_info
    type: single_value
    fields:
    - churn_model_training_info.average_iteration_time
    limit: 500
    query_timezone: America/New_York
    custom_color_enabled: false
    custom_color: forestgreen
    show_single_value_title: true
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
    show_null_points: true
    interpolation: linear
    show_totals_labels: false
    show_silhouette: false
    totals_color: "#808080"
    series_types: {}
    listen: {}
    row: 31
    col: 0
    width: 6
    height: 2
  - title: Training Details
    name: Training Details
    model: upff_google
    explore: churn_model_training_info
    type: table
    fields:
    - churn_model_training_info.iteration
    - churn_model_training_info.duration_ms
    - churn_model_training_info.learning_rate
    - churn_model_training_info.eval_loss
    sorts:
    - churn_model_training_info.iteration
    limit: 500
    query_timezone: America/New_York
    show_view_names: false
    show_row_numbers: false
    truncate_column_names: true
    hide_totals: false
    hide_row_totals: false
    table_theme: gray
    limit_displayed_rows: false
    enable_conditional_formatting: false
    conditional_formatting_include_totals: false
    conditional_formatting_include_nulls: false
    custom_color_enabled: false
    custom_color: forestgreen
    show_single_value_title: true
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
    show_null_points: true
    interpolation: linear
    show_totals_labels: false
    show_silhouette: false
    totals_color: "#808080"
    series_types: {}
    listen: {}
    row: 27
    col: 6
    width: 8
    height: 6
  - title: Accuracy (static)
    name: Accuracy (static)
    model: upff_google
    explore: churn_model_evaluation
    type: single_value
    fields:
    - churn_model_evaluation.accuracy
    sorts:
    - churn_model_evaluation.accuracy
    limit: 500
    query_timezone: America/New_York
    custom_color_enabled: false
    custom_color: forestgreen
    show_single_value_title: true
    show_comparison: false
    comparison_type: value
    comparison_reverse_colors: false
    show_comparison_label: true
    listen: {}
    row: 0
    col: 0
    width: 6
    height: 2
  - title: Recall (static)
    name: Recall (static)
    model: upff_google
    explore: churn_model_evaluation
    type: single_value
    fields:
    - churn_model_evaluation.recall
    sorts:
    - churn_model_evaluation.recall
    limit: 500
    query_timezone: America/New_York
    custom_color_enabled: false
    custom_color: forestgreen
    show_single_value_title: true
    show_comparison: false
    comparison_type: value
    comparison_reverse_colors: false
    show_comparison_label: true
    listen: {}
    row: 0
    col: 6
    width: 6
    height: 2
  - title: Precision-Recall Curve
    name: Precision-Recall Curve
    model: upff_google
    explore: churn_roc_curve
    type: looker_line
    fields:
    - churn_roc_curve.precision
    - churn_roc_curve.recall
    sorts:
    - churn_roc_curve.precision
    limit: 500
    query_timezone: America/New_York
    stacking: ''
    show_value_labels: false
    label_density: 25
    legend_position: center
    x_axis_gridlines: false
    y_axis_gridlines: true
    show_view_names: false
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
    show_null_points: true
    interpolation: monotone
    series_types: {}
    y_axes:
    - label: ''
      orientation: left
      series:
      - id: churn_roc_curve.precision
        name: Precision
        axisId: churn_roc_curve.precision
        __FILE: upff/model_performance.dashboard.lookml
        __LINE_NUM: 287
      showLabels: true
      showValues: true
      unpinAxis: false
      tickDensity: default
      tickDensityCustom: 5
      type: linear
      __FILE: upff/model_performance.dashboard.lookml
      __LINE_NUM: 284
    x_axis_datetime_label: ''
    hide_legend: false
    listen: {}
    row: 4
    col: 12
    width: 12
    height: 8
  - title: F1 Score (static)
    name: F1 Score (static)
    model: upff_google
    explore: churn_model_evaluation
    type: single_value
    fields:
    - churn_model_evaluation.f1_score
    sorts:
    - churn_model_evaluation.f1_score
    limit: 500
    query_timezone: America/New_York
    custom_color_enabled: false
    custom_color: forestgreen
    show_single_value_title: true
    show_comparison: false
    comparison_type: value
    comparison_reverse_colors: false
    show_comparison_label: true
    listen: {}
    row: 2
    col: 6
    width: 6
    height: 2
  - title: Total Iterations
    name: Total Iterations
    model: upff_google
    explore: churn_model_training_info
    type: single_value
    fields:
    - churn_model_training_info.total_iterations
    limit: 500
    query_timezone: America/New_York
    custom_color_enabled: false
    custom_color: forestgreen
    show_single_value_title: true
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
    show_null_points: true
    interpolation: linear
    show_totals_labels: false
    show_silhouette: false
    totals_color: "#808080"
    series_types: {}
    listen: {}
    row: 29
    col: 0
    width: 6
    height: 2
  - name: Training Metrics
    type: text
    title_text: Training Metrics
    subtitle_text: ___________________________________________________________________
    row: 25
    col: 0
    width: 24
    height: 2
  - title: ROC Curve
    name: ROC Curve
    model: upff_google
    explore: churn_roc_curve
    type: looker_line
    fields:
    - churn_roc_curve.false_positives
    - churn_roc_curve.false_negatives
    - churn_roc_curve.true_negatives
    - churn_roc_curve.total_true_positives
    sorts:
    - random_tpr desc
    limit: 500
    column_limit: 50
    dynamic_fields:
    - table_calculation: tpr
      label: TPR
      expression: "${churn_roc_curve.total_true_positives}/(${churn_roc_curve.total_true_positives}\
        \ + ${churn_roc_curve.false_negatives})"
      value_format:
      value_format_name:
      _kind_hint: measure
      _type_hint: number
    - table_calculation: _
      label: "-"
      expression: max(${churn_roc_curve.total_true_positives})*(row()/max(row()))
      value_format:
      value_format_name:
      _kind_hint: measure
      _type_hint: number
    - table_calculation: fpr
      label: FPR
      expression: "${churn_roc_curve.false_positives}/ (${churn_roc_curve.false_positives} + ${churn_roc_curve.true_negatives})"
      value_format:
      value_format_name:
      _kind_hint: dimension
      _type_hint: number
    - table_calculation: random_tpr
      label: Random TPR
      expression: "${fpr}+(0*${churn_roc_curve.total_true_positives})"
      value_format:
      value_format_name:
      _kind_hint: measure
      _type_hint: number
    stacking: ''
    show_value_labels: false
    label_density: 25
    legend_position: center
    hide_legend: true
    x_axis_gridlines: true
    y_axis_gridlines: true
    show_view_names: false
    point_style: none
    series_colors:
      _: "#d5d7db"
      random_tpr: "#B1B0B0"
    series_types: {}
    series_point_styles:
      random_tpr: diamond
    limit_displayed_rows: false
    y_axes:
    - label: ''
      orientation: left
      series:
      - id: churn_roc_curve.total_true_positives
        name: Total True Positives
        axisId: churn_roc_curve.total_true_positives
        __FILE: upff/model_performance.dashboard.lookml
        __LINE_NUM: 871
      - id: _
        name: "-"
        axisId: _
        __FILE: upff/model_performance.dashboard.lookml
        __LINE_NUM: 874
      showLabels: true
      showValues: true
      unpinAxis: false
      tickDensity: default
      tickDensityCustom: 5
      type: linear
      __FILE: upff/model_performance.dashboard.lookml
      __LINE_NUM: 868
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
    reference_lines: []
    trend_lines: []
    show_null_points: true
    interpolation: monotone
    ordering: none
    show_null_labels: false
    show_totals_labels: false
    show_silhouette: false
    totals_color: "#808080"
    hidden_fields:
    - churn_roc_curve.true_negatives
    - churn_roc_curve.false_negatives
    - churn_roc_curve.false_positives
    - churn_roc_curve.total_true_positives
    - _
    listen: {}
    row: 4
    col: 0
    width: 12
    height: 8
  - title: True Positives
    name: True Positives
    model: upff_google
    explore: churn_confusion_matrix
    type: single_value
    fields:
    - churn_confusion_matrix._1
    - churn_confusion_matrix.expected_label
    sorts:
    - churn_confusion_matrix._1 desc
    limit: 500
    custom_color_enabled: true
    custom_color: forestgreen
    show_single_value_title: true
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
    listen: {}
    row: 0
    col: 12
    width: 6
    height: 2
  - title: False Positives
    name: False Positives
    model: upff_google
    explore: churn_confusion_matrix
    type: single_value
    fields:
    - churn_confusion_matrix._1
    - churn_confusion_matrix.expected_label
    sorts:
    - churn_confusion_matrix._1
    limit: 500
    custom_color_enabled: true
    custom_color: "#e31a1c"
    show_single_value_title: true
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
    row: 0
    col: 18
    width: 6
    height: 2
  - title: True Negatives
    name: True Negatives
    model: upff_google
    explore: churn_confusion_matrix
    type: single_value
    fields:
    - churn_confusion_matrix._0
    - churn_confusion_matrix.expected_label
    sorts:
    - churn_confusion_matrix._0 desc
    limit: 500
    custom_color_enabled: true
    custom_color: forestgreen
    show_single_value_title: true
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
    row: 2
    col: 18
    width: 6
    height: 2
  - title: False Negatives
    name: False Negatives
    model: upff_google
    explore: churn_confusion_matrix
    type: single_value
    fields:
    - churn_confusion_matrix._0
    - churn_confusion_matrix.expected_label
    sorts:
    - churn.confusion_matrix._0
    limit: 500
    custom_color_enabled: true
    custom_color: "#e31a1c"
    show_single_value_title: true
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
    row: 2
    col: 12
    width: 6
    height: 2
  - title: ROC Area Under the Curve
    name: ROC Area Under the Curve
    model: upff_google
    explore: churn_model_evaluation
    type: single_value
    fields:
    - churn_model_evaluation.roc_auc
    sorts:
    - churn_model_evaluation.roc_auc
    limit: 500
    custom_color_enabled: false
    custom_color: forestgreen
    show_single_value_title: true
    value_format: 0.00%
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
    listen: {}
    row: 2
    col: 0
    width: 6
    height: 2
  - title: Numerical Input Weights
    name: Numerical Input Weights
    model: upff_google
    explore: churn_weights
    type: looker_bar
    fields:
    - churn_weights.weight
    - churn_weights.processed_input
    filters:
      churn_weights.weight: NOT NULL
    sorts:
    - churn_weights.weight desc
    limit: 500
    show_view_names: false
    show_row_numbers: true
    truncate_column_names: false
    subtotals_at_bottom: false
    hide_totals: false
    hide_row_totals: false
    table_theme: white
    limit_displayed_rows: false
    enable_conditional_formatting: true
    conditional_formatting:
    - type: low to high
      value:
      background_color:
      font_color:
      palette:
        name: Red to Yellow to Green
        colors:
        - "#F36254"
        - "#FCF758"
        - "#4FBC89"
      bold: false
      italic: false
      strikethrough: false
      fields:
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
    plot_size_by_field: false
    ordering: none
    show_null_labels: false
    show_totals_labels: false
    show_silhouette: false
    totals_color: "#808080"
    series_types: {}
    row: 12
    col: 0
    width: 12
    height: 13
  - title: Categorical Input Weights
    name: Categorical Input Weights
    model: upff_google
    explore: churn_cat_weights
    type: looker_bar
    fields:
    - churn_cat_weights.catweight
    - churn_cat_weights.cat
    sorts:
    - churn_cat_weights.catweight desc
    limit: 500
    stacking: ''
    show_value_labels: false
    label_density: 25
    legend_position: center
    x_axis_gridlines: false
    y_axis_gridlines: true
    show_view_names: false
    point_style: none
    series_types: {}
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
    listen: {}
    row: 12
    col: 12
    width: 12
    height: 13
  - title: Average Propensity Score
    name: Average Propensity Score
    model: upff_google
    explore: churn_prediction
    type: single_value
    fields:
    - churn_prediction.average_predicted_score
    limit: 500
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
    row: 35
    col: 16
    width: 8
    height: 2
  - title: Churn Propensity Scores
    name: Churn Propensity Scores
    model: upff_google
    explore: churn_prediction
    type: table
    fields:
    - churn_prediction.customer_id
    - churn_prediction.predicted_churn_status_probability
    sorts:
    - churn_prediction.predicted_get_status_probability desc
    limit: 500
    show_view_names: false
    show_row_numbers: true
    truncate_column_names: false
    subtotals_at_bottom: false
    hide_totals: false
    hide_row_totals: false
    table_theme: editable
    limit_displayed_rows: false
    enable_conditional_formatting: true
    conditional_formatting:
    - type: low to high
      value:
      background_color:
      font_color:
      palette:
        name: Red to Yellow to Green
        colors:
        - "#F36254"
        - "#FCF758"
        - "#4FBC89"
      bold: false
      italic: false
      strikethrough: false
      fields:
    conditional_formatting_include_totals: false
    conditional_formatting_include_nulls: true
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
    plot_size_by_field: false
    ordering: none
    show_null_labels: false
    show_totals_labels: false
    show_silhouette: false
    totals_color: "#808080"
    series_types: {}
    listen: {}
    row: 37
    col: 16
    width: 8
    height: 12
  - title: Propensity Score Distribution
    name: Propensity Score Distribution
    model: upff_google
    explore: churn_prediction
    type: histogram
    fields:
    - churn_prediction.predicted_churn_status_probability
    sorts:
    - churn_prediction.predicted_churn_status_probability
    limit: 500
    color_range:
    - "#dd3333"
    - "#80ce5d"
    - "#f78131"
    - "#369dc1"
    - "#c572d3"
    - "#36c1b3"
    - "#b57052"
    - "#ed69af"
    bins: 10
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
    row: 35
    col: 0
    width: 16
    height: 14
  - name: Propensity Scoring
    type: text
    title_text: Propensity Scoring
    row: 33
    col: 0
    width: 24
    height: 2
  filters:
  - name: Error Matrix Threshold
    title: Error Matrix Threshold
    type: number_filter
    default_value: ">=0.08"
    allow_multiple_values: true
    required: true
