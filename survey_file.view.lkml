view: survey_file {
  sql_table_name: svod_titles.survey_file ;;

  dimension: customer_id {
    type: number
    sql: ${TABLE}.customer_id ;;
  }

  dimension_group: date {
    type: time
    timeframes: [
      raw,
      time,
      date,
      week,
      month,
      quarter,
      year
    ]
    sql: ${TABLE}.date ;;
  }

  dimension: dcua {
    type: string
    sql: ${TABLE}.dcua ;;
  }

  dimension: dec_lang {
    type: string
    sql: ${TABLE}.decLang ;;
  }

  dimension: h_clus_sumc1 {
    type: number
    sql: ${TABLE}.hClusSumc1 ;;
  }

  dimension: h_clus_sumc2 {
    type: number
    sql: ${TABLE}.hClusSumc2 ;;
  }

  dimension: h_clus_sumc3 {
    type: number
    sql: ${TABLE}.hClusSumc3 ;;
  }

  dimension: h_clus_sumc4 {
    type: number
    sql: ${TABLE}.hClusSumc4 ;;
  }

  dimension: h_clusr10c1 {
    type: number
    sql: ${TABLE}.hClusr10c1 ;;
  }

  dimension: h_clusr10c2 {
    type: number
    sql: ${TABLE}.hClusr10c2 ;;
  }

  dimension: h_clusr10c3 {
    type: number
    sql: ${TABLE}.hClusr10c3 ;;
  }

  dimension: h_clusr10c4 {
    type: number
    sql: ${TABLE}.hClusr10c4 ;;
  }

  dimension: h_clusr11c1 {
    type: number
    sql: ${TABLE}.hClusr11c1 ;;
  }

  dimension: h_clusr11c2 {
    type: number
    sql: ${TABLE}.hClusr11c2 ;;
  }

  dimension: h_clusr11c3 {
    type: number
    sql: ${TABLE}.hClusr11c3 ;;
  }

  dimension: h_clusr11c4 {
    type: number
    sql: ${TABLE}.hClusr11c4 ;;
  }

  dimension: h_clusr12c1 {
    type: number
    sql: ${TABLE}.hClusr12c1 ;;
  }

  dimension: h_clusr12c2 {
    type: number
    sql: ${TABLE}.hClusr12c2 ;;
  }

  dimension: h_clusr12c3 {
    type: number
    sql: ${TABLE}.hClusr12c3 ;;
  }

  dimension: h_clusr12c4 {
    type: number
    sql: ${TABLE}.hClusr12c4 ;;
  }

  dimension: h_clusr13c1 {
    type: number
    sql: ${TABLE}.hClusr13c1 ;;
  }

  dimension: h_clusr13c2 {
    type: number
    sql: ${TABLE}.hClusr13c2 ;;
  }

  dimension: h_clusr13c3 {
    type: number
    sql: ${TABLE}.hClusr13c3 ;;
  }

  dimension: h_clusr13c4 {
    type: number
    sql: ${TABLE}.hClusr13c4 ;;
  }

  dimension: h_clusr1c1 {
    type: number
    sql: ${TABLE}.hClusr1c1 ;;
  }

  dimension: h_clusr1c2 {
    type: number
    sql: ${TABLE}.hClusr1c2 ;;
  }

  dimension: h_clusr1c3 {
    type: number
    sql: ${TABLE}.hClusr1c3 ;;
  }

  dimension: h_clusr1c4 {
    type: number
    sql: ${TABLE}.hClusr1c4 ;;
  }

  dimension: h_clusr2c1 {
    type: number
    sql: ${TABLE}.hClusr2c1 ;;
  }

  dimension: h_clusr2c2 {
    type: number
    sql: ${TABLE}.hClusr2c2 ;;
  }

  dimension: h_clusr2c3 {
    type: number
    sql: ${TABLE}.hClusr2c3 ;;
  }

  dimension: h_clusr2c4 {
    type: number
    sql: ${TABLE}.hClusr2c4 ;;
  }

  dimension: h_clusr3c1 {
    type: number
    sql: ${TABLE}.hClusr3c1 ;;
  }

  dimension: h_clusr3c2 {
    type: number
    sql: ${TABLE}.hClusr3c2 ;;
  }

  dimension: h_clusr3c3 {
    type: number
    sql: ${TABLE}.hClusr3c3 ;;
  }

  dimension: h_clusr3c4 {
    type: number
    sql: ${TABLE}.hClusr3c4 ;;
  }

  dimension: h_clusr4c1 {
    type: number
    sql: ${TABLE}.hClusr4c1 ;;
  }

  dimension: h_clusr4c2 {
    type: number
    sql: ${TABLE}.hClusr4c2 ;;
  }

  dimension: h_clusr4c3 {
    type: number
    sql: ${TABLE}.hClusr4c3 ;;
  }

  dimension: h_clusr4c4 {
    type: number
    sql: ${TABLE}.hClusr4c4 ;;
  }

  dimension: h_clusr5c1 {
    type: number
    sql: ${TABLE}.hClusr5c1 ;;
  }

  dimension: h_clusr5c2 {
    type: number
    sql: ${TABLE}.hClusr5c2 ;;
  }

  dimension: h_clusr5c3 {
    type: number
    sql: ${TABLE}.hClusr5c3 ;;
  }

  dimension: h_clusr5c4 {
    type: number
    sql: ${TABLE}.hClusr5c4 ;;
  }

  dimension: h_clusr6c1 {
    type: number
    sql: ${TABLE}.hClusr6c1 ;;
  }

  dimension: h_clusr6c2 {
    type: number
    sql: ${TABLE}.hClusr6c2 ;;
  }

  dimension: h_clusr6c3 {
    type: number
    sql: ${TABLE}.hClusr6c3 ;;
  }

  dimension: h_clusr6c4 {
    type: number
    sql: ${TABLE}.hClusr6c4 ;;
  }

  dimension: h_clusr7c1 {
    type: number
    sql: ${TABLE}.hClusr7c1 ;;
  }

  dimension: h_clusr7c2 {
    type: number
    sql: ${TABLE}.hClusr7c2 ;;
  }

  dimension: h_clusr7c3 {
    type: number
    sql: ${TABLE}.hClusr7c3 ;;
  }

  dimension: h_clusr7c4 {
    type: number
    sql: ${TABLE}.hClusr7c4 ;;
  }

  dimension: h_clusr8c1 {
    type: number
    sql: ${TABLE}.hClusr8c1 ;;
  }

  dimension: h_clusr8c2 {
    type: number
    sql: ${TABLE}.hClusr8c2 ;;
  }

  dimension: h_clusr8c3 {
    type: number
    sql: ${TABLE}.hClusr8c3 ;;
  }

  dimension: h_clusr8c4 {
    type: number
    sql: ${TABLE}.hClusr8c4 ;;
  }

  dimension: h_clusr9c1 {
    type: number
    sql: ${TABLE}.hClusr9c1 ;;
  }

  dimension: h_clusr9c2 {
    type: number
    sql: ${TABLE}.hClusr9c2 ;;
  }

  dimension: h_clusr9c3 {
    type: number
    sql: ${TABLE}.hClusr9c3 ;;
  }

  dimension: h_clusr9c4 {
    type: number
    sql: ${TABLE}.hClusr9c4 ;;
  }

  dimension: h_rsvalsr1 {
    type: number
    sql: ${TABLE}.hRSValsr1 ;;
  }

  dimension: h_rsvalsr10 {
    type: number
    sql: ${TABLE}.hRSValsr10 ;;
  }

  dimension: h_rsvalsr11 {
    type: number
    sql: ${TABLE}.hRSValsr11 ;;
  }

  dimension: h_rsvalsr12 {
    type: number
    sql: ${TABLE}.hRSValsr12 ;;
  }

  dimension: h_rsvalsr13 {
    type: number
    sql: ${TABLE}.hRSValsr13 ;;
  }

  dimension: h_rsvalsr2 {
    type: number
    sql: ${TABLE}.hRSValsr2 ;;
  }

  dimension: h_rsvalsr3 {
    type: number
    sql: ${TABLE}.hRSValsr3 ;;
  }

  dimension: h_rsvalsr4 {
    type: number
    sql: ${TABLE}.hRSValsr4 ;;
  }

  dimension: h_rsvalsr5 {
    type: number
    sql: ${TABLE}.hRSValsr5 ;;
  }

  dimension: h_rsvalsr6 {
    type: number
    sql: ${TABLE}.hRSValsr6 ;;
  }

  dimension: h_rsvalsr7 {
    type: number
    sql: ${TABLE}.hRSValsr7 ;;
  }

  dimension: h_rsvalsr8 {
    type: number
    sql: ${TABLE}.hRSValsr8 ;;
  }

  dimension: h_rsvalsr9 {
    type: number
    sql: ${TABLE}.hRSValsr9 ;;
  }

  dimension: h_segment {
    type: number
    sql: ${TABLE}.hSegment ;;
  }

  dimension: h_segment_final {
    type: number
    sql: ${TABLE}.hSegmentFinal ;;
  }

  dimension: hidd_flag_q2r1 {
    type: number
    sql: ${TABLE}.HiddFlagQ2r1 ;;
  }

  dimension: hidd_flag_q3r1 {
    type: number
    sql: ${TABLE}.HiddFlagQ3r1 ;;
  }

  dimension: list {
    type: number
    sql: ${TABLE}.list ;;
  }

  dimension: markers {
    type: string
    sql: ${TABLE}.markers ;;
  }

  dimension: noanswer_sweepstakes_na1 {
    type: number
    sql: ${TABLE}.noanswerSWEEPSTAKES_na1 ;;
  }

  dimension: q1 {
    type: number
    sql: ${TABLE}.Q1 ;;
  }

  dimension: q10r_a {
    type: number
    sql: ${TABLE}.Q10rA ;;
  }

  dimension: q10r_b {
    type: number
    sql: ${TABLE}.Q10rB ;;
  }

  dimension: q10r_c {
    type: number
    sql: ${TABLE}.Q10rC ;;
  }

  dimension: q10r_d {
    type: number
    sql: ${TABLE}.Q10rD ;;
  }

  dimension: q10r_e {
    type: number
    sql: ${TABLE}.Q10rE ;;
  }

  dimension: q10r_f {
    type: number
    sql: ${TABLE}.Q10rF ;;
  }

  dimension: q10r_g {
    type: number
    sql: ${TABLE}.Q10rG ;;
  }

  dimension: q10r_h {
    type: number
    sql: ${TABLE}.Q10rH ;;
  }

  dimension: q10r_i {
    type: number
    sql: ${TABLE}.Q10rI ;;
  }

  dimension: q10r_j {
    type: number
    sql: ${TABLE}.Q10rJ ;;
  }

  dimension: q10r_k {
    type: number
    sql: ${TABLE}.Q10rK ;;
  }

  dimension: q10r_l {
    type: number
    sql: ${TABLE}.Q10rL ;;
  }

  dimension: q10r_m {
    type: number
    sql: ${TABLE}.Q10rM ;;
  }

  dimension: q10r_n {
    type: number
    sql: ${TABLE}.Q10rN ;;
  }

  dimension: q10r_o {
    type: number
    sql: ${TABLE}.Q10rO ;;
  }

  dimension: q10r_p {
    type: number
    sql: ${TABLE}.Q10rP ;;
  }

  dimension: q10r_q {
    type: number
    sql: ${TABLE}.Q10rQ ;;
  }

  dimension: q10r_r {
    type: number
    sql: ${TABLE}.Q10rR ;;
  }

  dimension: q10r_s {
    type: number
    sql: ${TABLE}.Q10rS ;;
  }

  dimension: q11_dr1 {
    type: number
    sql: ${TABLE}.Q11Dr1 ;;
  }

  dimension: q11_dr11 {
    type: number
    sql: ${TABLE}.Q11Dr11 ;;
  }

  dimension: q11_dr2 {
    type: number
    sql: ${TABLE}.Q11Dr2 ;;
  }

  dimension: q11_dr5 {
    type: number
    sql: ${TABLE}.Q11Dr5 ;;
  }

  dimension: q11_dr8 {
    type: number
    sql: ${TABLE}.Q11Dr8 ;;
  }

  dimension: q11_dr9 {
    type: number
    sql: ${TABLE}.Q11Dr9 ;;
  }

  dimension: q11_mr1 {
    type: number
    sql: ${TABLE}.Q11Mr1 ;;
  }

  dimension: q11_mr11 {
    type: number
    sql: ${TABLE}.Q11Mr11 ;;
  }

  dimension: q11_mr2 {
    type: number
    sql: ${TABLE}.Q11Mr2 ;;
  }

  dimension: q11_mr5 {
    type: number
    sql: ${TABLE}.Q11Mr5 ;;
  }

  dimension: q11_mr8 {
    type: number
    sql: ${TABLE}.Q11Mr8 ;;
  }

  dimension: q11_mr9 {
    type: number
    sql: ${TABLE}.Q11Mr9 ;;
  }

  dimension: q11r1 {
    type: number
    sql: ${TABLE}.Q11r1 ;;
  }

  dimension: q11r11 {
    type: number
    sql: ${TABLE}.Q11r11 ;;
  }

  dimension: q11r2 {
    type: number
    sql: ${TABLE}.Q11r2 ;;
  }

  dimension: q11r5 {
    type: number
    sql: ${TABLE}.Q11r5 ;;
  }

  dimension: q11r8 {
    type: number
    sql: ${TABLE}.Q11r8 ;;
  }

  dimension: q11r9 {
    type: number
    sql: ${TABLE}.Q11r9 ;;
  }

  dimension: q12_dr1 {
    type: number
    sql: ${TABLE}.Q12Dr1 ;;
  }

  dimension: q12_dr10 {
    type: number
    sql: ${TABLE}.Q12Dr10 ;;
  }

  dimension: q12_dr2 {
    type: number
    sql: ${TABLE}.Q12Dr2 ;;
  }

  dimension: q12_dr4 {
    type: number
    sql: ${TABLE}.Q12Dr4 ;;
  }

  dimension: q12_dr5 {
    type: number
    sql: ${TABLE}.Q12Dr5 ;;
  }

  dimension: q12_dr7 {
    type: number
    sql: ${TABLE}.Q12Dr7 ;;
  }

  dimension: q12_dr9 {
    type: number
    sql: ${TABLE}.Q12Dr9 ;;
  }

  dimension: q12_mr1 {
    type: number
    sql: ${TABLE}.Q12Mr1 ;;
  }

  dimension: q12_mr10 {
    type: number
    sql: ${TABLE}.Q12Mr10 ;;
  }

  dimension: q12_mr2 {
    type: number
    sql: ${TABLE}.Q12Mr2 ;;
  }

  dimension: q12_mr4 {
    type: number
    sql: ${TABLE}.Q12Mr4 ;;
  }

  dimension: q12_mr5 {
    type: number
    sql: ${TABLE}.Q12Mr5 ;;
  }

  dimension: q12_mr7 {
    type: number
    sql: ${TABLE}.Q12Mr7 ;;
  }

  dimension: q12_mr9 {
    type: number
    sql: ${TABLE}.Q12Mr9 ;;
  }

  dimension: q12r1 {
    type: number
    sql: ${TABLE}.Q12r1 ;;
  }

  dimension: q12r10 {
    type: number
    sql: ${TABLE}.Q12r10 ;;
  }

  dimension: q12r2 {
    type: number
    sql: ${TABLE}.Q12r2 ;;
  }

  dimension: q12r4 {
    type: number
    sql: ${TABLE}.Q12r4 ;;
  }

  dimension: q12r5 {
    type: number
    sql: ${TABLE}.Q12r5 ;;
  }

  dimension: q12r7 {
    type: number
    sql: ${TABLE}.Q12r7 ;;
  }

  dimension: q12r9 {
    type: number
    sql: ${TABLE}.Q12r9 ;;
  }

  dimension: q13r1 {
    type: number
    sql: ${TABLE}.Q13r1 ;;
  }

  dimension: q13r2 {
    type: number
    sql: ${TABLE}.Q13r2 ;;
  }

  dimension: q13r3 {
    type: number
    sql: ${TABLE}.Q13r3 ;;
  }

  dimension: q13r4 {
    type: number
    sql: ${TABLE}.Q13r4 ;;
  }

  dimension: q14r1 {
    type: number
    sql: ${TABLE}.Q14r1 ;;
  }

  dimension: q14r2 {
    type: number
    sql: ${TABLE}.Q14r2 ;;
  }

  dimension: q14r3 {
    type: number
    sql: ${TABLE}.Q14r3 ;;
  }

  dimension: q14r4 {
    type: number
    sql: ${TABLE}.Q14r4 ;;
  }

  dimension: q14r5 {
    type: number
    sql: ${TABLE}.Q14r5 ;;
  }

  dimension: q15r_a {
    type: number
    sql: ${TABLE}.Q15rA ;;
  }

  dimension: q15r_b {
    type: number
    sql: ${TABLE}.Q15rB ;;
  }

  dimension: q15r_c {
    type: number
    sql: ${TABLE}.Q15rC ;;
  }

  dimension: q15r_d {
    type: number
    sql: ${TABLE}.Q15rD ;;
  }

  dimension: q16 {
    type: number
    sql: ${TABLE}.Q16 ;;
  }

  dimension: q17r_a {
    type: number
    sql: ${TABLE}.Q17rA ;;
  }

  dimension: q17r_b {
    type: number
    sql: ${TABLE}.Q17rB ;;
  }

  dimension: q17r_c {
    type: number
    sql: ${TABLE}.Q17rC ;;
  }

  dimension: q17r_d {
    type: number
    sql: ${TABLE}.Q17rD ;;
  }

  dimension: q17r_e {
    type: number
    sql: ${TABLE}.Q17rE ;;
  }

  dimension: q17r_f {
    type: number
    sql: ${TABLE}.Q17rF ;;
  }

  dimension: q17r_g {
    type: number
    sql: ${TABLE}.Q17rG ;;
  }

  dimension: q17r_h {
    type: number
    sql: ${TABLE}.Q17rH ;;
  }

  dimension: q17r_i {
    type: number
    sql: ${TABLE}.Q17rI ;;
  }

  dimension: q17r_j {
    type: number
    sql: ${TABLE}.Q17rJ ;;
  }

  dimension: q18 {
    type: string
    sql: ${TABLE}.Q18 ;;
  }

  dimension: q19r_a {
    type: number
    sql: ${TABLE}.Q19rA ;;
  }

  dimension: q19r_b {
    type: number
    sql: ${TABLE}.Q19rB ;;
  }

  dimension: q20r_a {
    type: number
    sql: ${TABLE}.Q20rA ;;
  }

  dimension: q20r_b {
    type: number
    sql: ${TABLE}.Q20rB ;;
  }

  dimension: q20r_c {
    type: number
    sql: ${TABLE}.Q20rC ;;
  }

  dimension: q20r_d {
    type: number
    sql: ${TABLE}.Q20rD ;;
  }

  dimension: q20r_e {
    type: number
    sql: ${TABLE}.Q20rE ;;
  }

  dimension: q20r_f {
    type: number
    sql: ${TABLE}.Q20rF ;;
  }

  dimension: q20r_g {
    type: number
    sql: ${TABLE}.Q20rG ;;
  }

  dimension: q20r_h {
    type: number
    sql: ${TABLE}.Q20rH ;;
  }

  dimension: q20r_i {
    type: number
    sql: ${TABLE}.Q20rI ;;
  }

  dimension: q20r_j {
    type: number
    sql: ${TABLE}.Q20rJ ;;
  }

  dimension: q21r_a {
    type: number
    sql: ${TABLE}.Q21rA ;;
  }

  dimension: q21r_b {
    type: number
    sql: ${TABLE}.Q21rB ;;
  }

  dimension: q21r_c {
    type: number
    sql: ${TABLE}.Q21rC ;;
  }

  dimension: q21r_d {
    type: number
    sql: ${TABLE}.Q21rD ;;
  }

  dimension: q21r_e {
    type: number
    sql: ${TABLE}.Q21rE ;;
  }

  dimension: q21r_f {
    type: number
    sql: ${TABLE}.Q21rF ;;
  }

  dimension: q21r_g {
    type: number
    sql: ${TABLE}.Q21rG ;;
  }

  dimension: q21r_h {
    type: number
    sql: ${TABLE}.Q21rH ;;
  }

  dimension: q21r_i {
    type: number
    sql: ${TABLE}.Q21rI ;;
  }

  dimension: q21r_j {
    type: number
    sql: ${TABLE}.Q21rJ ;;
  }

  dimension: q21r_k {
    type: number
    sql: ${TABLE}.Q21rK ;;
  }

  dimension: q22r_a {
    type: number
    sql: ${TABLE}.Q22rA ;;
  }

  dimension: q22r_b {
    type: number
    sql: ${TABLE}.Q22rB ;;
  }

  dimension: q22r_c {
    type: number
    sql: ${TABLE}.Q22rC ;;
  }

  dimension: q22r_d {
    type: number
    sql: ${TABLE}.Q22rD ;;
  }

  dimension: q22r_e {
    type: number
    sql: ${TABLE}.Q22rE ;;
  }

  dimension: q22r_f {
    type: number
    sql: ${TABLE}.Q22rF ;;
  }

  dimension: q22r_g {
    type: number
    sql: ${TABLE}.Q22rG ;;
  }

  dimension: q22r_h {
    type: number
    sql: ${TABLE}.Q22rH ;;
  }

  dimension: q22r_i {
    type: number
    sql: ${TABLE}.Q22rI ;;
  }

  dimension: q22r_j {
    type: number
    sql: ${TABLE}.Q22rJ ;;
  }

  dimension: q22r_k {
    type: number
    sql: ${TABLE}.Q22rK ;;
  }

  dimension: q22r_l {
    type: number
    sql: ${TABLE}.Q22rL ;;
  }

  dimension: q22r_m {
    type: number
    sql: ${TABLE}.Q22rM ;;
  }

  dimension: q23r_a {
    type: number
    sql: ${TABLE}.Q23rA ;;
  }

  dimension: q23r_b {
    type: number
    sql: ${TABLE}.Q23rB ;;
  }

  dimension: q23r_c {
    type: number
    sql: ${TABLE}.Q23rC ;;
  }

  dimension: q23r_d {
    type: number
    sql: ${TABLE}.Q23rD ;;
  }

  dimension: q23r_e {
    type: number
    sql: ${TABLE}.Q23rE ;;
  }

  dimension: q23r_f {
    type: number
    sql: ${TABLE}.Q23rF ;;
  }

  dimension: q23r_g {
    type: number
    sql: ${TABLE}.Q23rG ;;
  }

  dimension: q23r_goe {
    type: string
    sql: ${TABLE}.Q23rGoe ;;
  }

  dimension: q24r_a {
    type: number
    sql: ${TABLE}.Q24rA ;;
  }

  dimension: q24r_b {
    type: number
    sql: ${TABLE}.Q24rB ;;
  }

  dimension: q24r_c {
    type: number
    sql: ${TABLE}.Q24rC ;;
  }

  dimension: q24r_d {
    type: number
    sql: ${TABLE}.Q24rD ;;
  }

  dimension: q24r_e {
    type: number
    sql: ${TABLE}.Q24rE ;;
  }

  dimension: q24r_f {
    type: number
    sql: ${TABLE}.Q24rF ;;
  }

  dimension: q24r_g {
    type: number
    sql: ${TABLE}.Q24rG ;;
  }

  dimension: q24r_h {
    type: number
    sql: ${TABLE}.Q24rH ;;
  }

  dimension: q24r_i {
    type: number
    sql: ${TABLE}.Q24rI ;;
  }

  dimension: q24r_j {
    type: number
    sql: ${TABLE}.Q24rJ ;;
  }

  dimension: q24r_k {
    type: number
    sql: ${TABLE}.Q24rK ;;
  }

  dimension: q24r_l {
    type: number
    sql: ${TABLE}.Q24rL ;;
  }

  dimension: q24r_loe {
    type: string
    sql: ${TABLE}.Q24rLoe ;;
  }

  dimension: q25r_a {
    type: number
    sql: ${TABLE}.Q25rA ;;
  }

  dimension: q25r_b {
    type: number
    sql: ${TABLE}.Q25rB ;;
  }

  dimension: q25r_c {
    type: number
    sql: ${TABLE}.Q25rC ;;
  }

  dimension: q25r_d {
    type: number
    sql: ${TABLE}.Q25rD ;;
  }

  dimension: q26_piper1_l {
    type: number
    sql: ${TABLE}.Q26_piper1_L ;;
  }

  dimension: q26_piper1_r {
    type: number
    sql: ${TABLE}.Q26_piper1_R ;;
  }

  dimension: q26_piper2_l {
    type: number
    sql: ${TABLE}.Q26_piper2_L ;;
  }

  dimension: q26_piper2_r {
    type: number
    sql: ${TABLE}.Q26_piper2_R ;;
  }

  dimension: q26_piper3_l {
    type: number
    sql: ${TABLE}.Q26_piper3_L ;;
  }

  dimension: q26_piper3_r {
    type: number
    sql: ${TABLE}.Q26_piper3_R ;;
  }

  dimension: q26r1 {
    type: number
    sql: ${TABLE}.Q26r1 ;;
  }

  dimension: q26r2 {
    type: number
    sql: ${TABLE}.Q26r2 ;;
  }

  dimension: q26r3 {
    type: number
    sql: ${TABLE}.Q26r3 ;;
  }

  dimension: q27 {
    type: number
    sql: ${TABLE}.Q27 ;;
  }

  dimension: q28 {
    type: number
    sql: ${TABLE}.Q28 ;;
  }

  dimension: q29 {
    type: string
    sql: ${TABLE}.Q29 ;;
  }

  dimension: q2r_a {
    type: number
    sql: ${TABLE}.Q2rA ;;
  }

  dimension: q2r_b {
    type: number
    sql: ${TABLE}.Q2rB ;;
  }

  dimension: q2r_c {
    type: number
    sql: ${TABLE}.Q2rC ;;
  }

  dimension: q2r_d {
    type: number
    sql: ${TABLE}.Q2rD ;;
  }

  dimension: q2r_e {
    type: number
    sql: ${TABLE}.Q2rE ;;
  }

  dimension: q2r_f {
    type: number
    sql: ${TABLE}.Q2rF ;;
  }

  dimension: q2r_g {
    type: number
    sql: ${TABLE}.Q2rG ;;
  }

  dimension: q2r_h {
    type: number
    sql: ${TABLE}.Q2rH ;;
  }

  dimension: q2r_i {
    type: number
    sql: ${TABLE}.Q2rI ;;
  }

  dimension: q2r_j {
    type: number
    sql: ${TABLE}.Q2rJ ;;
  }

  dimension: q2r_k {
    type: number
    sql: ${TABLE}.Q2rK ;;
  }

  dimension: q2r_koe {
    type: string
    sql: ${TABLE}.Q2rKoe ;;
  }

  dimension: q2r_l {
    type: number
    sql: ${TABLE}.Q2rL ;;
  }

  dimension: q30r_a {
    type: number
    sql: ${TABLE}.Q30rA ;;
  }

  dimension: q30r_b {
    type: number
    sql: ${TABLE}.Q30rB ;;
  }

  dimension: q30r_c {
    type: number
    sql: ${TABLE}.Q30rC ;;
  }

  dimension: q30r_d {
    type: number
    sql: ${TABLE}.Q30rD ;;
  }

  dimension: q31 {
    type: number
    sql: ${TABLE}.Q31 ;;
  }

  dimension: q32 {
    type: number
    sql: ${TABLE}.Q32 ;;
  }

  dimension: q33_flagr1 {
    type: number
    sql: ${TABLE}.Q33_flagr1 ;;
  }

  dimension: q33_flagr2 {
    type: number
    sql: ${TABLE}.Q33_flagr2 ;;
  }

  dimension: q33r_a {
    type: number
    sql: ${TABLE}.Q33rA ;;
  }

  dimension: q33r_b {
    type: number
    sql: ${TABLE}.Q33rB ;;
  }

  dimension: q33r_c {
    type: number
    sql: ${TABLE}.Q33rC ;;
  }

  dimension: q33r_d {
    type: number
    sql: ${TABLE}.Q33rD ;;
  }

  dimension: q33r_e {
    type: number
    sql: ${TABLE}.Q33rE ;;
  }

  dimension: q33r_f {
    type: number
    sql: ${TABLE}.Q33rF ;;
  }

  dimension: q34 {
    type: number
    sql: ${TABLE}.Q34 ;;
  }

  dimension: q35 {
    type: number
    sql: ${TABLE}.Q35 ;;
  }

  dimension: q36 {
    type: number
    sql: ${TABLE}.Q36 ;;
  }

  dimension: q37r1 {
    type: number
    sql: ${TABLE}.Q37r1 ;;
  }

  dimension: q37r2 {
    type: number
    sql: ${TABLE}.Q37r2 ;;
  }

  dimension: q37r3 {
    type: number
    sql: ${TABLE}.Q37r3 ;;
  }

  dimension: q37r4 {
    type: number
    sql: ${TABLE}.Q37r4 ;;
  }

  dimension: q37r5 {
    type: number
    sql: ${TABLE}.Q37r5 ;;
  }

  dimension: q38 {
    type: number
    sql: ${TABLE}.Q38 ;;
  }

  dimension: q39 {
    type: number
    sql: ${TABLE}.Q39 ;;
  }

  dimension: q3r_a {
    type: number
    sql: ${TABLE}.Q3rA ;;
  }

  dimension: q3r_b {
    type: number
    sql: ${TABLE}.Q3rB ;;
  }

  dimension: q3r_c {
    type: number
    sql: ${TABLE}.Q3rC ;;
  }

  dimension: q3r_d {
    type: number
    sql: ${TABLE}.Q3rD ;;
  }

  dimension: q3r_e {
    type: number
    sql: ${TABLE}.Q3rE ;;
  }

  dimension: q3r_f {
    type: number
    sql: ${TABLE}.Q3rF ;;
  }

  dimension: q3r_g {
    type: number
    sql: ${TABLE}.Q3rG ;;
  }

  dimension: q3r_h {
    type: number
    sql: ${TABLE}.Q3rH ;;
  }

  dimension: q3r_i {
    type: number
    sql: ${TABLE}.Q3rI ;;
  }

  dimension: q3r_j {
    type: number
    sql: ${TABLE}.Q3rJ ;;
  }

  dimension: q3r_k {
    type: number
    sql: ${TABLE}.Q3rK ;;
  }

  dimension: q3r_l {
    type: number
    sql: ${TABLE}.Q3rL ;;
  }

  dimension: q3r_loe {
    type: string
    sql: ${TABLE}.Q3rLoe ;;
  }

  dimension: q4 {
    type: number
    sql: ${TABLE}.Q4 ;;
  }

  dimension: q40 {
    type: number
    sql: ${TABLE}.Q40 ;;
  }

  dimension: q5_lr1 {
    type: number
    sql: ${TABLE}.Q5_Lr1 ;;
  }

  dimension: q5_lr10 {
    type: string
    sql: ${TABLE}.Q5_Lr10 ;;
  }

  dimension: q5_lr11 {
    type: number
    sql: ${TABLE}.Q5_Lr11 ;;
  }

  dimension: q5_lr12 {
    type: number
    sql: ${TABLE}.Q5_Lr12 ;;
  }

  dimension: q5_lr2 {
    type: number
    sql: ${TABLE}.Q5_Lr2 ;;
  }

  dimension: q5_lr3 {
    type: number
    sql: ${TABLE}.Q5_Lr3 ;;
  }

  dimension: q5_lr4 {
    type: number
    sql: ${TABLE}.Q5_Lr4 ;;
  }

  dimension: q5_lr5 {
    type: number
    sql: ${TABLE}.Q5_Lr5 ;;
  }

  dimension: q5_lr6 {
    type: number
    sql: ${TABLE}.Q5_Lr6 ;;
  }

  dimension: q5_lr7 {
    type: number
    sql: ${TABLE}.Q5_Lr7 ;;
  }

  dimension: q5_lr8 {
    type: number
    sql: ${TABLE}.Q5_Lr8 ;;
  }

  dimension: q5_lr9 {
    type: number
    sql: ${TABLE}.Q5_Lr9 ;;
  }

  dimension: q6 {
    type: string
    sql: ${TABLE}.Q6 ;;
  }

  dimension: q7 {
    type: string
    sql: ${TABLE}.Q7 ;;
  }

  dimension: q8 {
    type: number
    sql: ${TABLE}.Q8 ;;
  }

  dimension: q9 {
    type: number
    sql: ${TABLE}.Q9 ;;
  }

  dimension: qtime {
    type: number
    sql: ${TABLE}.qtime ;;
  }

  dimension: record {
    type: number
    sql: ${TABLE}.record ;;
  }

  dimension: session {
    type: string
    sql: ${TABLE}.session ;;
  }

  dimension: sl_q10r1 {
    type: number
    sql: ${TABLE}.SL_Q10r1 ;;
  }

  dimension: sl_q20r1 {
    type: number
    sql: ${TABLE}.SL_Q20r1 ;;
  }

  dimension: source {
    type: string
    sql: ${TABLE}.source ;;
  }

  dimension: speeder {
    type: number
    sql: ${TABLE}.speeder ;;
  }

  dimension_group: start {
    type: time
    timeframes: [
      raw,
      time,
      date,
      week,
      month,
      quarter,
      year
    ]
    sql: ${TABLE}.start_date ;;
  }

  dimension: status {
    type: number
    sql: ${TABLE}.status ;;
  }

  dimension: sweepstakesr1 {
    type: string
    sql: ${TABLE}.SWEEPSTAKESR1 ;;
  }

  dimension: sweepstakesr2 {
    type: string
    sql: ${TABLE}.SWEEPSTAKESR2 ;;
  }

  dimension: sweepstakesterm {
    type: string
    sql: ${TABLE}.SWEEPSTAKESTERM ;;
  }

  dimension: url {
    type: string
    sql: ${TABLE}.url ;;
  }

  dimension: user_agent {
    type: string
    sql: ${TABLE}.userAgent ;;
  }

  dimension: uuid {
    type: string
    sql: ${TABLE}.uuid ;;
  }

  dimension: vbrowser {
    type: number
    sql: ${TABLE}.vbrowser ;;
  }

  dimension: vbrowserr15oe {
    type: string
    sql: ${TABLE}.vbrowserr15oe ;;
  }

  dimension: vdropout {
    type: string
    sql: ${TABLE}.vdropout ;;
  }

  dimension: vlist {
    type: number
    sql: ${TABLE}.vlist ;;
  }

  dimension: vmobiledevice {
    type: number
    sql: ${TABLE}.vmobiledevice ;;
  }

  dimension: vmobileos {
    type: number
    sql: ${TABLE}.vmobileos ;;
  }

  dimension: vos {
    type: number
    sql: ${TABLE}.vos ;;
  }

  dimension: vosr15oe {
    type: string
    sql: ${TABLE}.vosr15oe ;;
  }

  measure: count {
    type: count
    drill_fields: []
  }
}
