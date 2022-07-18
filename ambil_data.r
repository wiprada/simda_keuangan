require(tidyverse)
require(DBI)
require(odbc)
require(fs)
require(glue)
require(here)

nama_server <- "ND-JD8NNF2\\SQLEXPRESS"
nama_db <- "bangli_audited"

kon <-
  DBI::dbConnect(
    odbc::odbc(),
    Driver = "SQL Server Native Client 11.0",
    Server = nama_server,
    Database = nama_db,
    trusted_connection = "yes",
    port = 0
  )



tarik_saldo_nrc_aset <- function(kon) {
  pemda <- tbl(kon, "ta_pemda") %>%
    arrange(-tahun) %>%
    head(1) %>%
    collect()

  ta <- pemda %>%
    select(tahun) %>%
    pull()

  entitas <- pemda %>%
    select(nm_pemda) %>%
    pull() %>%
    word(start = 2, end = -1) %>%
    str_to_lower()

  jrn_nrc <- tbl(kon, "ta_jurnalsemuaak")
  jrn_nrc_r <- tbl(kon, "ta_jurnalsemuaak_rinc")
  xjrn <- intersect(colnames(jrn_nrc), colnames(jrn_nrc_r))
  jrn_nrc_i <-
    left_join(jrn_nrc, jrn_nrc_r, xjrn) %>%
    filter(tahun == ta) %>%
    filter(kd_rek90_1 == 1) %>%
    filter(kd_jurnal != 8) %>%
    group_by(
      kd_urusan,
      kd_bidang,
      kd_unit,
      kd_rek90_1,
      kd_rek90_2,
      kd_rek90_3,
      kd_rek90_4,
      kd_rek90_5,
      kd_rek90_6
    ) %>%
    summarise(
      debet = sum(debet, na.rm = TRUE),
      kredit = sum(kredit, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    collect()

  sal_awl <-
    tbl(kon, "ta_saldo_awalak") %>%
    filter(tahun == ta - 1) %>%
    filter(kd_rek90_1 == 1)

  sal_a <-
    sal_awl %>%
    group_by(
      kd_urusan,
      kd_bidang,
      kd_unit,
      kd_rek90_1,
      kd_rek90_2,
      kd_rek90_3,
      kd_rek90_4,
      kd_rek90_5,
      kd_rek90_6,
      d_k
    ) %>%
    summarise(
      saldo = sum(saldo, na.rm = TRUE),
      .groups = "drop"
    )

  sal_b <-
    sal_a %>%
    collect() %>%
    mutate(saldo = if_else(d_k == "D", saldo * 1, saldo * -1)) %>%
    group_by(
      kd_urusan,
      kd_bidang,
      kd_unit,
      kd_rek90_1,
      kd_rek90_2,
      kd_rek90_3,
      kd_rek90_4,
      kd_rek90_5,
      kd_rek90_6
    ) %>%
    summarise(
      saldo = sum(saldo, na.rm = TRUE),
      .groups = "drop"
    )

  xsal <- intersect(colnames(jrn_nrc_i), colnames(sal_b))

  aset <-
    full_join(jrn_nrc_i, sal_b, xsal) %>%
    mutate(
      saldo = replace_na(saldo, 0),
      debet = replace_na(debet, 0),
      kredit = replace_na(kredit, 0)
    ) %>%
    mutate(
      saldo_akhir = saldo + debet - kredit
    ) %>%
    relocate(
      saldo,
      .before = debet
    )


  # menambahkan referensi akun rek90
  rek1 <- tbl(kon, "ref_rek90_1")
  rek2 <- tbl(kon, "ref_rek90_2")
  rek3 <- tbl(kon, "ref_rek90_3")
  rek4 <- tbl(kon, "ref_rek90_4")
  rek5 <- tbl(kon, "ref_rek90_5")
  rek6 <- tbl(kon, "ref_rek90_6")

  rek <- rek6 %>%
    left_join(rek5, by = c(
      "kd_rek90_1",
      "kd_rek90_2",
      "kd_rek90_3",
      "kd_rek90_4",
      "kd_rek90_5"
    )) %>%
    left_join(rek4, by = c(
      "kd_rek90_1",
      "kd_rek90_2",
      "kd_rek90_3",
      "kd_rek90_4"
    )) %>%
    left_join(rek3, by = c(
      "kd_rek90_1",
      "kd_rek90_2",
      "kd_rek90_3"
    )) %>%
    left_join(rek2, by = c(
      "kd_rek90_1",
      "kd_rek90_2"
    )) %>%
    left_join(rek1, by = c("kd_rek90_1"))

  rek <- rek %>%
    select(
      kd_rek90_1, kd_rek90_2, kd_rek90_3,
      kd_rek90_4, kd_rek90_5, kd_rek90_6,
      nm_rek90_1, nm_rek90_2, nm_rek90_3,
      nm_rek90_4, nm_rek90_5, nm_rek90_6,
      saldonorm
    ) %>%
    collect()

  xrek <- intersect(colnames(aset), colnames(rek))

  unit <- tbl(kon, "ref_unit") %>% collect()
  xunit <- intersect(colnames(aset), colnames(unit))

  aset_1 <-
    left_join(aset, rek, xrek)

  aset_2 <-
    left_join(aset_1, unit, xunit)

  aset_3 <-
    aset_2 %>%
    select(
      kd_unit90,
      nm_unit,
      kd_rek90_1,
      kd_rek90_2,
      kd_rek90_3,
      kd_rek90_4,
      kd_rek90_5,
      kd_rek90_6,
      nm_rek90_1,
      nm_rek90_2,
      nm_rek90_3,
      nm_rek90_4,
      nm_rek90_5,
      nm_rek90_6,
      saldonorm,
      saldo_thn_lalu = saldo,
      debet,
      kredit,
      saldo_akhir
    )
  return(aset_3)
}

tarik_saldo_nrc_kewajiban <- function(kon) {
  pemda <- tbl(kon, "ta_pemda") %>%
    arrange(-tahun) %>%
    head(1) %>%
    collect()

  ta <- pemda %>%
    select(tahun) %>%
    pull()

  entitas <- pemda %>%
    select(nm_pemda) %>%
    pull() %>%
    word(start = 2, end = -1) %>%
    str_to_lower()

  jrn_nrc <- tbl(kon, "ta_jurnalsemuaak")
  jrn_nrc_r <- tbl(kon, "ta_jurnalsemuaak_rinc")
  xjrn <- intersect(colnames(jrn_nrc), colnames(jrn_nrc_r))
  jrn_nrc_i <-
    left_join(jrn_nrc, jrn_nrc_r, xjrn) %>%
    filter(tahun == ta) %>%
    filter(kd_rek90_1 == 2) %>%
    filter(kd_jurnal != 8) %>%
    group_by(
      kd_urusan,
      kd_bidang,
      kd_unit,
      kd_rek90_1,
      kd_rek90_2,
      kd_rek90_3,
      kd_rek90_4,
      kd_rek90_5,
      kd_rek90_6
    ) %>%
    summarise(
      debet = sum(debet, na.rm = TRUE),
      kredit = sum(kredit, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    collect()

  sal_awl <-
    tbl(kon, "ta_saldo_awalak") %>%
    filter(tahun == ta - 1) %>%
    filter(kd_rek90_1 == 2)

  sal_a <-
    sal_awl %>%
    group_by(
      kd_urusan,
      kd_bidang,
      kd_unit,
      kd_rek90_1,
      kd_rek90_2,
      kd_rek90_3,
      kd_rek90_4,
      kd_rek90_5,
      kd_rek90_6,
      d_k
    ) %>%
    summarise(
      saldo = sum(saldo, na.rm = TRUE),
      .groups = "drop"
    )

  sal_b <-
    sal_a %>%
    collect() %>%
    mutate(saldo = if_else(d_k == "K", saldo * 1, saldo * -1)) %>%
    group_by(
      kd_urusan,
      kd_bidang,
      kd_unit,
      kd_rek90_1,
      kd_rek90_2,
      kd_rek90_3,
      kd_rek90_4,
      kd_rek90_5,
      kd_rek90_6
    ) %>%
    summarise(
      saldo = sum(saldo, na.rm = TRUE),
      .groups = "drop"
    )

  xsal <- intersect(colnames(jrn_nrc_i), colnames(sal_b))

  kewajiban <-
    full_join(jrn_nrc_i, sal_b, xsal) %>%
    mutate(
      saldo = replace_na(saldo, 0),
      debet = replace_na(debet, 0),
      kredit = replace_na(kredit, 0)
    ) %>%
    mutate(
      saldo_akhir = saldo - debet + kredit
    ) %>%
    relocate(
      saldo,
      .before = debet
    )


  # menambahkan referensi akun rek90
  rek1 <- tbl(kon, "ref_rek90_1")
  rek2 <- tbl(kon, "ref_rek90_2")
  rek3 <- tbl(kon, "ref_rek90_3")
  rek4 <- tbl(kon, "ref_rek90_4")
  rek5 <- tbl(kon, "ref_rek90_5")
  rek6 <- tbl(kon, "ref_rek90_6")

  rek <- rek6 %>%
    left_join(rek5, by = c(
      "kd_rek90_1",
      "kd_rek90_2",
      "kd_rek90_3",
      "kd_rek90_4",
      "kd_rek90_5"
    )) %>%
    left_join(rek4, by = c(
      "kd_rek90_1",
      "kd_rek90_2",
      "kd_rek90_3",
      "kd_rek90_4"
    )) %>%
    left_join(rek3, by = c(
      "kd_rek90_1",
      "kd_rek90_2",
      "kd_rek90_3"
    )) %>%
    left_join(rek2, by = c(
      "kd_rek90_1",
      "kd_rek90_2"
    )) %>%
    left_join(rek1, by = c("kd_rek90_1"))

  rek <- rek %>%
    select(
      kd_rek90_1, kd_rek90_2, kd_rek90_3,
      kd_rek90_4, kd_rek90_5, kd_rek90_6,
      nm_rek90_1, nm_rek90_2, nm_rek90_3,
      nm_rek90_4, nm_rek90_5, nm_rek90_6,
      saldonorm
    ) %>%
    collect()

  xrek <- intersect(colnames(kewajiban), colnames(rek))

  unit <- tbl(kon, "ref_unit") %>% collect()
  xunit <- intersect(colnames(kewajiban), colnames(unit))

  kewajiban_1 <-
    left_join(kewajiban, rek, xrek)

  kewajiban_2 <-
    left_join(kewajiban_1, unit, xunit)

  kewajiban_3 <-
    kewajiban_2 %>%
    select(
      kd_unit90,
      nm_unit,
      kd_rek90_1,
      kd_rek90_2,
      kd_rek90_3,
      kd_rek90_4,
      kd_rek90_5,
      kd_rek90_6,
      nm_rek90_1,
      nm_rek90_2,
      nm_rek90_3,
      nm_rek90_4,
      nm_rek90_5,
      nm_rek90_6,
      saldonorm,
      saldo_thn_lalu = saldo,
      debet,
      kredit,
      saldo_akhir
    )
  return(kewajiban_3)
}

tarik_saldo_nrc_ekuitas <- function(kon) {
  pemda <- tbl(kon, "ta_pemda") %>%
    arrange(-tahun) %>%
    head(1) %>%
    collect()

  ta <- pemda %>%
    select(tahun) %>%
    pull()

  entitas <- pemda %>%
    select(nm_pemda) %>%
    pull() %>%
    word(start = 2, end = -1) %>%
    str_to_lower()

  jrn_nrc <- tbl(kon, "ta_jurnalsemuaak")
  jrn_nrc_r <- tbl(kon, "ta_jurnalsemuaak_rinc")
  xjrn <- intersect(colnames(jrn_nrc), colnames(jrn_nrc_r))
  jrn_nrc_i <-
    left_join(jrn_nrc, jrn_nrc_r, xjrn) %>%
    filter(tahun == ta) %>%
    filter(kd_rek90_1 == 3) %>%
    filter(kd_jurnal != 8) %>%
    group_by(
      kd_urusan,
      kd_bidang,
      kd_unit,
      kd_rek90_1,
      kd_rek90_2,
      kd_rek90_3,
      kd_rek90_4,
      kd_rek90_5,
      kd_rek90_6
    ) %>%
    summarise(
      debet = sum(debet, na.rm = TRUE),
      kredit = sum(kredit, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    collect()

  sal_awl <-
    tbl(kon, "ta_saldo_awalak") %>%
    filter(tahun == ta - 1) %>%
    filter(kd_rek90_1 == 3)

  sal_a <-
    sal_awl %>%
    group_by(
      kd_urusan,
      kd_bidang,
      kd_unit,
      kd_rek90_1,
      kd_rek90_2,
      kd_rek90_3,
      kd_rek90_4,
      kd_rek90_5,
      kd_rek90_6,
      d_k
    ) %>%
    summarise(
      saldo = sum(saldo, na.rm = TRUE),
      .groups = "drop"
    )

  sal_b <-
    sal_a %>%
    collect() %>%
    mutate(saldo = if_else(d_k == "K", saldo * 1, saldo * -1)) %>%
    group_by(
      kd_urusan,
      kd_bidang,
      kd_unit,
      kd_rek90_1,
      kd_rek90_2,
      kd_rek90_3,
      kd_rek90_4,
      kd_rek90_5,
      kd_rek90_6
    ) %>%
    summarise(
      saldo = sum(saldo, na.rm = TRUE),
      .groups = "drop"
    )

  xsal <- intersect(colnames(jrn_nrc_i), colnames(sal_b))

  ekuitas <-
    full_join(jrn_nrc_i, sal_b, xsal) %>%
    mutate(
      saldo = replace_na(saldo, 0),
      debet = replace_na(debet, 0),
      kredit = replace_na(kredit, 0)
    ) %>%
    mutate(
      saldo_akhir = saldo - debet + kredit
    ) %>%
    relocate(
      saldo,
      .before = debet
    )


  # menambahkan referensi akun rek90
  rek1 <- tbl(kon, "ref_rek90_1")
  rek2 <- tbl(kon, "ref_rek90_2")
  rek3 <- tbl(kon, "ref_rek90_3")
  rek4 <- tbl(kon, "ref_rek90_4")
  rek5 <- tbl(kon, "ref_rek90_5")
  rek6 <- tbl(kon, "ref_rek90_6")

  rek <- rek6 %>%
    left_join(rek5, by = c(
      "kd_rek90_1",
      "kd_rek90_2",
      "kd_rek90_3",
      "kd_rek90_4",
      "kd_rek90_5"
    )) %>%
    left_join(rek4, by = c(
      "kd_rek90_1",
      "kd_rek90_2",
      "kd_rek90_3",
      "kd_rek90_4"
    )) %>%
    left_join(rek3, by = c(
      "kd_rek90_1",
      "kd_rek90_2",
      "kd_rek90_3"
    )) %>%
    left_join(rek2, by = c(
      "kd_rek90_1",
      "kd_rek90_2"
    )) %>%
    left_join(rek1, by = c("kd_rek90_1"))

  rek <- rek %>%
    select(
      kd_rek90_1, kd_rek90_2, kd_rek90_3,
      kd_rek90_4, kd_rek90_5, kd_rek90_6,
      nm_rek90_1, nm_rek90_2, nm_rek90_3,
      nm_rek90_4, nm_rek90_5, nm_rek90_6,
      saldonorm
    ) %>%
    collect()

  xrek <- intersect(colnames(ekuitas), colnames(rek))

  unit <- tbl(kon, "ref_unit") %>% collect()
  xunit <- intersect(colnames(ekuitas), colnames(unit))

  ekuitas_1 <-
    left_join(ekuitas, rek, xrek)

  ekuitas_2 <-
    left_join(ekuitas_1, unit, xunit)

  ekuitas_3 <-
    ekuitas_2 %>%
    select(
      kd_unit90,
      nm_unit,
      kd_rek90_1,
      kd_rek90_2,
      kd_rek90_3,
      kd_rek90_4,
      kd_rek90_5,
      kd_rek90_6,
      nm_rek90_1,
      nm_rek90_2,
      nm_rek90_3,
      nm_rek90_4,
      nm_rek90_5,
      nm_rek90_6,
      saldonorm,
      saldo_thn_lalu = saldo,
      debet,
      kredit,
      saldo_akhir
    )
  return(ekuitas_3)
}

tarik_saldo_lra_pendapatan <- function(kon) {
  pemda <- tbl(kon, "ta_pemda") %>%
    arrange(-tahun) %>%
    head(1) %>%
    collect()

  ta <- pemda %>%
    select(tahun) %>%
    pull()

  entitas <- pemda %>%
    select(nm_pemda) %>%
    pull() %>%
    word(start = 2, end = -1) %>%
    str_to_lower()

  jrn_nrc <- tbl(kon, "ta_jurnalsemuaak")
  jrn_nrc_r <- tbl(kon, "ta_jurnalsemuaak_rinc")
  xjrn <- intersect(colnames(jrn_nrc), colnames(jrn_nrc_r))
  jrn_nrc_i <-
    left_join(jrn_nrc, jrn_nrc_r, xjrn) %>%
    filter(tahun == ta) %>%
    filter(kd_rek90_1 == 4) %>%
    filter(kd_jurnal != 8) %>%
    group_by(
      kd_urusan,
      kd_bidang,
      kd_unit,
      kd_rek90_1,
      kd_rek90_2,
      kd_rek90_3,
      kd_rek90_4,
      kd_rek90_5,
      kd_rek90_6
    ) %>%
    summarise(
      debet = sum(debet, na.rm = TRUE),
      kredit = sum(kredit, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    collect()

  sal_awl <-
    tbl(kon, "ta_saldo_awalak") %>%
    filter(tahun == ta - 1) %>%
    filter(kd_rek90_1 == 4)

  sal_a <-
    sal_awl %>%
    group_by(
      kd_urusan,
      kd_bidang,
      kd_unit,
      kd_rek90_1,
      kd_rek90_2,
      kd_rek90_3,
      kd_rek90_4,
      kd_rek90_5,
      kd_rek90_6,
      d_k
    ) %>%
    summarise(
      saldo = sum(saldo, na.rm = TRUE),
      .groups = "drop"
    )

  sal_b <-
    sal_a %>%
    collect() %>%
    mutate(saldo = if_else(d_k == "K", saldo * 1, saldo * -1)) %>%
    group_by(
      kd_urusan,
      kd_bidang,
      kd_unit,
      kd_rek90_1,
      kd_rek90_2,
      kd_rek90_3,
      kd_rek90_4,
      kd_rek90_5,
      kd_rek90_6
    ) %>%
    summarise(
      saldo = sum(saldo, na.rm = TRUE),
      .groups = "drop"
    )

  xsal <- intersect(colnames(jrn_nrc_i), colnames(sal_b))

  pendapatan <-
    full_join(jrn_nrc_i, sal_b, xsal) %>%
    mutate(
      saldo = replace_na(saldo, 0),
      debet = replace_na(debet, 0),
      kredit = replace_na(kredit, 0)
    ) %>%
    mutate(
      saldo_akhir = kredit - debet
    ) %>%
    relocate(
      saldo,
      .before = debet
    )

  # menarik tabel anggaran
  angg <- tbl(kon, "ta_rask_arsip") %>%
    filter(tahun == ta) %>%
    filter(kd_perubahan == 6)

  # menarik tabel untuk mengkonversi rek ke rek90
  map90 <- tbl(kon, "ref_rek_mapping") %>%
    select(kd_rek_1:kd_rek90_6)
  xmap90 <- intersect(colnames(angg), colnames(map90))

  # mengkonversi rek
  angg_t <- angg %>%
    left_join(map90, xmap90) %>%
    filter(kd_rek90_1 == 4)

  # menambahkan referensi
  angg_df <-
    angg_t %>%
    group_by(
      kd_urusan,
      kd_bidang,
      kd_unit,
      kd_rek90_1,
      kd_rek90_2,
      kd_rek90_3,
      kd_rek90_4,
      kd_rek90_5,
      kd_rek90_6
    ) %>%
    summarise(
      anggaran = sum(total, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    collect()

  xang <- intersect(colnames(pendapatan), colnames(angg_df))

  pendapatan_1 <-
    full_join(pendapatan, angg_df, xang) %>%
    replace_na(list(
      anggaran = 0,
      saldo = 0,
      debet = 0,
      kredit = 0,
      saldo_akhir = 0
    ))

  # menambahkan referensi akun rek90
  rek1 <- tbl(kon, "ref_rek90_1")
  rek2 <- tbl(kon, "ref_rek90_2")
  rek3 <- tbl(kon, "ref_rek90_3")
  rek4 <- tbl(kon, "ref_rek90_4")
  rek5 <- tbl(kon, "ref_rek90_5")
  rek6 <- tbl(kon, "ref_rek90_6")

  rek <- rek6 %>%
    left_join(rek5, by = c(
      "kd_rek90_1",
      "kd_rek90_2",
      "kd_rek90_3",
      "kd_rek90_4",
      "kd_rek90_5"
    )) %>%
    left_join(rek4, by = c(
      "kd_rek90_1",
      "kd_rek90_2",
      "kd_rek90_3",
      "kd_rek90_4"
    )) %>%
    left_join(rek3, by = c(
      "kd_rek90_1",
      "kd_rek90_2",
      "kd_rek90_3"
    )) %>%
    left_join(rek2, by = c(
      "kd_rek90_1",
      "kd_rek90_2"
    )) %>%
    left_join(rek1, by = c("kd_rek90_1"))

  rek <- rek %>%
    select(
      kd_rek90_1, kd_rek90_2, kd_rek90_3,
      kd_rek90_4, kd_rek90_5, kd_rek90_6,
      nm_rek90_1, nm_rek90_2, nm_rek90_3,
      nm_rek90_4, nm_rek90_5, nm_rek90_6,
      saldonorm
    ) %>%
    collect()

  xrek <- intersect(colnames(pendapatan), colnames(rek))

  unit <- tbl(kon, "ref_unit") %>% collect()
  xunit <- intersect(colnames(pendapatan), colnames(unit))

  pendapatan_2 <-
    left_join(pendapatan_1, rek, xrek)

  pendapatan_3 <-
    left_join(pendapatan_2, unit, xunit)

  pendapatan_4 <-
    pendapatan_3 %>%
    select(
      kd_unit90,
      nm_unit,
      kd_rek90_1,
      kd_rek90_2,
      kd_rek90_3,
      kd_rek90_4,
      kd_rek90_5,
      kd_rek90_6,
      nm_rek90_1,
      nm_rek90_2,
      nm_rek90_3,
      nm_rek90_4,
      nm_rek90_5,
      nm_rek90_6,
      saldonorm,
      anggaran,
      saldo_thn_lalu = saldo,
      debet,
      kredit,
      saldo_akhir
    )
  return(pendapatan_4)
}

tarik_saldo_lra_belanja <- function(kon) {
  pemda <- tbl(kon, "ta_pemda") %>%
    arrange(-tahun) %>%
    head(1) %>%
    collect()

  ta <- pemda %>%
    select(tahun) %>%
    pull()

  entitas <- pemda %>%
    select(nm_pemda) %>%
    pull() %>%
    word(start = 2, end = -1) %>%
    str_to_lower()

  jrn_nrc <- tbl(kon, "ta_jurnalsemuaak")
  jrn_nrc_r <- tbl(kon, "ta_jurnalsemuaak_rinc")
  xjrn <- intersect(colnames(jrn_nrc), colnames(jrn_nrc_r))
  jrn_nrc_i <-
    left_join(jrn_nrc, jrn_nrc_r, xjrn) %>%
    filter(tahun == ta) %>%
    filter(kd_rek90_1 == 5) %>%
    filter(kd_jurnal != 8) %>%
    group_by(
      kd_urusan,
      kd_bidang,
      kd_unit,
      kd_rek90_1,
      kd_rek90_2,
      kd_rek90_3,
      kd_rek90_4,
      kd_rek90_5,
      kd_rek90_6
    ) %>%
    summarise(
      debet = sum(debet, na.rm = TRUE),
      kredit = sum(kredit, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    collect()

  sal_awl <-
    tbl(kon, "ta_saldo_awalak") %>%
    filter(tahun == ta - 1) %>%
    filter(kd_rek90_1 == 5)

  sal_a <-
    sal_awl %>%
    group_by(
      kd_urusan,
      kd_bidang,
      kd_unit,
      kd_rek90_1,
      kd_rek90_2,
      kd_rek90_3,
      kd_rek90_4,
      kd_rek90_5,
      kd_rek90_6,
      d_k
    ) %>%
    summarise(
      saldo = sum(saldo, na.rm = TRUE),
      .groups = "drop"
    )

  sal_b <-
    sal_a %>%
    collect() %>%
    mutate(saldo = if_else(d_k == "D", saldo * 1, saldo * -1)) %>%
    group_by(
      kd_urusan,
      kd_bidang,
      kd_unit,
      kd_rek90_1,
      kd_rek90_2,
      kd_rek90_3,
      kd_rek90_4,
      kd_rek90_5,
      kd_rek90_6
    ) %>%
    summarise(
      saldo = sum(saldo, na.rm = TRUE),
      .groups = "drop"
    )

  xsal <- intersect(colnames(jrn_nrc_i), colnames(sal_b))

  belanja <-
    full_join(jrn_nrc_i, sal_b, xsal) %>%
    mutate(
      saldo = replace_na(saldo, 0),
      debet = replace_na(debet, 0),
      kredit = replace_na(kredit, 0)
    ) %>%
    mutate(
      saldo_akhir = debet - kredit
    ) %>%
    relocate(
      saldo,
      .before = debet
    )

  # menarik tabel anggaran
  angg <- tbl(kon, "ta_rask_arsip") %>%
    filter(tahun == ta) %>%
    filter(kd_perubahan == 6)

  # menarik tabel untuk mengkonversi rek ke rek90
  map90 <- tbl(kon, "ref_rek_mapping") %>%
    select(kd_rek_1:kd_rek90_6)
  xmap90 <- intersect(colnames(angg), colnames(map90))

  # mengkonversi rek
  angg_t <- angg %>%
    left_join(map90, xmap90) %>%
    filter(kd_rek90_1 == 5)

  # menambahkan referensi
  angg_df <-
    angg_t %>%
    group_by(
      kd_urusan,
      kd_bidang,
      kd_unit,
      kd_rek90_1,
      kd_rek90_2,
      kd_rek90_3,
      kd_rek90_4,
      kd_rek90_5,
      kd_rek90_6
    ) %>%
    summarise(
      anggaran = sum(total, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    collect()

  xang <- intersect(colnames(belanja), colnames(angg_df))

  belanja_1 <-
    full_join(belanja, angg_df, xang) %>%
    replace_na(list(
      anggaran = 0,
      saldo = 0,
      debet = 0,
      kredit = 0,
      saldo_akhir = 0
    ))

  # menambahkan referensi akun rek90
  rek1 <- tbl(kon, "ref_rek90_1")
  rek2 <- tbl(kon, "ref_rek90_2")
  rek3 <- tbl(kon, "ref_rek90_3")
  rek4 <- tbl(kon, "ref_rek90_4")
  rek5 <- tbl(kon, "ref_rek90_5")
  rek6 <- tbl(kon, "ref_rek90_6")

  rek <- rek6 %>%
    left_join(rek5, by = c(
      "kd_rek90_1",
      "kd_rek90_2",
      "kd_rek90_3",
      "kd_rek90_4",
      "kd_rek90_5"
    )) %>%
    left_join(rek4, by = c(
      "kd_rek90_1",
      "kd_rek90_2",
      "kd_rek90_3",
      "kd_rek90_4"
    )) %>%
    left_join(rek3, by = c(
      "kd_rek90_1",
      "kd_rek90_2",
      "kd_rek90_3"
    )) %>%
    left_join(rek2, by = c(
      "kd_rek90_1",
      "kd_rek90_2"
    )) %>%
    left_join(rek1, by = c("kd_rek90_1"))

  rek <- rek %>%
    select(
      kd_rek90_1, kd_rek90_2, kd_rek90_3,
      kd_rek90_4, kd_rek90_5, kd_rek90_6,
      nm_rek90_1, nm_rek90_2, nm_rek90_3,
      nm_rek90_4, nm_rek90_5, nm_rek90_6,
      saldonorm
    ) %>%
    collect()

  xrek <- intersect(colnames(belanja), colnames(rek))

  unit <- tbl(kon, "ref_unit") %>% collect()
  xunit <- intersect(colnames(belanja), colnames(unit))

  belanja_2 <-
    left_join(belanja_1, rek, xrek)

  belanja_3 <-
    left_join(belanja_2, unit, xunit)

  belanja_4 <-
    belanja_3 %>%
    select(
      kd_unit90,
      nm_unit,
      kd_rek90_1,
      kd_rek90_2,
      kd_rek90_3,
      kd_rek90_4,
      kd_rek90_5,
      kd_rek90_6,
      nm_rek90_1,
      nm_rek90_2,
      nm_rek90_3,
      nm_rek90_4,
      nm_rek90_5,
      nm_rek90_6,
      saldonorm,
      anggaran,
      saldo_thn_lalu = saldo,
      debet,
      kredit,
      saldo_akhir
    )
  return(belanja_4)
}

tarik_saldo_lra_terima_biaya <- function(kon) {
  pemda <- tbl(kon, "ta_pemda") %>%
    arrange(-tahun) %>%
    head(1) %>%
    collect()

  ta <- pemda %>%
    select(tahun) %>%
    pull()

  entitas <- pemda %>%
    select(nm_pemda) %>%
    pull() %>%
    word(start = 2, end = -1) %>%
    str_to_lower()

  jrn_nrc <- tbl(kon, "ta_jurnalsemuaak")
  jrn_nrc_r <- tbl(kon, "ta_jurnalsemuaak_rinc")
  xjrn <- intersect(colnames(jrn_nrc), colnames(jrn_nrc_r))
  jrn_nrc_i <-
    left_join(jrn_nrc, jrn_nrc_r, xjrn) %>%
    filter(tahun == ta) %>%
    filter(kd_rek90_1 == 6 & kd_rek90_2 == 1) %>%
    filter(kd_jurnal != 8) %>%
    group_by(
      kd_urusan,
      kd_bidang,
      kd_unit,
      kd_rek90_1,
      kd_rek90_2,
      kd_rek90_3,
      kd_rek90_4,
      kd_rek90_5,
      kd_rek90_6
    ) %>%
    summarise(
      debet = sum(debet, na.rm = TRUE),
      kredit = sum(kredit, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    collect()

  sal_awl <-
    tbl(kon, "ta_saldo_awalak") %>%
    filter(tahun == ta - 1) %>%
    filter(kd_rek90_1 == 6 & kd_rek90_2 == 1)

  sal_a <-
    sal_awl %>%
    group_by(
      kd_urusan,
      kd_bidang,
      kd_unit,
      kd_rek90_1,
      kd_rek90_2,
      kd_rek90_3,
      kd_rek90_4,
      kd_rek90_5,
      kd_rek90_6,
      d_k
    ) %>%
    summarise(
      saldo = sum(saldo, na.rm = TRUE),
      .groups = "drop"
    )

  sal_b <-
    sal_a %>%
    collect() %>%
    mutate(saldo = if_else(d_k == "D", saldo * 1, saldo * -1)) %>%
    group_by(
      kd_urusan,
      kd_bidang,
      kd_unit,
      kd_rek90_1,
      kd_rek90_2,
      kd_rek90_3,
      kd_rek90_4,
      kd_rek90_5,
      kd_rek90_6
    ) %>%
    summarise(
      saldo = sum(saldo, na.rm = TRUE),
      .groups = "drop"
    )

  xsal <- intersect(colnames(jrn_nrc_i), colnames(sal_b))

  pembiayaan <-
    full_join(jrn_nrc_i, sal_b, xsal) %>%
    mutate(
      saldo = replace_na(saldo, 0),
      debet = replace_na(debet, 0),
      kredit = replace_na(kredit, 0)
    ) %>%
    mutate(
      saldo_akhir = kredit - debet
    ) %>%
    relocate(
      saldo,
      .before = debet
    )

  # menarik tabel anggaran
  angg <- tbl(kon, "ta_rask_arsip") %>%
    filter(tahun == ta) %>%
    filter(kd_perubahan == 6)

  # menarik tabel untuk mengkonversi rek ke rek90
  map90 <- tbl(kon, "ref_rek_mapping") %>%
    select(kd_rek_1:kd_rek90_6)
  xmap90 <- intersect(colnames(angg), colnames(map90))

  # mengkonversi rek
  angg_t <- angg %>%
    left_join(map90, xmap90) %>%
    filter(kd_rek90_1 == 6 & kd_rek90_2 == 1)

  # menambahkan referensi
  angg_df <-
    angg_t %>%
    group_by(
      kd_urusan,
      kd_bidang,
      kd_unit,
      kd_rek90_1,
      kd_rek90_2,
      kd_rek90_3,
      kd_rek90_4,
      kd_rek90_5,
      kd_rek90_6
    ) %>%
    summarise(
      anggaran = sum(total, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    collect()

  xang <- intersect(colnames(pembiayaan), colnames(angg_df))

  pembiayaan_1 <-
    full_join(pembiayaan, angg_df, xang) %>%
    replace_na(list(
      anggaran = 0,
      saldo = 0,
      debet = 0,
      kredit = 0,
      saldo_akhir = 0
    ))

  # menambahkan referensi akun rek90
  rek1 <- tbl(kon, "ref_rek90_1")
  rek2 <- tbl(kon, "ref_rek90_2")
  rek3 <- tbl(kon, "ref_rek90_3")
  rek4 <- tbl(kon, "ref_rek90_4")
  rek5 <- tbl(kon, "ref_rek90_5")
  rek6 <- tbl(kon, "ref_rek90_6")

  rek <- rek6 %>%
    left_join(rek5, by = c(
      "kd_rek90_1",
      "kd_rek90_2",
      "kd_rek90_3",
      "kd_rek90_4",
      "kd_rek90_5"
    )) %>%
    left_join(rek4, by = c(
      "kd_rek90_1",
      "kd_rek90_2",
      "kd_rek90_3",
      "kd_rek90_4"
    )) %>%
    left_join(rek3, by = c(
      "kd_rek90_1",
      "kd_rek90_2",
      "kd_rek90_3"
    )) %>%
    left_join(rek2, by = c(
      "kd_rek90_1",
      "kd_rek90_2"
    )) %>%
    left_join(rek1, by = c("kd_rek90_1"))

  rek <- rek %>%
    select(
      kd_rek90_1, kd_rek90_2, kd_rek90_3,
      kd_rek90_4, kd_rek90_5, kd_rek90_6,
      nm_rek90_1, nm_rek90_2, nm_rek90_3,
      nm_rek90_4, nm_rek90_5, nm_rek90_6,
      saldonorm
    ) %>%
    collect()

  xrek <- intersect(colnames(pembiayaan), colnames(rek))

  unit <- tbl(kon, "ref_unit") %>% collect()
  xunit <- intersect(colnames(pembiayaan), colnames(unit))

  pembiayaan_2 <-
    left_join(pembiayaan_1, rek, xrek)

  pembiayaan_3 <-
    left_join(pembiayaan_2, unit, xunit)

  pembiayaan_4 <-
    pembiayaan_3 %>%
    select(
      kd_unit90,
      nm_unit,
      kd_rek90_1,
      kd_rek90_2,
      kd_rek90_3,
      kd_rek90_4,
      kd_rek90_5,
      kd_rek90_6,
      nm_rek90_1,
      nm_rek90_2,
      nm_rek90_3,
      nm_rek90_4,
      nm_rek90_5,
      nm_rek90_6,
      saldonorm,
      anggaran,
      saldo_thn_lalu = saldo,
      debet,
      kredit,
      saldo_akhir
    )
  return(pembiayaan_4)
}

tarik_saldo_lra_keluar_biaya <- function(kon) {
  pemda <- tbl(kon, "ta_pemda") %>%
    arrange(-tahun) %>%
    head(1) %>%
    collect()

  ta <- pemda %>%
    select(tahun) %>%
    pull()

  entitas <- pemda %>%
    select(nm_pemda) %>%
    pull() %>%
    word(start = 2, end = -1) %>%
    str_to_lower()

  jrn_nrc <- tbl(kon, "ta_jurnalsemuaak")
  jrn_nrc_r <- tbl(kon, "ta_jurnalsemuaak_rinc")
  xjrn <- intersect(colnames(jrn_nrc), colnames(jrn_nrc_r))
  jrn_nrc_i <-
    left_join(jrn_nrc, jrn_nrc_r, xjrn) %>%
    filter(tahun == ta) %>%
    filter(kd_rek90_1 == 6 & kd_rek90_2 == 2) %>%
    filter(kd_jurnal != 8) %>%
    group_by(
      kd_urusan,
      kd_bidang,
      kd_unit,
      kd_rek90_1,
      kd_rek90_2,
      kd_rek90_3,
      kd_rek90_4,
      kd_rek90_5,
      kd_rek90_6
    ) %>%
    summarise(
      debet = sum(debet, na.rm = TRUE),
      kredit = sum(kredit, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    collect()

  sal_awl <-
    tbl(kon, "ta_saldo_awalak") %>%
    filter(tahun == ta - 1) %>%
    filter(kd_rek90_1 == 6 & kd_rek90_2 == 2)

  sal_a <-
    sal_awl %>%
    group_by(
      kd_urusan,
      kd_bidang,
      kd_unit,
      kd_rek90_1,
      kd_rek90_2,
      kd_rek90_3,
      kd_rek90_4,
      kd_rek90_5,
      kd_rek90_6,
      d_k
    ) %>%
    summarise(
      saldo = sum(saldo, na.rm = TRUE),
      .groups = "drop"
    )

  sal_b <-
    sal_a %>%
    collect() %>%
    mutate(saldo = if_else(d_k == "D", saldo * 1, saldo * -1)) %>%
    group_by(
      kd_urusan,
      kd_bidang,
      kd_unit,
      kd_rek90_1,
      kd_rek90_2,
      kd_rek90_3,
      kd_rek90_4,
      kd_rek90_5,
      kd_rek90_6
    ) %>%
    summarise(
      saldo = sum(saldo, na.rm = TRUE),
      .groups = "drop"
    )

  xsal <- intersect(colnames(jrn_nrc_i), colnames(sal_b))

  pembiayaan <-
    full_join(jrn_nrc_i, sal_b, xsal) %>%
    mutate(
      saldo = replace_na(saldo, 0),
      debet = replace_na(debet, 0),
      kredit = replace_na(kredit, 0)
    ) %>%
    mutate(
      saldo_akhir = debet - kredit
    ) %>%
    relocate(
      saldo,
      .before = debet
    )

  # menarik tabel anggaran
  angg <- tbl(kon, "ta_rask_arsip") %>%
    filter(tahun == ta) %>%
    filter(kd_perubahan == 6)

  # menarik tabel untuk mengkonversi rek ke rek90
  map90 <- tbl(kon, "ref_rek_mapping") %>%
    select(kd_rek_1:kd_rek90_6)
  xmap90 <- intersect(colnames(angg), colnames(map90))

  # mengkonversi rek
  angg_t <- angg %>%
    left_join(map90, xmap90) %>%
    filter(kd_rek90_1 == 6 & kd_rek90_2 == 2)

  # menambahkan referensi
  angg_df <-
    angg_t %>%
    group_by(
      kd_urusan,
      kd_bidang,
      kd_unit,
      kd_rek90_1,
      kd_rek90_2,
      kd_rek90_3,
      kd_rek90_4,
      kd_rek90_5,
      kd_rek90_6
    ) %>%
    summarise(
      anggaran = sum(total, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    collect()

  xang <- intersect(colnames(pembiayaan), colnames(angg_df))

  pembiayaan_1 <-
    full_join(pembiayaan, angg_df, xang) %>%
    replace_na(list(
      anggaran = 0,
      saldo = 0,
      debet = 0,
      kredit = 0,
      saldo_akhir = 0
    ))

  # menambahkan referensi akun rek90
  rek1 <- tbl(kon, "ref_rek90_1")
  rek2 <- tbl(kon, "ref_rek90_2")
  rek3 <- tbl(kon, "ref_rek90_3")
  rek4 <- tbl(kon, "ref_rek90_4")
  rek5 <- tbl(kon, "ref_rek90_5")
  rek6 <- tbl(kon, "ref_rek90_6")

  rek <- rek6 %>%
    left_join(rek5, by = c(
      "kd_rek90_1",
      "kd_rek90_2",
      "kd_rek90_3",
      "kd_rek90_4",
      "kd_rek90_5"
    )) %>%
    left_join(rek4, by = c(
      "kd_rek90_1",
      "kd_rek90_2",
      "kd_rek90_3",
      "kd_rek90_4"
    )) %>%
    left_join(rek3, by = c(
      "kd_rek90_1",
      "kd_rek90_2",
      "kd_rek90_3"
    )) %>%
    left_join(rek2, by = c(
      "kd_rek90_1",
      "kd_rek90_2"
    )) %>%
    left_join(rek1, by = c("kd_rek90_1"))

  rek <- rek %>%
    select(
      kd_rek90_1, kd_rek90_2, kd_rek90_3,
      kd_rek90_4, kd_rek90_5, kd_rek90_6,
      nm_rek90_1, nm_rek90_2, nm_rek90_3,
      nm_rek90_4, nm_rek90_5, nm_rek90_6,
      saldonorm
    ) %>%
    collect()

  xrek <- intersect(colnames(pembiayaan), colnames(rek))

  unit <- tbl(kon, "ref_unit") %>% collect()
  xunit <- intersect(colnames(pembiayaan), colnames(unit))

  pembiayaan_2 <-
    left_join(pembiayaan_1, rek, xrek)

  pembiayaan_3 <-
    left_join(pembiayaan_2, unit, xunit)

  pembiayaan_4 <-
    pembiayaan_3 %>%
    select(
      kd_unit90,
      nm_unit,
      kd_rek90_1,
      kd_rek90_2,
      kd_rek90_3,
      kd_rek90_4,
      kd_rek90_5,
      kd_rek90_6,
      nm_rek90_1,
      nm_rek90_2,
      nm_rek90_3,
      nm_rek90_4,
      nm_rek90_5,
      nm_rek90_6,
      saldonorm,
      anggaran,
      saldo_thn_lalu = saldo,
      debet,
      kredit,
      saldo_akhir
    )
  return(pembiayaan_4)
}

tarik_saldo_lra_silpa <- function(kon) {
  pemda <- tbl(kon, "ta_pemda") %>%
    arrange(-tahun) %>%
    head(1) %>%
    collect()

  ta <- pemda %>%
    select(tahun) %>%
    pull()

  entitas <- pemda %>%
    select(nm_pemda) %>%
    pull() %>%
    word(start = 2, end = -1) %>%
    str_to_lower()

  jrn_nrc <- tbl(kon, "ta_jurnalsemuaak")
  jrn_nrc_r <- tbl(kon, "ta_jurnalsemuaak_rinc")
  xjrn <- intersect(colnames(jrn_nrc), colnames(jrn_nrc_r))
  jrn_nrc_i <-
    left_join(jrn_nrc, jrn_nrc_r, xjrn) %>%
    filter(tahun == ta) %>%
    filter(kd_rek90_1 == 6 & kd_rek90_2 == 3) %>%
    filter(kd_jurnal != 8) %>%
    group_by(
      kd_urusan,
      kd_bidang,
      kd_unit,
      kd_rek90_1,
      kd_rek90_2,
      kd_rek90_3,
      kd_rek90_4,
      kd_rek90_5,
      kd_rek90_6
    ) %>%
    summarise(
      debet = sum(debet, na.rm = TRUE),
      kredit = sum(kredit, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    collect()

  sal_awl <-
    tbl(kon, "ta_saldo_awalak") %>%
    filter(tahun == ta - 1) %>%
    filter(kd_rek90_1 == 6 & kd_rek90_2 == 3)

  sal_a <-
    sal_awl %>%
    group_by(
      kd_urusan,
      kd_bidang,
      kd_unit,
      kd_rek90_1,
      kd_rek90_2,
      kd_rek90_3,
      kd_rek90_4,
      kd_rek90_5,
      kd_rek90_6,
      d_k
    ) %>%
    summarise(
      saldo = sum(saldo, na.rm = TRUE),
      .groups = "drop"
    )

  sal_b <-
    sal_a %>%
    collect() %>%
    mutate(saldo = if_else(d_k == "D", saldo * 1, saldo * -1)) %>%
    group_by(
      kd_urusan,
      kd_bidang,
      kd_unit,
      kd_rek90_1,
      kd_rek90_2,
      kd_rek90_3,
      kd_rek90_4,
      kd_rek90_5,
      kd_rek90_6
    ) %>%
    summarise(
      saldo = sum(saldo, na.rm = TRUE),
      .groups = "drop"
    )

  xsal <- intersect(colnames(jrn_nrc_i), colnames(sal_b))

  pembiayaan <-
    full_join(jrn_nrc_i, sal_b, xsal) %>%
    mutate(
      saldo = replace_na(saldo, 0),
      debet = replace_na(debet, 0),
      kredit = replace_na(kredit, 0)
    ) %>%
    mutate(
      saldo_akhir = kredit - debet
    ) %>%
    relocate(
      saldo,
      .before = debet
    )

  # menarik tabel anggaran
  angg <- tbl(kon, "ta_rask_arsip") %>%
    filter(tahun == ta) %>%
    filter(kd_perubahan == 6)

  # menarik tabel untuk mengkonversi rek ke rek90
  map90 <- tbl(kon, "ref_rek_mapping") %>%
    select(kd_rek_1:kd_rek90_6)
  xmap90 <- intersect(colnames(angg), colnames(map90))

  # mengkonversi rek
  angg_t <- angg %>%
    left_join(map90, xmap90) %>%
    filter(kd_rek90_1 == 6 & kd_rek90_2 == 3)

  # menambahkan referensi
  angg_df <-
    angg_t %>%
    group_by(
      kd_urusan,
      kd_bidang,
      kd_unit,
      kd_rek90_1,
      kd_rek90_2,
      kd_rek90_3,
      kd_rek90_4,
      kd_rek90_5,
      kd_rek90_6
    ) %>%
    summarise(
      anggaran = sum(total, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    collect()

  xang <- intersect(colnames(pembiayaan), colnames(angg_df))

  pembiayaan_1 <-
    full_join(pembiayaan, angg_df, xang) %>%
    replace_na(list(
      anggaran = 0,
      saldo = 0,
      debet = 0,
      kredit = 0,
      saldo_akhir = 0
    ))

  # menambahkan referensi akun rek90
  rek1 <- tbl(kon, "ref_rek90_1")
  rek2 <- tbl(kon, "ref_rek90_2")
  rek3 <- tbl(kon, "ref_rek90_3")
  rek4 <- tbl(kon, "ref_rek90_4")
  rek5 <- tbl(kon, "ref_rek90_5")
  rek6 <- tbl(kon, "ref_rek90_6")

  rek <- rek6 %>%
    left_join(rek5, by = c(
      "kd_rek90_1",
      "kd_rek90_2",
      "kd_rek90_3",
      "kd_rek90_4",
      "kd_rek90_5"
    )) %>%
    left_join(rek4, by = c(
      "kd_rek90_1",
      "kd_rek90_2",
      "kd_rek90_3",
      "kd_rek90_4"
    )) %>%
    left_join(rek3, by = c(
      "kd_rek90_1",
      "kd_rek90_2",
      "kd_rek90_3"
    )) %>%
    left_join(rek2, by = c(
      "kd_rek90_1",
      "kd_rek90_2"
    )) %>%
    left_join(rek1, by = c("kd_rek90_1"))

  rek <- rek %>%
    select(
      kd_rek90_1, kd_rek90_2, kd_rek90_3,
      kd_rek90_4, kd_rek90_5, kd_rek90_6,
      nm_rek90_1, nm_rek90_2, nm_rek90_3,
      nm_rek90_4, nm_rek90_5, nm_rek90_6,
      saldonorm
    ) %>%
    collect()

  xrek <- intersect(colnames(pembiayaan), colnames(rek))

  unit <- tbl(kon, "ref_unit") %>% collect()
  xunit <- intersect(colnames(pembiayaan), colnames(unit))

  pembiayaan_2 <-
    left_join(pembiayaan_1, rek, xrek)

  pembiayaan_3 <-
    left_join(pembiayaan_2, unit, xunit)

  pembiayaan_4 <-
    pembiayaan_3 %>%
    select(
      kd_unit90,
      nm_unit,
      kd_rek90_1,
      kd_rek90_2,
      kd_rek90_3,
      kd_rek90_4,
      kd_rek90_5,
      kd_rek90_6,
      nm_rek90_1,
      nm_rek90_2,
      nm_rek90_3,
      nm_rek90_4,
      nm_rek90_5,
      nm_rek90_6,
      saldonorm,
      anggaran,
      saldo_thn_lalu = saldo,
      debet,
      kredit,
      saldo_akhir
    )
  return(pembiayaan_4)
}

tarik_saldo_lo_pendapatan <- function(kon) {
  pemda <- tbl(kon, "ta_pemda") %>%
    arrange(-tahun) %>%
    head(1) %>%
    collect()

  ta <- pemda %>%
    select(tahun) %>%
    pull()

  entitas <- pemda %>%
    select(nm_pemda) %>%
    pull() %>%
    word(start = 2, end = -1) %>%
    str_to_lower()

  jrn_nrc <- tbl(kon, "ta_jurnalsemuaak")
  jrn_nrc_r <- tbl(kon, "ta_jurnalsemuaak_rinc")
  xjrn <- intersect(colnames(jrn_nrc), colnames(jrn_nrc_r))
  jrn_nrc_i <-
    left_join(jrn_nrc, jrn_nrc_r, xjrn) %>%
    filter(tahun == ta) %>%
    filter(kd_rek90_1 == 7 & kd_rek90_2 < 4) %>%
    filter(kd_jurnal != 8) %>%
    group_by(
      kd_urusan,
      kd_bidang,
      kd_unit,
      kd_rek90_1,
      kd_rek90_2,
      kd_rek90_3,
      kd_rek90_4,
      kd_rek90_5,
      kd_rek90_6
    ) %>%
    summarise(
      debet = sum(debet, na.rm = TRUE),
      kredit = sum(kredit, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    collect()

  sal_awl <-
    tbl(kon, "ta_saldo_awalak") %>%
    filter(tahun == ta - 1) %>%
    filter(kd_rek90_1 == 7 & kd_rek90_2 < 4)

  sal_a <-
    sal_awl %>%
    group_by(
      kd_urusan,
      kd_bidang,
      kd_unit,
      kd_rek90_1,
      kd_rek90_2,
      kd_rek90_3,
      kd_rek90_4,
      kd_rek90_5,
      kd_rek90_6,
      d_k
    ) %>%
    summarise(
      saldo = sum(saldo, na.rm = TRUE),
      .groups = "drop"
    )

  sal_b <-
    sal_a %>%
    collect() %>%
    mutate(saldo = if_else(d_k == "K", saldo * 1, saldo * -1)) %>%
    group_by(
      kd_urusan,
      kd_bidang,
      kd_unit,
      kd_rek90_1,
      kd_rek90_2,
      kd_rek90_3,
      kd_rek90_4,
      kd_rek90_5,
      kd_rek90_6
    ) %>%
    summarise(
      saldo = sum(saldo, na.rm = TRUE),
      .groups = "drop"
    )

  xsal <- intersect(colnames(jrn_nrc_i), colnames(sal_b))

  lo_pendapatan <-
    full_join(jrn_nrc_i, sal_b, xsal) %>%
    mutate(
      saldo = replace_na(saldo, 0),
      debet = replace_na(debet, 0),
      kredit = replace_na(kredit, 0)
    ) %>%
    mutate(
      saldo_akhir = kredit - debet
    ) %>%
    relocate(
      saldo,
      .before = debet
    )


  # menambahkan referensi akun rek90
  rek1 <- tbl(kon, "ref_rek90_1")
  rek2 <- tbl(kon, "ref_rek90_2")
  rek3 <- tbl(kon, "ref_rek90_3")
  rek4 <- tbl(kon, "ref_rek90_4")
  rek5 <- tbl(kon, "ref_rek90_5")
  rek6 <- tbl(kon, "ref_rek90_6")

  rek <- rek6 %>%
    left_join(rek5, by = c(
      "kd_rek90_1",
      "kd_rek90_2",
      "kd_rek90_3",
      "kd_rek90_4",
      "kd_rek90_5"
    )) %>%
    left_join(rek4, by = c(
      "kd_rek90_1",
      "kd_rek90_2",
      "kd_rek90_3",
      "kd_rek90_4"
    )) %>%
    left_join(rek3, by = c(
      "kd_rek90_1",
      "kd_rek90_2",
      "kd_rek90_3"
    )) %>%
    left_join(rek2, by = c(
      "kd_rek90_1",
      "kd_rek90_2"
    )) %>%
    left_join(rek1, by = c("kd_rek90_1"))

  rek <- rek %>%
    select(
      kd_rek90_1, kd_rek90_2, kd_rek90_3,
      kd_rek90_4, kd_rek90_5, kd_rek90_6,
      nm_rek90_1, nm_rek90_2, nm_rek90_3,
      nm_rek90_4, nm_rek90_5, nm_rek90_6,
      saldonorm
    ) %>%
    collect()

  xrek <- intersect(colnames(lo_pendapatan), colnames(rek))

  unit <- tbl(kon, "ref_unit") %>% collect()
  xunit <- intersect(colnames(lo_pendapatan), colnames(unit))

  lo_pendapatan_1 <-
    left_join(lo_pendapatan, rek, xrek)

  lo_pendapatan_2 <-
    left_join(lo_pendapatan_1, unit, xunit)

  lo_pendapatan_3 <-
    lo_pendapatan_2 %>%
    select(
      kd_unit90,
      nm_unit,
      kd_rek90_1,
      kd_rek90_2,
      kd_rek90_3,
      kd_rek90_4,
      kd_rek90_5,
      kd_rek90_6,
      nm_rek90_1,
      nm_rek90_2,
      nm_rek90_3,
      nm_rek90_4,
      nm_rek90_5,
      nm_rek90_6,
      saldonorm,
      saldo_thn_lalu = saldo,
      debet,
      kredit,
      saldo_akhir
    )
  return(lo_pendapatan_3)
}

tarik_saldo_lo_surplus <- function(kon) {
  pemda <- tbl(kon, "ta_pemda") %>%
    arrange(-tahun) %>%
    head(1) %>%
    collect()

  ta <- pemda %>%
    select(tahun) %>%
    pull()

  entitas <- pemda %>%
    select(nm_pemda) %>%
    pull() %>%
    word(start = 2, end = -1) %>%
    str_to_lower()

  jrn_nrc <- tbl(kon, "ta_jurnalsemuaak")
  jrn_nrc_r <- tbl(kon, "ta_jurnalsemuaak_rinc")
  xjrn <- intersect(colnames(jrn_nrc), colnames(jrn_nrc_r))
  jrn_nrc_i <-
    left_join(jrn_nrc, jrn_nrc_r, xjrn) %>%
    filter(tahun == ta) %>%
    filter(kd_rek90_1 == 7 & kd_rek90_2 == 4) %>%
    filter(kd_jurnal != 8) %>%
    group_by(
      kd_urusan,
      kd_bidang,
      kd_unit,
      kd_rek90_1,
      kd_rek90_2,
      kd_rek90_3,
      kd_rek90_4,
      kd_rek90_5,
      kd_rek90_6
    ) %>%
    summarise(
      debet = sum(debet, na.rm = TRUE),
      kredit = sum(kredit, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    collect()

  sal_awl <-
    tbl(kon, "ta_saldo_awalak") %>%
    filter(tahun == ta - 1) %>%
    filter(kd_rek90_1 == 7 & kd_rek90_2 == 4)

  sal_a <-
    sal_awl %>%
    group_by(
      kd_urusan,
      kd_bidang,
      kd_unit,
      kd_rek90_1,
      kd_rek90_2,
      kd_rek90_3,
      kd_rek90_4,
      kd_rek90_5,
      kd_rek90_6,
      d_k
    ) %>%
    summarise(
      saldo = sum(saldo, na.rm = TRUE),
      .groups = "drop"
    )

  sal_b <-
    sal_a %>%
    collect() %>%
    mutate(saldo = if_else(d_k == "K", saldo * 1, saldo * -1)) %>%
    group_by(
      kd_urusan,
      kd_bidang,
      kd_unit,
      kd_rek90_1,
      kd_rek90_2,
      kd_rek90_3,
      kd_rek90_4,
      kd_rek90_5,
      kd_rek90_6
    ) %>%
    summarise(
      saldo = sum(saldo, na.rm = TRUE),
      .groups = "drop"
    )

  xsal <- intersect(colnames(jrn_nrc_i), colnames(sal_b))

  lo_pendapatan <-
    full_join(jrn_nrc_i, sal_b, xsal) %>%
    mutate(
      saldo = replace_na(saldo, 0),
      debet = replace_na(debet, 0),
      kredit = replace_na(kredit, 0)
    ) %>%
    mutate(
      saldo_akhir = kredit - debet
    ) %>%
    relocate(
      saldo,
      .before = debet
    )


  # menambahkan referensi akun rek90
  rek1 <- tbl(kon, "ref_rek90_1")
  rek2 <- tbl(kon, "ref_rek90_2")
  rek3 <- tbl(kon, "ref_rek90_3")
  rek4 <- tbl(kon, "ref_rek90_4")
  rek5 <- tbl(kon, "ref_rek90_5")
  rek6 <- tbl(kon, "ref_rek90_6")

  rek <- rek6 %>%
    left_join(rek5, by = c(
      "kd_rek90_1",
      "kd_rek90_2",
      "kd_rek90_3",
      "kd_rek90_4",
      "kd_rek90_5"
    )) %>%
    left_join(rek4, by = c(
      "kd_rek90_1",
      "kd_rek90_2",
      "kd_rek90_3",
      "kd_rek90_4"
    )) %>%
    left_join(rek3, by = c(
      "kd_rek90_1",
      "kd_rek90_2",
      "kd_rek90_3"
    )) %>%
    left_join(rek2, by = c(
      "kd_rek90_1",
      "kd_rek90_2"
    )) %>%
    left_join(rek1, by = c("kd_rek90_1"))

  rek <- rek %>%
    select(
      kd_rek90_1, kd_rek90_2, kd_rek90_3,
      kd_rek90_4, kd_rek90_5, kd_rek90_6,
      nm_rek90_1, nm_rek90_2, nm_rek90_3,
      nm_rek90_4, nm_rek90_5, nm_rek90_6,
      saldonorm
    ) %>%
    collect()

  xrek <- intersect(colnames(lo_pendapatan), colnames(rek))

  unit <- tbl(kon, "ref_unit") %>% collect()
  xunit <- intersect(colnames(lo_pendapatan), colnames(unit))

  lo_pendapatan_1 <-
    left_join(lo_pendapatan, rek, xrek)

  lo_pendapatan_2 <-
    left_join(lo_pendapatan_1, unit, xunit)

  lo_pendapatan_3 <-
    lo_pendapatan_2 %>%
    select(
      kd_unit90,
      nm_unit,
      kd_rek90_1,
      kd_rek90_2,
      kd_rek90_3,
      kd_rek90_4,
      kd_rek90_5,
      kd_rek90_6,
      nm_rek90_1,
      nm_rek90_2,
      nm_rek90_3,
      nm_rek90_4,
      nm_rek90_5,
      nm_rek90_6,
      saldonorm,
      saldo_thn_lalu = saldo,
      debet,
      kredit,
      saldo_akhir
    )
  return(lo_pendapatan_3)
}

tarik_saldo_lo_beban <- function(kon) {
  pemda <- tbl(kon, "ta_pemda") %>%
    arrange(-tahun) %>%
    head(1) %>%
    collect()

  ta <- pemda %>%
    select(tahun) %>%
    pull()

  entitas <- pemda %>%
    select(nm_pemda) %>%
    pull() %>%
    word(start = 2, end = -1) %>%
    str_to_lower()

  jrn_nrc <- tbl(kon, "ta_jurnalsemuaak")
  jrn_nrc_r <- tbl(kon, "ta_jurnalsemuaak_rinc")
  xjrn <- intersect(colnames(jrn_nrc), colnames(jrn_nrc_r))
  jrn_nrc_i <-
    left_join(jrn_nrc, jrn_nrc_r, xjrn) %>%
    filter(tahun == ta) %>%
    filter(kd_rek90_1 == 8 & kd_rek90_2 < 5) %>%
    filter(kd_jurnal != 8) %>%
    group_by(
      kd_urusan,
      kd_bidang,
      kd_unit,
      kd_rek90_1,
      kd_rek90_2,
      kd_rek90_3,
      kd_rek90_4,
      kd_rek90_5,
      kd_rek90_6
    ) %>%
    summarise(
      debet = sum(debet, na.rm = TRUE),
      kredit = sum(kredit, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    collect()

  sal_awl <-
    tbl(kon, "ta_saldo_awalak") %>%
    filter(tahun == ta - 1) %>%
    filter(kd_rek90_1 == 8 & kd_rek90_2 < 5)

  sal_a <-
    sal_awl %>%
    group_by(
      kd_urusan,
      kd_bidang,
      kd_unit,
      kd_rek90_1,
      kd_rek90_2,
      kd_rek90_3,
      kd_rek90_4,
      kd_rek90_5,
      kd_rek90_6,
      d_k
    ) %>%
    summarise(
      saldo = sum(saldo, na.rm = TRUE),
      .groups = "drop"
    )

  sal_b <-
    sal_a %>%
    collect() %>%
    mutate(saldo = if_else(d_k == "D", saldo * 1, saldo * -1)) %>%
    group_by(
      kd_urusan,
      kd_bidang,
      kd_unit,
      kd_rek90_1,
      kd_rek90_2,
      kd_rek90_3,
      kd_rek90_4,
      kd_rek90_5,
      kd_rek90_6
    ) %>%
    summarise(
      saldo = sum(saldo, na.rm = TRUE),
      .groups = "drop"
    )

  xsal <- intersect(colnames(jrn_nrc_i), colnames(sal_b))

  lo_beban <-
    full_join(jrn_nrc_i, sal_b, xsal) %>%
    mutate(
      saldo = replace_na(saldo, 0),
      debet = replace_na(debet, 0),
      kredit = replace_na(kredit, 0)
    ) %>%
    mutate(
      saldo_akhir = debet - kredit
    ) %>%
    relocate(
      saldo,
      .before = debet
    )


  # menambahkan referensi akun rek90
  rek1 <- tbl(kon, "ref_rek90_1")
  rek2 <- tbl(kon, "ref_rek90_2")
  rek3 <- tbl(kon, "ref_rek90_3")
  rek4 <- tbl(kon, "ref_rek90_4")
  rek5 <- tbl(kon, "ref_rek90_5")
  rek6 <- tbl(kon, "ref_rek90_6")

  rek <- rek6 %>%
    left_join(rek5, by = c(
      "kd_rek90_1",
      "kd_rek90_2",
      "kd_rek90_3",
      "kd_rek90_4",
      "kd_rek90_5"
    )) %>%
    left_join(rek4, by = c(
      "kd_rek90_1",
      "kd_rek90_2",
      "kd_rek90_3",
      "kd_rek90_4"
    )) %>%
    left_join(rek3, by = c(
      "kd_rek90_1",
      "kd_rek90_2",
      "kd_rek90_3"
    )) %>%
    left_join(rek2, by = c(
      "kd_rek90_1",
      "kd_rek90_2"
    )) %>%
    left_join(rek1, by = c("kd_rek90_1"))

  rek <- rek %>%
    select(
      kd_rek90_1, kd_rek90_2, kd_rek90_3,
      kd_rek90_4, kd_rek90_5, kd_rek90_6,
      nm_rek90_1, nm_rek90_2, nm_rek90_3,
      nm_rek90_4, nm_rek90_5, nm_rek90_6,
      saldonorm
    ) %>%
    collect()

  xrek <- intersect(colnames(lo_beban), colnames(rek))

  unit <- tbl(kon, "ref_unit") %>% collect()
  xunit <- intersect(colnames(lo_beban), colnames(unit))

  lo_beban_1 <-
    left_join(lo_beban, rek, xrek)

  lo_beban_2 <-
    left_join(lo_beban_1, unit, xunit)

  lo_beban_3 <-
    lo_beban_2 %>%
    select(
      kd_unit90,
      nm_unit,
      kd_rek90_1,
      kd_rek90_2,
      kd_rek90_3,
      kd_rek90_4,
      kd_rek90_5,
      kd_rek90_6,
      nm_rek90_1,
      nm_rek90_2,
      nm_rek90_3,
      nm_rek90_4,
      nm_rek90_5,
      nm_rek90_6,
      saldonorm,
      saldo_thn_lalu = saldo,
      debet,
      kredit,
      saldo_akhir
    )
  return(lo_beban_3)
}

tarik_saldo_lo_defisit <- function(kon) {
  pemda <- tbl(kon, "ta_pemda") %>%
    arrange(-tahun) %>%
    head(1) %>%
    collect()

  ta <- pemda %>%
    select(tahun) %>%
    pull()

  entitas <- pemda %>%
    select(nm_pemda) %>%
    pull() %>%
    word(start = 2, end = -1) %>%
    str_to_lower()

  jrn_nrc <- tbl(kon, "ta_jurnalsemuaak")
  jrn_nrc_r <- tbl(kon, "ta_jurnalsemuaak_rinc")
  xjrn <- intersect(colnames(jrn_nrc), colnames(jrn_nrc_r))
  jrn_nrc_i <-
    left_join(jrn_nrc, jrn_nrc_r, xjrn) %>%
    filter(tahun == ta) %>%
    filter(kd_rek90_1 == 8 & kd_rek90_2 == 5) %>%
    filter(kd_jurnal != 8) %>%
    group_by(
      kd_urusan,
      kd_bidang,
      kd_unit,
      kd_rek90_1,
      kd_rek90_2,
      kd_rek90_3,
      kd_rek90_4,
      kd_rek90_5,
      kd_rek90_6
    ) %>%
    summarise(
      debet = sum(debet, na.rm = TRUE),
      kredit = sum(kredit, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    collect()

  sal_awl <-
    tbl(kon, "ta_saldo_awalak") %>%
    filter(tahun == ta - 1) %>%
    filter(kd_rek90_1 == 8 & kd_rek90_2 == 5)

  sal_a <-
    sal_awl %>%
    group_by(
      kd_urusan,
      kd_bidang,
      kd_unit,
      kd_rek90_1,
      kd_rek90_2,
      kd_rek90_3,
      kd_rek90_4,
      kd_rek90_5,
      kd_rek90_6,
      d_k
    ) %>%
    summarise(
      saldo = sum(saldo, na.rm = TRUE),
      .groups = "drop"
    )

  sal_b <-
    sal_a %>%
    collect() %>%
    mutate(saldo = if_else(d_k == "D", saldo * 1, saldo * -1)) %>%
    group_by(
      kd_urusan,
      kd_bidang,
      kd_unit,
      kd_rek90_1,
      kd_rek90_2,
      kd_rek90_3,
      kd_rek90_4,
      kd_rek90_5,
      kd_rek90_6
    ) %>%
    summarise(
      saldo = sum(saldo, na.rm = TRUE),
      .groups = "drop"
    )

  xsal <- intersect(colnames(jrn_nrc_i), colnames(sal_b))

  lo_beban <-
    full_join(jrn_nrc_i, sal_b, xsal) %>%
    mutate(
      saldo = replace_na(saldo, 0),
      debet = replace_na(debet, 0),
      kredit = replace_na(kredit, 0)
    ) %>%
    mutate(
      saldo_akhir = debet - kredit
    ) %>%
    relocate(
      saldo,
      .before = debet
    )


  # menambahkan referensi akun rek90
  rek1 <- tbl(kon, "ref_rek90_1")
  rek2 <- tbl(kon, "ref_rek90_2")
  rek3 <- tbl(kon, "ref_rek90_3")
  rek4 <- tbl(kon, "ref_rek90_4")
  rek5 <- tbl(kon, "ref_rek90_5")
  rek6 <- tbl(kon, "ref_rek90_6")

  rek <- rek6 %>%
    left_join(rek5, by = c(
      "kd_rek90_1",
      "kd_rek90_2",
      "kd_rek90_3",
      "kd_rek90_4",
      "kd_rek90_5"
    )) %>%
    left_join(rek4, by = c(
      "kd_rek90_1",
      "kd_rek90_2",
      "kd_rek90_3",
      "kd_rek90_4"
    )) %>%
    left_join(rek3, by = c(
      "kd_rek90_1",
      "kd_rek90_2",
      "kd_rek90_3"
    )) %>%
    left_join(rek2, by = c(
      "kd_rek90_1",
      "kd_rek90_2"
    )) %>%
    left_join(rek1, by = c("kd_rek90_1"))

  rek <- rek %>%
    select(
      kd_rek90_1, kd_rek90_2, kd_rek90_3,
      kd_rek90_4, kd_rek90_5, kd_rek90_6,
      nm_rek90_1, nm_rek90_2, nm_rek90_3,
      nm_rek90_4, nm_rek90_5, nm_rek90_6,
      saldonorm
    ) %>%
    collect()

  xrek <- intersect(colnames(lo_beban), colnames(rek))

  unit <- tbl(kon, "ref_unit") %>% collect()
  xunit <- intersect(colnames(lo_beban), colnames(unit))

  lo_beban_1 <-
    left_join(lo_beban, rek, xrek)

  lo_beban_2 <-
    left_join(lo_beban_1, unit, xunit)

  lo_beban_3 <-
    lo_beban_2 %>%
    select(
      kd_unit90,
      nm_unit,
      kd_rek90_1,
      kd_rek90_2,
      kd_rek90_3,
      kd_rek90_4,
      kd_rek90_5,
      kd_rek90_6,
      nm_rek90_1,
      nm_rek90_2,
      nm_rek90_3,
      nm_rek90_4,
      nm_rek90_5,
      nm_rek90_6,
      saldonorm,
      saldo_thn_lalu = saldo,
      debet,
      kredit,
      saldo_akhir
    )
  return(lo_beban_3)
}

tarik_sp2d_spm <- function(kon) {
  sp2d <- tbl(kon, "ta_sp2d")
  
  spm <- tbl(kon, "ta_spm")
  spm_r <- tbl(kon, "ta_spm_rinc")
  xspm <- intersect(colnames(spm), colnames(spm_r))
  spm_df <- left_join(spm, spm_r, xspm)
  
  bayar_df <- left_join(sp2d, spm_df, by = c("tahun", "no_spm"))
  
  map90 <- tbl(kon, "ref_rek_mapping") %>%
    select(kd_rek_1:kd_rek90_6)
  
  xmap90 <- intersect(colnames(bayar_df), colnames(map90))
  
  bayar_df2 <- left_join(bayar_df, map90, xmap90)
  
  bayar_df3 <-
    bayar_df2 %>%
    group_by(
      kd_urusan,
      kd_bidang,
      kd_unit,
      no_sp2d,
      tgl_sp2d,
      kd_rek90_1,
      kd_rek90_2,
      kd_rek90_3,
      kd_rek90_4,
      kd_rek90_5,
      kd_rek90_6,
      no_spm,
      tgl_spm,
      jn_spm,
      uraian,
      nm_penerima,
      bank_penerima,
      rek_penerima,
      npwp
    ) %>%
    summarise(nilai = sum(nilai, na.rm = TRUE),
              .groups = "drop") %>%
    collect()
  
  # menambahkan referensi akun rek90
  rek1 <- tbl(kon, "ref_rek90_1")
  rek2 <- tbl(kon, "ref_rek90_2")
  rek3 <- tbl(kon, "ref_rek90_3")
  rek4 <- tbl(kon, "ref_rek90_4")
  rek5 <- tbl(kon, "ref_rek90_5")
  rek6 <- tbl(kon, "ref_rek90_6")
  
  rek <- rek6 %>%
    left_join(rek5,
              by = c(
                "kd_rek90_1",
                "kd_rek90_2",
                "kd_rek90_3",
                "kd_rek90_4",
                "kd_rek90_5"
              )) %>%
    left_join(rek4,
              by = c("kd_rek90_1",
                     "kd_rek90_2",
                     "kd_rek90_3",
                     "kd_rek90_4")) %>%
    left_join(rek3, by = c("kd_rek90_1",
                           "kd_rek90_2",
                           "kd_rek90_3")) %>%
    left_join(rek2, by = c("kd_rek90_1",
                           "kd_rek90_2")) %>%
    left_join(rek1, by = c("kd_rek90_1"))
  
  rek <- rek %>%
    select(
      kd_rek90_1,
      kd_rek90_2,
      kd_rek90_3,
      kd_rek90_4,
      kd_rek90_5,
      kd_rek90_6,
      nm_rek90_1,
      nm_rek90_2,
      nm_rek90_3,
      nm_rek90_4,
      nm_rek90_5,
      nm_rek90_6
    ) %>%
    collect()
  
  xrek <- intersect(colnames(bayar_df3), colnames(rek))
  
  unit <- tbl(kon, "ref_unit") %>% collect()
  xunit <- intersect(colnames(bayar_df3), colnames(unit))
  
  jns_spm <- tbl(kon, "ref_jenis_spm") %>% collect()
  
  bayar_df4 <-
    bayar_df3 %>%
    left_join(rek, xrek) %>%
    left_join(unit, xunit) %>%
    left_join(jns_spm, "jn_spm") %>%
    select(
      kd_unit90,
      nm_unit,
      no_sp2d,
      tgl_sp2d,
      kd_rek90_1,
      kd_rek90_2,
      kd_rek90_3,
      kd_rek90_4,
      kd_rek90_5,
      kd_rek90_6,
      nm_rek90_1,
      nm_rek90_2,
      nm_rek90_3,
      nm_rek90_4,
      nm_rek90_5,
      nm_rek90_6,
      no_spm,
      tgl_spm,
      nm_jn_spm,
      uraian,
      nm_penerima,
      bank_penerima,
      rek_penerima,
      npwp,
      nilai
    ) %>% 
    mutate(rek_penerima = str_remove_all(rek_penerima, "[:punct:]"))
  
  return(bayar_df4)
  
}

tarik_sts <- function(kon) {
  
  sts <- tbl(kon, "ta_sts")
  sts_r <- tbl(kon, "ta_sts_rinc")
  xsts <- intersect(colnames(sts), colnames(sts_r))
  sts_df <- left_join(sts, sts_r, xsts)
  
  map90 <- tbl(kon, "ref_rek_mapping") %>%
    select(kd_rek_1:kd_rek90_6)
  
  xmap90 <- intersect(colnames(sts_df), colnames(map90))
  
  sts_df2 <- left_join(sts_df, map90, xmap90)
  
  sts_df3 <-
    sts_df2 %>%
    filter(!is.na(nilai)) %>% 
    group_by(
      kd_urusan,
      kd_bidang,
      kd_unit,
      no_sts,
      tgl_sts,
      kd_rek90_1,
      kd_rek90_2,
      kd_rek90_3,
      kd_rek90_4,
      kd_rek90_5,
      kd_rek90_6,
      keterangan
    ) %>%
    summarise(nilai = sum(nilai, na.rm = TRUE),
              .groups = "drop") %>%
    collect()
  
  # menambahkan referensi akun rek90
  rek1 <- tbl(kon, "ref_rek90_1")
  rek2 <- tbl(kon, "ref_rek90_2")
  rek3 <- tbl(kon, "ref_rek90_3")
  rek4 <- tbl(kon, "ref_rek90_4")
  rek5 <- tbl(kon, "ref_rek90_5")
  rek6 <- tbl(kon, "ref_rek90_6")
  
  rek <- rek6 %>%
    left_join(rek5,
              by = c(
                "kd_rek90_1",
                "kd_rek90_2",
                "kd_rek90_3",
                "kd_rek90_4",
                "kd_rek90_5"
              )) %>%
    left_join(rek4,
              by = c("kd_rek90_1",
                     "kd_rek90_2",
                     "kd_rek90_3",
                     "kd_rek90_4")) %>%
    left_join(rek3, by = c("kd_rek90_1",
                           "kd_rek90_2",
                           "kd_rek90_3")) %>%
    left_join(rek2, by = c("kd_rek90_1",
                           "kd_rek90_2")) %>%
    left_join(rek1, by = c("kd_rek90_1"))
  
  rek <- rek %>%
    select(
      kd_rek90_1,
      kd_rek90_2,
      kd_rek90_3,
      kd_rek90_4,
      kd_rek90_5,
      kd_rek90_6,
      nm_rek90_1,
      nm_rek90_2,
      nm_rek90_3,
      nm_rek90_4,
      nm_rek90_5,
      nm_rek90_6
    ) %>%
    collect()
  
  xrek <- intersect(colnames(sts_df3), colnames(rek))
  
  unit <- tbl(kon, "ref_unit") %>% collect()
  xunit <- intersect(colnames(sts_df3), colnames(unit))
  
  sts_df4 <-
    sts_df3 %>%
    left_join(rek, xrek) %>%
    left_join(unit, xunit) %>%
    select(
      kd_unit90,
      nm_unit,
      no_sts,
      tgl_sts,
      kd_rek90_1,
      kd_rek90_2,
      kd_rek90_3,
      kd_rek90_4,
      kd_rek90_5,
      kd_rek90_6,
      nm_rek90_1,
      nm_rek90_2,
      nm_rek90_3,
      nm_rek90_4,
      nm_rek90_5,
      nm_rek90_6,
      keterangan,
      nilai
    )
  
  return(sts_df4)
  
}

bayar_kontrak <- function(kon) {
  
  spp_kontrak <- tbl(kon, "ta_spp_kontrak") 
  spp_kontrak <- spp_kontrak %>% select(tahun, no_spp, no_kontrak)
  spp_rinc <- tbl(kon, "ta_spp_rinc")
  spp <- tbl(kon, "ta_spp")
  
  spp_kontrak_c <-
    spp_kontrak %>%
    left_join(spp_rinc, by = c("tahun", "no_spp")) %>%
    left_join(spp,
              by = c(
                "tahun",
                "no_spp",
                "kd_urusan",
                "kd_bidang",
                "kd_unit",
                "kd_sub"
              ))
  
  map90 <- tbl(kon, "ref_rek_mapping") %>%
    select(kd_rek_1:kd_rek90_6)
  
  xmap90 <- intersect(colnames(spp_kontrak_c), colnames(map90))
  
  spp_kontrak_d <-
    spp_kontrak_c %>%
    left_join(map90, xmap90) %>%
    collect()
  
  # menambahkan referensi akun rek90
  rek1 <- tbl(kon, "ref_rek90_1")
  rek2 <- tbl(kon, "ref_rek90_2")
  rek3 <- tbl(kon, "ref_rek90_3")
  rek4 <- tbl(kon, "ref_rek90_4")
  rek5 <- tbl(kon, "ref_rek90_5")
  rek6 <- tbl(kon, "ref_rek90_6")
  
  rek <- rek6 %>%
    left_join(rek5,
              by = c(
                "kd_rek90_1",
                "kd_rek90_2",
                "kd_rek90_3",
                "kd_rek90_4",
                "kd_rek90_5"
              )) %>%
    left_join(rek4,
              by = c("kd_rek90_1",
                     "kd_rek90_2",
                     "kd_rek90_3",
                     "kd_rek90_4")) %>%
    left_join(rek3, by = c("kd_rek90_1",
                           "kd_rek90_2",
                           "kd_rek90_3")) %>%
    left_join(rek2, by = c("kd_rek90_1",
                           "kd_rek90_2")) %>%
    left_join(rek1, by = c("kd_rek90_1"))
  
  rek <- rek %>%
    select(
      kd_rek90_1,
      kd_rek90_2,
      kd_rek90_3,
      kd_rek90_4,
      kd_rek90_5,
      kd_rek90_6,
      nm_rek90_1,
      nm_rek90_2,
      nm_rek90_3,
      nm_rek90_4,
      nm_rek90_5,
      nm_rek90_6
    ) %>%
    collect()
  
  xrek <- intersect(colnames(spp_kontrak_d), colnames(rek))
  
  unit <- tbl(kon, "ref_unit") %>% collect()
  xunit <- intersect(colnames(spp_kontrak_d), colnames(unit))
  
  tagihan <- tbl(kon, "ref_jenis_tagihan") %>% collect()
  
  spp_kontrak_e <-
    spp_kontrak_d %>%
    left_join(rek, xrek) %>%
    left_join(unit, xunit) %>%
    left_join(tagihan, "jns_tagihan") %>%
    select(
      kd_unit90,
      nm_unit,
      kd_rek90_1,
      kd_rek90_2,
      kd_rek90_3,
      kd_rek90_4,
      kd_rek90_5,
      kd_rek90_6,
      nm_rek90_1,
      nm_rek90_2,
      nm_rek90_3,
      nm_rek90_4,
      nm_rek90_5,
      nm_rek90_6,
      no_kontrak,
      no_spp,
      tgl_spp,
      uraian,
      no_spj,
      no_tagihan,
      tgl_tagihan,
      nm_tagihan,
      realisasi_fisik,
      nm_penerima,
      alamat_penerima,
      bank_penerima,
      rek_penerima,
      npwp,
      nilai
    ) %>% 
    mutate(rek_penerima = str_remove_all(rek_penerima, "[:punct:]"))
  
  return(spp_kontrak_e)
  
}

daftar_kontrak <- function(kon) {
  
  kontrak <- tbl(kon, "ta_kontrak")
  kontrak_add <- tbl(kon, "ta_kontrak_addendum")
  
  kontrak_a <-
    left_join(kontrak, kontrak_add, by = c("tahun", "no_kontrak")) %>%
    collect() %>%
    mutate(
      keperluan = if_else(is.na(keperluan.y), keperluan.x, keperluan.y),
      waktu = if_else(is.na(waktu.y), waktu.x, waktu.y),
      nilai = if_else(is.na(nilai.y), nilai.x, nilai.y)
    ) %>%
    select(
      tahun,
      kd_urusan,
      kd_bidang,
      kd_unit,
      no_kontrak,
      tgl_kontrak,
      no_addendum,
      tgl_addendum,
      keperluan,
      waktu,
      nilai,
      nm_perusahaan,
      bentuk,
      alamat,
      nm_pemilik,
      npwp,
      nm_bank,
      nm_rekening,
      no_rekening
    )
  
  unit <- tbl(kon, "ref_unit") %>% collect()
  xunit <- intersect(colnames(kontrak_a), colnames(unit))
  
  kontrak_b <-
    kontrak_a %>%
    left_join(unit, xunit) %>%
    select(
      kd_unit90,
           nm_unit,
           no_kontrak:waktu,
           nm_perusahaan:no_rekening,
           nilai
      ) %>% 
    mutate(no_rekening = str_remove_all(no_rekening, "[:punct:]"))
  
  return(kontrak_b)
}



tarik_jurnal <- function(kon) {
  pemda <- tbl(kon, "ta_pemda") %>%
    arrange(-tahun) %>%
    head(1) %>%
    collect()
  
  ta <- pemda %>%
    select(tahun) %>%
    pull()
  
  entitas <- pemda %>%
    select(nm_pemda) %>%
    pull() %>%
    word(start = 2, end = -1) %>%
    str_to_lower()
  
  jrn_nrc <- tbl(kon, "ta_jurnalsemuaak")
  jrn_nrc_r <- tbl(kon, "ta_jurnalsemuaak_rinc")
  xjrn <- intersect(colnames(jrn_nrc), colnames(jrn_nrc_r))
  jrn_nrc_i <-
    left_join(jrn_nrc, jrn_nrc_r, xjrn) %>%
    filter(tahun == ta) %>%
    collect()
  
  # menambahkan referensi akun rek90
  rek1 <- tbl(kon, "ref_rek90_1")
  rek2 <- tbl(kon, "ref_rek90_2")
  rek3 <- tbl(kon, "ref_rek90_3")
  rek4 <- tbl(kon, "ref_rek90_4")
  rek5 <- tbl(kon, "ref_rek90_5")
  rek6 <- tbl(kon, "ref_rek90_6")
  
  rek <- rek6 %>%
    left_join(rek5,
              by = c(
                "kd_rek90_1",
                "kd_rek90_2",
                "kd_rek90_3",
                "kd_rek90_4",
                "kd_rek90_5"
              )) %>%
    left_join(rek4,
              by = c("kd_rek90_1",
                     "kd_rek90_2",
                     "kd_rek90_3",
                     "kd_rek90_4")) %>%
    left_join(rek3, by = c("kd_rek90_1",
                           "kd_rek90_2",
                           "kd_rek90_3")) %>%
    left_join(rek2, by = c("kd_rek90_1",
                           "kd_rek90_2")) %>%
    left_join(rek1, by = c("kd_rek90_1"))
  
  rek <- rek %>%
    select(
      kd_rek90_1,
      kd_rek90_2,
      kd_rek90_3,
      kd_rek90_4,
      kd_rek90_5,
      kd_rek90_6,
      nm_rek90_1,
      nm_rek90_2,
      nm_rek90_3,
      nm_rek90_4,
      nm_rek90_5,
      nm_rek90_6,
      saldonorm
    ) %>%
    collect()
  
  xrek <- intersect(colnames(jrn_nrc_i), colnames(rek))
  
  unit <- tbl(kon, "ref_unit") %>% collect()
  xunit <- intersect(colnames(jrn_nrc_i), colnames(unit))
  
  jurnal <- tbl(kon, "ref_jurnal") %>% collect()
  
  jurnal_transaksi <-
    jrn_nrc_i %>%
    left_join(unit, xunit) %>%
    left_join(jurnal, "kd_jurnal") %>%
    left_join(rek, xrek) %>%
    left_join(
      rek,
      by = c(
        "kd_lwn_1" = "kd_rek90_1",
        "kd_lwn_2" = "kd_rek90_2",
        "kd_lwn_3" = "kd_rek90_3",
        "kd_lwn_4" = "kd_rek90_4",
        "kd_lwn_5" = "kd_rek90_5",
        "kd_lwn_6" = "kd_rek90_6"
      )
    ) %>%
    select(
      tahun,
      kd_unit90,
      nm_unit,
      no_bukti,
      tgl_bukti,
      no_bku,
      keterangan,
      nm_jurnal,
      kd_rek90_1,
      kd_rek90_2,
      kd_rek90_3,
      kd_rek90_4,
      kd_rek90_5,
      kd_rek90_6,
      nm_rek90_1 = nm_rek90_1.x,
      nm_rek90_2 = nm_rek90_2.x,
      nm_rek90_3 = nm_rek90_3.x,
      nm_rek90_4 = nm_rek90_4.x,
      nm_rek90_5 = nm_rek90_5.x,
      nm_rek90_6 = nm_rek90_6.x,
      normal.x = saldonorm.x,
      debet,
      kredit,
      kd_lwn_1,
      kd_lwn_2,
      kd_lwn_3,
      kd_lwn_4,
      kd_lwn_5,
      kd_lwn_6,
      nm_lwn_1 = nm_rek90_1.y,
      nm_lwn_2 = nm_rek90_2.y,
      nm_lwn_3 = nm_rek90_3.y,
      nm_lwn_4 = nm_rek90_4.y,
      nm_lwn_5 = nm_rek90_5.y,
      nm_lwn_6 = nm_rek90_6.y,
      normal.y = saldonorm.y
    )
  
  return(jurnal_transaksi)
}


# SIMPAN DATA
simpan_data_simda <- function(kon) {
  pemda <- tbl(kon, "ta_pemda") %>%
    arrange(-tahun) %>%
    head(1) %>%
    collect()
  
  ta <- pemda %>%
    select(tahun) %>%
    pull()
  
  entitas <- pemda %>%
    select(nm_pemda) %>%
    pull() %>%
    word(start = 2, end = -1) %>%
    str_to_lower()
  
  tempat_simpan <-
    entitas %>%
    str_replace_all(" ", "_")
  
  fs::dir_create(tempat_simpan)
  
  tarik_saldo_nrc_aset(kon) %>%
    mutate(entitas = entitas) %>%
    write_csv(., glue("{tempat_simpan}/nrc_aset.csv"), append = FALSE)
  
  tarik_saldo_nrc_ekuitas(kon) %>%
    mutate(entitas = entitas) %>%
    write_csv(., glue("{tempat_simpan}/nrc_ekuitas.csv"), append = FALSE)
  
  tarik_saldo_nrc_kewajiban(kon) %>%
    mutate(entitas = entitas) %>%
    write_csv(., glue("{tempat_simpan}/nrc_kewajiban.csv"), append = FALSE)
  
  tarik_saldo_lra_pendapatan(kon) %>%
    mutate(entitas = entitas) %>%
    write_csv(., glue("{tempat_simpan}/lra_pendapatan.csv"), append = FALSE)
  
  tarik_saldo_lra_belanja(kon) %>%
    mutate(entitas = entitas) %>%
    write_csv(., glue("{tempat_simpan}/lra_belanja.csv"), append = FALSE)
  
  tarik_saldo_lra_terima_biaya(kon) %>%
    mutate(entitas = entitas) %>%
    write_csv(., glue("{tempat_simpan}/lra_biaya_terima.csv"), append = FALSE)
  
  tarik_saldo_lra_keluar_biaya(kon) %>%
    mutate(entitas = entitas) %>%
    write_csv(., glue("{tempat_simpan}/lra_biaya_keluar.csv"), append = FALSE)
  
  tarik_saldo_lra_silpa(kon) %>%
    mutate(entitas = entitas) %>%
    write_csv(., glue("{tempat_simpan}/lra_silpa.csv"), append = FALSE)
  
  tarik_saldo_lo_pendapatan(kon) %>%
    mutate(entitas = entitas) %>%
    write_csv(., glue("{tempat_simpan}/lo_pendapatan.csv"), append = FALSE)
  
  tarik_saldo_lo_beban(kon) %>%
    mutate(entitas = entitas) %>%
    write_csv(., glue("{tempat_simpan}/lo_beban.csv"), append = FALSE)
  
  tarik_saldo_lo_surplus(kon) %>%
    mutate(entitas = entitas) %>%
    write_csv(., glue("{tempat_simpan}/lo_surplus.csv"), append = FALSE)
  
  tarik_saldo_lo_defisit(kon) %>%
    mutate(entitas = entitas) %>%
    write_csv(., glue("{tempat_simpan}/lo_defisit.csv"), append = FALSE)
  
  tarik_jurnal(kon) %>% 
    mutate(entitas = entitas) %>% 
    write_csv(., glue("{tempat_simpan}/jurnal.csv"), append = FALSE)
  
  tarik_sp2d_spm(kon) %>%
    mutate(entitas = entitas) %>%
    write_csv(., glue("{tempat_simpan}/sp2d.csv"), append = FALSE)
  
  tarik_sts(kon) %>%
    mutate(entitas = entitas) %>%
    write_csv(., glue("{tempat_simpan}/sts.csv"), append = FALSE)
  
  bayar_kontrak(kon) %>%
    mutate(entitas = entitas) %>%
    write_csv(., glue("{tempat_simpan}/kontrak_bayar.csv"), append = FALSE)
  
  daftar_kontrak(kon) %>%
    mutate(entitas = entitas) %>%
    write_csv(., glue("{tempat_simpan}/kontrak_daftar.csv"), append = FALSE)
  
  return(glue("semua {entitas} sudah disimpan"))
  
}


