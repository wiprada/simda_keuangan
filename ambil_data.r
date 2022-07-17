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
