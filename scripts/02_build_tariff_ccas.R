# library(readxl)
library(readr)
library(qs)
library(DBI)
library(tidyr)
library(dplyr)
library(tibble)
library(countrycode)
rm(list=ls())


### Merge TC data into the flows and tariffs data ###
#----------------------------------------------------
setwd("~/models_v2/transportcanada/clean_data_app/cca")
flows <- qs::qread("allflows_forCCAs_2022.qs")
eu_countries <- c("Austria","Belgium","Bulgaria","Croatia","Cyprus","Czechia","Denmark","Estonia","Finland","France","Germany","Greece","Hungary","Ireland","Italy","Latvia","Lithuania","Luxembourg","Malta","Netherlands (Kingdom of the)","Poland","Portugal","Romania","Slovak Republic","Slovenia","Spain","Sweden")

can_exports <- flows %>%
  subset(., exporter == "Canada") %>%
  mutate(hs4 = substr(hs6,1,4)) %>%
  subset(.,hs4 != "9999") %>%
  group_by(hs4) %>%
  mutate(total_fob_hs4 = sum(fob, na.rm = TRUE)) %>%
  ungroup() %>%
  mutate(total_fob = sum(fob, na.rm = TRUE)) %>%
  arrange(desc(total_fob_hs4)) %>%
  group_by(hs4) %>%
  distinct(hs4, total_fob_hs4, total_fob) %>%
  ungroup() %>%
  slice_head(n = 30) 

top30_hs4_exports <- as.vector(can_exports$hs4)

top_hs6_exports <- flows %>%
  mutate(hs4 = substr(hs6,1,4)) %>%
  subset(hs4 %in% top30_hs4_exports) %>%
  select(hs6) %>%
  group_by(hs6) %>%
  unique()

can_imports <- flows %>%
  subset(., importer == "Canada") %>%
  mutate(hs4 = substr(hs6,1,4)) %>%
  subset(.,hs4 != "9999") %>%
  group_by(hs4) %>%
  mutate(total_fob_hs4 = sum(fob, na.rm = TRUE)) %>%
  ungroup() %>%
  mutate(total_fob = sum(fob, na.rm = TRUE)) %>%
  arrange(desc(total_fob_hs4)) %>%
  group_by(hs4) %>%
  distinct(hs4, total_fob_hs4, total_fob) %>%
  ungroup() %>%
  slice_head(n = 30) 

top30_hs4_imports <- as.vector(can_imports$hs4)

top_hs6_imports <- flows %>%
  mutate(hs4 = substr(hs6,1,4)) %>%
  subset(hs4 %in% top30_hs4_imports) %>%
  select(hs6) %>%
  group_by(hs6) %>%
  unique()

flows <- flows %>% 
  mutate(cif = fob + freight)



### Calculate value perkg and cost perkg with freight costs (currently expressed as (CIF-FOB)/CIF) ###
#-----------------------------------------------------------------------------------------------------
# Flows with top exported products
exp_flows <- flows %>% 
  subset(.,hs6 %in% top_hs6_exports$hs6) %>% 
  subset(., !is.na(freight)) %>% 
  mutate(fob_perkg = fob/kg,
         freight_perkg = freight/kg,
         cif_perkg = cif/kg) %>% 
  subset(.,fob_perkg > 0) %>% 
  group_by(importer, year, hs6) %>%
  subset(.,cif_perkg > 0 & fob_perkg > 0) %>% 
  mutate(tau = tau/100) %>% 
  ungroup() %>% 
  dplyr::select(exporter,importer,hs6,year,fob_perkg,freight_perkg,cif_perkg,tau) %>% 
  subset(., exporter != importer) 


# Flows with top imported products
imp_flows <- flows %>% 
  subset(.,hs6 %in% top_hs6_imports$hs6) %>% 
  subset(., !is.na(freight)) %>% 
  mutate(fob_perkg = fob/kg,
         freight_perkg = freight/kg,
         cif_perkg = cif/kg) %>% 
  subset(.,fob_perkg > 0) %>% 
  group_by(importer, year, hs6) %>%
  subset(.,cif_perkg > 0 & fob_perkg > 0) %>% 
  mutate(tau = tau/100) %>% 
  ungroup() %>% 
  dplyr::select(exporter,importer,hs6,year,fob_perkg,freight_perkg,cif_perkg,tau) %>% 
  subset(., exporter != importer) 


# Trades not involving Canada (Can will become new exporter here)
nocan_deni <- exp_flows %>%
  dplyr::select(exporter,importer,hs6,year,fob_perkg,freight_perkg,cif_perkg,tau) %>% 
  subset(., exporter != "Canada" & importer != "Canada") %>%
  rename(fob_perkg_deni = fob_perkg, freight_perkg_deni = freight_perkg, cif_perkg_deni = cif_perkg, dropped_exporter = exporter,new_importer = importer,tau_deni = tau) 

# Canada as exporter (this serves as ne -> di with Can as ne)
can_export <- exp_flows %>% 
  subset(., exporter == "Canada") %>%
  dplyr::select(exporter,importer,hs6, year, fob_perkg, freight_perkg, cif_perkg,tau) %>% 
  rename(fob_perkg_nedi = fob_perkg, freight_perkg_nedi = freight_perkg, cif_perkg_nedi = cif_perkg,new_exporter = exporter, dropped_importer = importer,tau_nedi = tau)


# Unique freight costs when Canada is exporter
canada_exportfreight_neni <- can_export %>% 
  dplyr::select(new_exporter, dropped_importer, hs6, year, freight_perkg_nedi,tau_nedi) %>% 
  rename(new_importer = dropped_importer,freight_perkg_neni = freight_perkg_nedi,tau_neni = tau_nedi)


### Merge the combinations when Canada is the new exporter and when Canada isnt the dropped exporter ###
merged_export_combos <- merge(nocan_deni, can_export, by = c("hs6","year"), all = TRUE) %>% 
  left_join(canada_exportfreight_neni, by = c("new_importer", "hs6", "new_exporter","year")) %>% 
  mutate(tau_neni = ifelse(new_exporter %in% eu_countries & new_importer %in% eu_countries,0,tau_neni)) %>%
  subset(.,!is.na(freight_perkg_neni) & !is.na(tau_neni)) %>%
  mutate(cca_notau = cif_perkg_deni - freight_perkg_neni - fob_perkg_nedi) %>% 
  mutate(cca_tau = (cif_perkg_deni * (1 + tau_deni))/(1 + tau_neni) - freight_perkg_neni - (fob_perkg_nedi / (1 + tau_nedi))) %>%
  dplyr::select(new_exporter, dropped_importer,dropped_exporter, new_importer,hs6,year,cca_notau,cca_tau) %>% 
  mutate(hs4 = substr(hs6,1,4)) %>% 
  subset(., !is.na(cca_tau) & !is.na(cca_notau))



# Trades not involving Canada (Can will become new importer here)
nocan_nedi <- imp_flows %>%
  dplyr::select(exporter,importer,hs6,year,fob_perkg,freight_perkg,cif_perkg,tau) %>%
  subset(., exporter != "Canada" & importer != "Canada") %>%
  rename(fob_perkg_nedi = fob_perkg, freight_perkg_nedi = freight_perkg, cif_perkg_nedi = cif_perkg, dropped_importer = importer,new_exporter = exporter,tau_nedi = tau)


# Canada as importer (this serves as de -> ni with Can as ni)
can_import <- imp_flows %>%
  dplyr::select(exporter,importer,hs6,year,fob_perkg,freight_perkg,cif_perkg,tau) %>%
  subset(., importer == "Canada") %>%
  rename(fob_perkg_deni = fob_perkg, freight_perkg_deni = freight_perkg, cif_perkg_deni = cif_perkg, dropped_exporter = exporter,new_importer = importer,tau_deni = tau)


# Unique freight costs when canada is importer
canada_importfreight_neni <- can_import %>%
  dplyr::select(dropped_exporter, new_importer, hs6, year, freight_perkg_deni,tau_deni) %>%
  rename(new_exporter = dropped_exporter,freight_perkg_neni = freight_perkg_deni,tau_neni = tau_deni)


### Merge the combinations when Canada is the new importer and when Canada isnt the dropped importer ###
merged_import_combos <- merge(nocan_nedi, can_import, by = c("hs6","year"), all = TRUE) %>%
  left_join(canada_importfreight_neni, by = c("new_importer", "hs6", "new_exporter","year")) %>%
  mutate(tau_neni = ifelse(new_exporter %in% eu_countries & new_importer %in% eu_countries,0,tau_neni)) %>%
  subset(.,!is.na(freight_perkg_neni) & !is.na(tau_neni)) %>%
  mutate(cca_notau = cif_perkg_deni - freight_perkg_neni - fob_perkg_nedi) %>%
  mutate(cca_tau = (cif_perkg_deni * (1 + tau_deni))/(1 + tau_neni) - freight_perkg_neni - (fob_perkg_nedi / (1 + tau_nedi))) %>%
  dplyr::select(new_exporter, dropped_importer,dropped_exporter, new_importer,hs6,year,cca_notau,cca_tau) %>%
  mutate(hs4 = substr(hs6,1,4)) %>% 
  subset(., !is.na(cca_tau) & !is.na(cca_notau))

# Combine both sets of combinations
merged_combos <- bind_rows(merged_export_combos, merged_import_combos)


# Find thresholds of observations to keep by taking the 1% of the maximum flow value by exporter-importer-product-year pair
thresholds <- flows %>%
  mutate(hs4 = substr(hs6,1,4)) %>% 
  dplyr::select(exporter, importer, hs4, year, fob) %>%
  group_by(exporter, importer, hs4, year) %>%
  summarise(fob = sum(fob, na.rm = TRUE)) %>%
  subset(.,fob > 0 & !is.na(fob)) %>%
  group_by(importer, hs4, year) %>%
  mutate(maximum_flow = max(fob, na.rm = TRUE)) %>%
  mutate(threshold = 0.10 * maximum_flow) %>%
  ungroup() %>%
  dplyr::select(exporter, importer, hs4, year, fob, threshold)


# Apply thresholds to trades between the dropped exporter and new importer
merged_combos <- merged_combos %>%
  left_join(thresholds, by = c("dropped_exporter" = "exporter",
                               "new_importer" = "importer",
                               "hs4" = "hs4",
                               "year" = "year")) %>%
  subset(.,fob > threshold & dropped_exporter != "Canada" | dropped_exporter == "Rest of World" | dropped_importer == "Rest of World") %>%
  dplyr::select(-fob, -threshold)



merged_combosnew <- merged_combos %>%
  subset(., dropped_importer != new_importer)



hs6_descriptions <- flows %>%
  select(hs6, hs6_desc) %>%
  unique() %>%
  distinct(hs6,.keep_all = T)

merged_combosnew <- merged_combosnew %>%
  left_join(hs6_descriptions, by = c("hs6" = "hs6"))


ordinal_en <- function(n) {
  suf <- ifelse(n %% 100L %in% 11:13, "th",
                ifelse(n %% 10L == 1L, "st",
                       ifelse(n %% 10L == 2L, "nd",
                              ifelse(n %% 10L == 3L, "rd", "th"))))
  paste0(n, suf)
}


can_exports <- subset(flows,exporter == "Canada") %>%
  select(hs6,year,fob) %>%
  group_by(hs6,year) %>%
  summarise(can_export_fob = sum(fob, na.rm = TRUE)) %>%
  ungroup() %>%
  group_by(year) %>%
  # Rank HS codes
  arrange(desc(can_export_fob)) %>%
  mutate(rank = row_number()) %>%
  ungroup() %>%
  # Give suffix "most exported after ranking" for example "Most exported product", "2nd most exported product",
  mutate(
    rank_suffix = if_else(
      rank == 1,
      "Most exported HS6 product",
      paste0(ordinal_en(rank), " most exported HS6 product")
    )
  ) %>%
  mutate(year = as.numeric(year))


can_imports <- subset(flows,importer == "Canada") %>%
  select(hs6,year,fob) %>%
  group_by(hs6,year) %>%
  summarise(can_import_fob = sum(fob, na.rm = TRUE)) %>%
  ungroup() %>%
  group_by(year) %>%
  # Rank HS codes
  arrange(desc(can_import_fob)) %>%
  mutate(rank = row_number()) %>%
  ungroup() %>%
  # Give suffix "most exported after ranking" for example "Most exported product", "2nd most exported product",
  mutate(
    rank_suffix = if_else(
      rank == 1,
      "Most exported HS6 product",
      paste0(ordinal_en(rank), " most exported HS6 product")
    )
  ) %>%
  mutate(year = as.numeric(year))


merged_export_combos <- merged_combosnew %>%
  subset(.,new_exporter == "Canada") %>% 
  left_join(can_exports %>% select(hs6, year, rank, rank_suffix), by = c("hs6" = "hs6", "year" = "year")) %>%
  rename(hs6_rank = rank, hs6_rank_suffix = rank_suffix, cca_hs6_notau = cca_notau, cca_hs6_tau = cca_tau) %>%
  select(new_exporter, dropped_importer, dropped_exporter, new_importer,year,hs6,hs6_desc,hs6_rank,hs6_rank_suffix,cca_hs6_notau,cca_hs6_tau)

merged_import_combos <- merged_combosnew %>%
  subset(.,new_importer == "Canada") %>% 
  left_join(can_imports %>% select(hs6, year, rank, rank_suffix), by = c("hs6" = "hs6", "year" = "year")) %>%
  rename(hs6_rank = rank, hs6_rank_suffix = rank_suffix, cca_hs6_notau = cca_notau, cca_hs6_tau = cca_tau) %>%
  select(new_exporter, dropped_importer, dropped_exporter, new_importer,year,hs6,hs6_desc,hs6_rank,hs6_rank_suffix,cca_hs6_notau,cca_hs6_tau)



### Now repeat for HS4's ###
exp_flows_hs4 <- flows %>%
  subset(.,hs6 %in% top_hs6_exports$hs6) %>% 
  mutate(hs4 = substr(hs6, 1, 4)) %>% 
  mutate(tau_weights = tau * cif) %>%
  group_by(exporter, importer, year, hs4) %>%
  mutate(fob_hs4 = sum(fob, na.rm = TRUE),
         cif_hs4 = sum(cif, na.rm = TRUE),
         freight_hs4 = sum(freight, na.rm = T),
         kg_hs4 = sum(kg, na.rm = TRUE),
         tau_sum = sum(tau_weights, na.rm = TRUE)) %>%
  mutate(tau_hs4 = tau_sum / fob_hs4) %>%
  select(exporter, importer, year, hs4,fob_hs4,kg_hs4, freight_hs4,cif_hs4,tau_hs4) %>%
  unique() %>%
  ungroup() %>%
  mutate(cif_perkg = cif_hs4 / kg_hs4,
         freight_perkg = freight_hs4 / kg_hs4,
         fob_perkg = fob_hs4 / kg_hs4) 


exp_flows2_hs4 <- exp_flows_hs4 %>%
  # left_join(tc, by = c("importer","exporter","year","hs4")) %>%
  subset(., !is.na(freight_perkg)) %>%
  # mutate(fob_perkg = cif_perkg - freight_perkg) %>%
  subset(.,fob_perkg > 0) %>%
  group_by(importer, year, hs4) %>%
  subset(.,cif_perkg > 0 & fob_perkg > 0) %>%
  rename(fob = fob_hs4) %>%
  ungroup()


# HS4 imports 
imp_flows_hs4 <- flows %>%
  subset(.,hs6 %in% top_hs6_imports$hs6) %>% 
  mutate(hs4 = substr(hs6, 1, 4)) %>% 
  mutate(tau_weights = tau * cif) %>%
  group_by(exporter, importer, year, hs4) %>%
  mutate(fob_hs4 = sum(fob, na.rm = TRUE),
         cif_hs4 = sum(cif, na.rm = TRUE),
         freight_hs4 = sum(freight, na.rm = T),
         kg_hs4 = sum(kg, na.rm = TRUE),
         tau_sum = sum(tau_weights, na.rm = TRUE)) %>%
  mutate(tau_hs4 = tau_sum / fob_hs4) %>%
  select(exporter, importer, year, hs4,fob_hs4,kg_hs4, freight_hs4,cif_hs4,tau_hs4) %>%
  unique() %>%
  ungroup() %>%
  mutate(cif_perkg = cif_hs4 / kg_hs4,
         freight_perkg = freight_hs4 / kg_hs4,
         fob_perkg = fob_hs4 / kg_hs4)


imp_flows2_hs4 <- imp_flows_hs4 %>%
  # left_join(tc, by = c("importer","exporter","year","hs4")) %>%
  subset(., !is.na(freight_perkg)) %>%
  # mutate(fob_perkg = cif_perkg - freight_perkg) %>%
  subset(.,fob_perkg > 0) %>%
  group_by(importer, year, hs4) %>%
  subset(.,cif_perkg > 0 & fob_perkg > 0) %>%
  rename(fob = fob_hs4) %>%
  ungroup()



### Unique trade combos where Canada is not the exporter ###
noncan_export_combos4 <- exp_flows2_hs4 %>%
  dplyr::select(exporter,importer,hs4,year,fob_perkg,freight_perkg,cif_perkg,tau_hs4) %>%
  subset(., exporter != "Canada" & importer != "Canada") %>%
  rename(fob_perkg_deni = fob_perkg, freight_perkg_deni = freight_perkg, cif_perkg_deni = cif_perkg, dropped_exporter = exporter,new_importer = importer,tau_hs4_deni = tau_hs4)



### Unique trade combos where Canada is the exporter ###
can_export_combos4 <- exp_flows2_hs4 %>%
  subset(., exporter == "Canada") %>%
  dplyr::select(exporter,importer,hs4, year, fob_perkg, freight_perkg, cif_perkg,tau_hs4) %>%
  rename(fob_perkg_nedi = fob_perkg, freight_perkg_nedi = freight_perkg, cif_perkg_nedi = cif_perkg,new_exporter = exporter, dropped_importer = importer,tau_hs4_nedi = tau_hs4)



### Unique freight costs when Canada is exporter ###
canada_freight_combos4 <- can_export_combos4 %>%
  dplyr::select(new_exporter, dropped_importer, hs4, year, freight_perkg_nedi,tau_hs4_nedi) %>%
  rename(new_importer = dropped_importer,freight_perkg_neni = freight_perkg_nedi,tau_hs4_neni = tau_hs4_nedi)



### Merge the combinations when canada is the new exporter and when Canada isnt the dropped exporter ###
merged_export_combos4 <- merge(noncan_export_combos4, can_export_combos4, by = c("hs4","year"), all = TRUE) %>%
  left_join(canada_freight_combos4, by = c("new_importer", "hs4", "new_exporter","year")) %>%
  mutate(cca_notau = cif_perkg_deni - freight_perkg_neni - fob_perkg_nedi) %>%
  mutate(cca_tau = (cif_perkg_deni * (1 + tau_hs4_deni))/(1 + tau_hs4_neni) - freight_perkg_neni - (fob_perkg_nedi / (1 + tau_hs4_nedi))) %>%
  select(new_exporter, dropped_importer, dropped_exporter, new_importer,hs4,year,cca_notau,cca_tau) %>% 
  subset(., !is.na(cca_tau) & !is.na(cca_notau)) %>% 
  group_by(new_exporter, dropped_importer, dropped_exporter, new_importer,hs4,year) %>% 
  unique()



noncan_import_combos4 <- imp_flows2_hs4 %>%
  dplyr::select(exporter,importer,hs4,year,fob_perkg,freight_perkg,cif_perkg,tau_hs4) %>%
  subset(., exporter != "Canada" & importer != "Canada") %>%
  rename(fob_perkg_nedi = fob_perkg, freight_perkg_nedi = freight_perkg, cif_perkg_nedi = cif_perkg, dropped_importer = importer,new_exporter = exporter,tau_hs4_nedi = tau_hs4)



### Unique trade combos where Canada is the exporter ###
can_import_combos4 <- imp_flows2_hs4 %>%
  subset(., importer == "Canada") %>%
  dplyr::select(exporter,importer,hs4, year, fob_perkg, freight_perkg, cif_perkg,tau_hs4) %>%
  rename(fob_perkg_deni = fob_perkg, freight_perkg_deni = freight_perkg, cif_perkg_deni = cif_perkg,new_importer = importer, dropped_exporter = exporter,tau_hs4_deni = tau_hs4)



### Unique freight costs when Canada is exporter ###
canada_freight_combos4 <- can_import_combos4 %>%
  dplyr::select(new_importer, dropped_exporter, hs4, year, freight_perkg_deni,tau_hs4_deni) %>%
  rename(new_exporter = dropped_exporter,freight_perkg_neni = freight_perkg_deni,tau_hs4_neni = tau_hs4_deni)



### Merge the combinations when canada is the new exporter and when Canada isnt the dropped exporter ###
merged_import_combos4 <- merge(noncan_import_combos4, can_import_combos4, by = c("hs4","year"), all = TRUE) %>%
  left_join(canada_freight_combos4, by = c("new_importer", "hs4", "new_exporter","year")) %>%
  mutate(cca_notau = cif_perkg_deni - freight_perkg_neni - fob_perkg_nedi) %>%
  mutate(cca_tau = (cif_perkg_deni * (1 + tau_hs4_deni))/(1 + tau_hs4_neni) - freight_perkg_neni - (fob_perkg_nedi / (1 + tau_hs4_nedi))) %>%
  select(new_exporter, dropped_importer, dropped_exporter, new_importer,hs4,year,cca_notau,cca_tau) %>% 
  subset(., !is.na(cca_tau) & !is.na(cca_notau))

merged_combos4 <- bind_rows(merged_export_combos4, merged_import_combos4) 



# Apply thresholds to trades between the dropped exporter and new importer
merged_combos4 <- merged_combos4 %>%
  left_join(thresholds, by = c("dropped_exporter" = "exporter",
                               "new_importer" = "importer",
                               "hs4" = "hs4",
                               "year" = "year")) %>%
  subset(.,fob > threshold & dropped_exporter != "Canada" | dropped_exporter == "Rest of World" | dropped_importer == "Rest of World") %>%
  dplyr::select(-fob, -threshold)



can_exports4 <- subset(flows,exporter == "Canada") %>%
  mutate(hs4 = substr(hs6,1,4)) %>% 
  select(hs4,year,fob) %>%
  group_by(hs4,year) %>%
  summarise(can_export_fob = sum(fob, na.rm = TRUE)) %>%
  ungroup() %>%
  group_by(year) %>%
  # Rank HS codes
  arrange(desc(can_export_fob)) %>%
  mutate(rank = row_number()) %>%
  ungroup() %>%
  # Give suffix "most exported after ranking" for example "Most exported product", "2nd most exported product",
  mutate(
    rank_suffix = if_else(
      rank == 1,
      "Most exported HS4 product",
      paste0(ordinal_en(rank), " most exported HS4 product")
    )
  ) %>%
  mutate(year = as.numeric(year))


can_imports4 <- subset(flows,importer == "Canada") %>%
  mutate(hs4 = substr(hs6,1,4)) %>% 
  select(hs4,year,fob) %>%
  group_by(hs4,year) %>%
  summarise(can_import_fob = sum(fob, na.rm = TRUE)) %>%
  ungroup() %>%
  group_by(year) %>%
  # Rank HS codes
  arrange(desc(can_import_fob)) %>%
  mutate(rank = row_number()) %>%
  ungroup() %>%
  # Give suffix "most exported after ranking" for example "Most exported product", "2nd most exported product",
  mutate(
    rank_suffix = if_else(
      rank == 1,
      "Most imported HS4 product",
      paste0(ordinal_en(rank), " most imported HS4 product")
    )
  ) %>%
  mutate(year = as.numeric(year))


merged_export_combos_hs4 <- merged_combos4 %>%
  subset(.,new_exporter == "Canada") %>% 
  left_join(can_exports4 %>% select(hs4, year, rank, rank_suffix), by = c("hs4" = "hs4", "year" = "year")) %>%
  rename(hs4_rank = rank, hs4_rank_suffix = rank_suffix, cca_hs4_notau = cca_notau, cca_hs4_tau = cca_tau) %>%
  select(new_exporter, dropped_importer, dropped_exporter, new_importer,year,hs4,hs4_rank,hs4_rank_suffix,cca_hs4_notau,cca_hs4_tau) %>% 
  subset(., dropped_importer != new_importer & new_exporter != dropped_exporter) %>% 
  ungroup()

merged_import_combos_hs4 <- merged_combos4 %>%
  subset(.,new_importer == "Canada") %>% 
  left_join(can_imports4 %>% select(hs4, year, rank, rank_suffix), by = c("hs4" = "hs4", "year" = "year")) %>%
  rename(hs4_rank = rank, hs4_rank_suffix = rank_suffix, cca_hs4_notau = cca_notau, cca_hs4_tau = cca_tau) %>%
  select(new_exporter, dropped_importer, dropped_exporter, new_importer,year,hs4,hs4_rank,hs4_rank_suffix,cca_hs4_notau,cca_hs4_tau) %>% 
  subset(., dropped_importer != new_importer & new_exporter != dropped_exporter) %>% 
  ungroup()


merged_export_combos_hs4$hs4_desc[merged_export_combos_hs4$hs4 == "0203"] <- "Meat of swine"
merged_export_combos_hs4$hs4_desc[merged_export_combos_hs4$hs4 == "0306"] <- "Crustaceans"
merged_export_combos_hs4$hs4_desc[merged_export_combos_hs4$hs4 == "0713"] <- "Dried pulses"
merged_export_combos_hs4$hs4_desc[merged_export_combos_hs4$hs4 == "1001"] <- "Wheat & meslin"
merged_export_combos_hs4$hs4_desc[merged_export_combos_hs4$hs4 == "1205"] <- "Rape or colza seeds"
merged_export_combos_hs4$hs4_desc[merged_export_combos_hs4$hs4 == "1514"] <- "Rape, colza, mustard oil"
merged_export_combos_hs4$hs4_desc[merged_export_combos_hs4$hs4 == "1905"] <- "Baked food"
merged_export_combos_hs4$hs4_desc[merged_export_combos_hs4$hs4 == "2601"] <- "Iron ores"
merged_export_combos_hs4$hs4_desc[merged_export_combos_hs4$hs4 == "2603"] <- "Copper ores"
merged_export_combos_hs4$hs4_desc[merged_export_combos_hs4$hs4 == "2701"] <- "Coal briquettes"
merged_export_combos_hs4$hs4_desc[merged_export_combos_hs4$hs4 == "2709"] <- "Crude oil"
merged_export_combos_hs4$hs4_desc[merged_export_combos_hs4$hs4 == "2710"] <- "Petroleum oils"
merged_export_combos_hs4$hs4_desc[merged_export_combos_hs4$hs4 == "2711"] <- "Petroleum gases"
merged_export_combos_hs4$hs4_desc[merged_export_combos_hs4$hs4 == "2713"] <- "Petroleum coke and bitumen"
merged_export_combos_hs4$hs4_desc[merged_export_combos_hs4$hs4 == "3004"] <- "Medicaments"
merged_export_combos_hs4$hs4_desc[merged_export_combos_hs4$hs4 == "3104"] <- "Mineral or chemical fertilizers"
merged_export_combos_hs4$hs4_desc[merged_export_combos_hs4$hs4 == "3901"] <- "Polymers of ethylene"
merged_export_combos_hs4$hs4_desc[merged_export_combos_hs4$hs4 == "4407"] <- "Wood products"
merged_export_combos_hs4$hs4_desc[merged_export_combos_hs4$hs4 == "4410"] <- "Particle board and OSB"
merged_export_combos_hs4$hs4_desc[merged_export_combos_hs4$hs4 == "4703"] <- "Chemical wood pulp"
merged_export_combos_hs4$hs4_desc[merged_export_combos_hs4$hs4 == "7102"] <- "Unmounted diamonds"
merged_export_combos_hs4$hs4_desc[merged_export_combos_hs4$hs4 == "7108"] <- "Unwrought / semi-manufactured gold"
merged_export_combos_hs4$hs4_desc[merged_export_combos_hs4$hs4 == "7501"] <- "Nickel mattes, sinters"
merged_export_combos_hs4$hs4_desc[merged_export_combos_hs4$hs4 == "7601"] <- "Unwrought aluminium"
merged_export_combos_hs4$hs4_desc[merged_export_combos_hs4$hs4 == "8411"] <- "Jet engines"
merged_export_combos_hs4$hs4_desc[merged_export_combos_hs4$hs4 == "8703"] <- "Passenger vehicles"
merged_export_combos_hs4$hs4_desc[merged_export_combos_hs4$hs4 == "8704"] <- "Cargo trucks"
merged_export_combos_hs4$hs4_desc[merged_export_combos_hs4$hs4 == "8708"] <- "Vehicle parts"
merged_export_combos_hs4$hs4_desc[merged_export_combos_hs4$hs4 == "8802"] <- "Aircrafts"
merged_export_combos_hs4$hs4_desc[merged_export_combos_hs4$hs4 == "9403"] <- "Furniture"


merged_import_combos_hs4$hs4_desc[merged_import_combos_hs4$hs4 == "2709"] <- "Crude oil"
merged_import_combos_hs4$hs4_desc[merged_import_combos_hs4$hs4 == "2710"] <- "Petroleum oils"
merged_import_combos_hs4$hs4_desc[merged_import_combos_hs4$hs4 == "2711"] <- "Petroleum gases"
merged_import_combos_hs4$hs4_desc[merged_import_combos_hs4$hs4 == "3004"] <- "Medicaments"
merged_import_combos_hs4$hs4_desc[merged_import_combos_hs4$hs4 == "7108"] <- "Unwrought / semi-manufactured gold"
merged_import_combos_hs4$hs4_desc[merged_import_combos_hs4$hs4 == "8703"] <- "Passenger vehicles"
merged_import_combos_hs4$hs4_desc[merged_import_combos_hs4$hs4 == "8704"] <- "Cargo trucks"
merged_import_combos_hs4$hs4_desc[merged_import_combos_hs4$hs4 == "8708"] <- "Vehicle parts"
merged_import_combos_hs4$hs4_desc[merged_import_combos_hs4$hs4 == "8802"] <- "Aircrafts"
merged_import_combos_hs4$hs4_desc[merged_import_combos_hs4$hs4 == "9403"] <- "Furniture"
merged_import_combos_hs4$hs4_desc[merged_import_combos_hs4$hs4 == "8544"] <- "Insulated wire"
merged_import_combos_hs4$hs4_desc[merged_import_combos_hs4$hs4 == "8407"] <- "Spark-ignition engines"
merged_import_combos_hs4$hs4_desc[merged_import_combos_hs4$hs4 == "2204"] <- "Wine of grapes"
merged_import_combos_hs4$hs4_desc[merged_import_combos_hs4$hs4 == "3002"] <- "Vaccines and sera"
merged_import_combos_hs4$hs4_desc[merged_import_combos_hs4$hs4 == "3923"] <- "Plastic packaging articles"
merged_import_combos_hs4$hs4_desc[merged_import_combos_hs4$hs4 == "4011"] <- "New rubber tires"
merged_import_combos_hs4$hs4_desc[merged_import_combos_hs4$hs4 == "7112"] <- "Precious-metal scrap"
merged_import_combos_hs4$hs4_desc[merged_import_combos_hs4$hs4 == "8415"] <- "Air-conditioning machines"
merged_import_combos_hs4$hs4_desc[merged_import_combos_hs4$hs4 == "8419"] <- "Thermal-processing machinery"
merged_import_combos_hs4$hs4_desc[merged_import_combos_hs4$hs4 == "8421"] <- "Filters and centrifuges"
merged_import_combos_hs4$hs4_desc[merged_import_combos_hs4$hs4 == "8429"] <- "Earthmoving machinery"
merged_import_combos_hs4$hs4_desc[merged_import_combos_hs4$hs4 == "8431"] <- "Heavy-machinery parts"
merged_import_combos_hs4$hs4_desc[merged_import_combos_hs4$hs4 == "8471"] <- "Computers and servers"
merged_import_combos_hs4$hs4_desc[merged_import_combos_hs4$hs4 == "8481"] <- "Valves and taps"
merged_import_combos_hs4$hs4_desc[merged_import_combos_hs4$hs4 == "8483"] <- "Transmission shafts, gears"
merged_import_combos_hs4$hs4_desc[merged_import_combos_hs4$hs4 == "8517"] <- "Telecom/network equipment"
merged_import_combos_hs4$hs4_desc[merged_import_combos_hs4$hs4 == "8528"] <- "Monitors and projectors"
merged_import_combos_hs4$hs4_desc[merged_import_combos_hs4$hs4 == "8701"] <- "Tractors"
merged_import_combos_hs4$hs4_desc[merged_import_combos_hs4$hs4 == "8716"] <- "Trailers and semi-trailers"
merged_import_combos_hs4$hs4_desc[merged_import_combos_hs4$hs4 == "9401"] <- "Seats and parts"



# Final columns and order
merged_export_combos_hs4 <- merged_export_combos_hs4 %>%
  select(new_exporter, dropped_importer, dropped_exporter, new_importer,year,hs4,hs4_desc,hs4_rank, hs4_rank_suffix,cca_hs4_notau,cca_hs4_tau) %>%
  subset(.,!is.na(cca_hs4_notau) & !is.na(cca_hs4_tau))

cca_hs6 <- merged_export_combos %>%   # has hs6_* and cca_hs6_{notau,tau}
  transmute(
    year,
    product_code  = hs6,
    product_level = "HS6",
    hs4_code      = substr(hs6, 1, 4),
    dropped_exporter, dropped_importer, new_exporter, new_importer,
    cca_notau     = as.numeric(cca_hs6_notau),
    cca_tau       = as.numeric(cca_hs6_tau)
  )

# ---- Harmonize HS4 to the same long format ----
cca_hs4 <- merged_export_combos_hs4 %>%      # has hs4_* and cca_hs4_{notau,tau}
  transmute(
    year,
    product_code  = hs4,
    product_level = "HS4",
    hs4_code      = hs4,
    dropped_exporter, dropped_importer, new_exporter, new_importer,
    cca_notau     = as.numeric(cca_hs4_notau),
    cca_tau       = as.numeric(cca_hs4_tau)
  )

# ---- Build product metadata (labels + ranks) ----
# HS6 labels/ranks
prod_hs6 <- merged_export_combos %>%
  transmute(
    year,
    product_code  = hs6,
    product_level = "HS6",
    hs4_code      = substr(hs6, 1, 4),
    product_label = hs6_desc,
    hs4_rank      = NA_integer_,
    hs6_rank      = hs6_rank
  ) %>% distinct()

# HS4 labels/ranks
prod_hs4 <- merged_export_combos_hs4 %>%
  transmute(
    year,
    product_code  = hs4,
    product_level = "HS4",
    hs4_code      = hs4,
    product_label = hs4_desc,
    hs4_rank      = hs4_rank,
    hs6_rank      = NA_integer_
  ) %>% distinct()

products_meta <- bind_rows(prod_hs4, prod_hs6) %>%
  group_by(year, product_code, product_level) %>%
  slice(1) %>%
  ungroup()

# propagate HS4 ranks to HS6 via hs4_code
hs4_ranks <- prod_hs4 %>% select(year, hs4_code, hs4_rank) %>% distinct()
products_meta <- products_meta %>%
  left_join(hs4_ranks, by = c("year","hs4_code")) %>%
  mutate(hs4_rank = coalesce(hs4_rank.x, hs4_rank.y)) %>%
  select(-hs4_rank.x, -hs4_rank.y)

# ---- Final one-long table that the module can use directly ----
cca_all_exports <- bind_rows(cca_hs4, cca_hs6) %>%
  left_join(products_meta,
            by = c("year", "product_code", "product_level", "hs4_code")) %>%
  select(
    year, product_code, product_level, hs4_code,
    product_label, hs4_rank, hs6_rank,
    dropped_exporter, dropped_importer, new_exporter, new_importer,
    cca_notau, cca_tau
  ) %>%
  mutate(
    year          = as.integer(year),
    product_code  = as.character(product_code),
    product_level = as.character(product_level),
    hs4_code      = as.character(hs4_code),
    hs4_rank      = as.integer(hs4_rank),
    hs6_rank      = as.integer(hs6_rank),
    cca_notau     = as.numeric(cca_notau),
    cca_tau       = as.numeric(cca_tau)
  ) %>%
  distinct()


cca_all_exports <- cca_all_exports %>%
  mutate(
    Importer_ISO_country = countrycode(
      new_importer, "country.name", "iso3c",
      custom_match = c(
        "United States"        = "USA",
        "Netherlands"    = "NLD",
        "Switzerland"      = "CHE",  # pick CHE for the combined name
        "Ivory Coast"                   = "CIV",
        "South Korea"              = "KOR",
        "Hong Kong"            = "HKG",
        "China, Macao SAR"                = "MAC",
        "Lao People's Dem. Rep."          = "LAO",
        "Iran (Islamic Republic of)"      = "IRN",
        "Moldova, Republic of"            = "MDA",
        "Tanzania, United Republic of"    = "TZA",
        "Turkiye"                         = "TUR",
        "Cabo Verde"                      = "CPV",
        "Bolivia"= "BOL",
        "Bahamas"                         = "BHS",
        "Brunei"               = "BRN",
        "DR Congo"         = "COD"
        # add more edge names here if you see NAs in a quick table(…)
      )
    )
  )



hs6_lookup <- tribble(
  ~product_code, ~short_label,
  # --- Pork ---
  "020311","Pork carcasses/half, fresh",
  "020312","Pork ham/shoulder bone-in, fresh",
  "020319","Pork, other cuts n.e.c., fresh",
  "020321","Pork carcasses/half, frozen",
  "020322","Pork ham/shoulder bone-in, frozen",
  "020329","Pork, other cuts n.e.c., frozen",
  
  # --- Crustaceans (0306) ---
  "030611","Rock lobster etc., frozen (any prep)",
  "030612","Lobster, frozen (any prep)",
  "030614","Crab, frozen (any prep)",
  "030615","Norway lobster, frozen (any prep)",
  "030616","Shrimp/prawn cold-water, frozen (any prep)",
  "030617","Shrimp/prawn excl. cold-water, frozen (any prep)",
  "030619","Crustaceans n.e.c., frozen/meal",
  "030631","Rock lobster etc., live/fresh/chilled",
  "030632","Lobster, live/fresh/chilled",
  "030633","Crab, live/fresh/chilled",
  "030634","Norway lobster, live/fresh/chilled",
  "030635","Shrimp/prawn cold-water, live/fresh/chilled",
  "030636","Shrimp/prawn excl. cold-water, live/fresh/chilled",
  "030691","Rock lobster etc., smoked (any prep)",
  "030692","Lobster, smoked (any prep)",
  "030693","Crab, smoked (any prep)",
  "030695","Shrimp/prawn, smoked (any prep)",
  "030699","Crustaceans n.e.c., smoked/meal",
  
  # --- Dried pulses (0713) ---
  "071310","Peas, dried",
  "071320","Chickpeas, dried",
  "071331","Mung/green gram, dried",
  "071332","Adzuki beans, dried",
  "071333","Kidney/white pea beans, dried",
  "071334","Bambara beans, dried",
  "071335","Cowpeas, dried",
  "071339","Beans n.e.c., dried",
  "071340","Lentils, dried",
  "071350","Broad/horse beans, dried",
  "071360","Pigeon peas, dried",
  "071390","Legumes n.e.c., dried",
  
  # --- Wheat (1001) ---
  "100111","Durum wheat, seed",
  "100119","Durum wheat, other",
  "100191","Other wheat/meslin, seed",
  "100199","Other wheat/meslin, other",
  
  # --- Rapeseed/Canola (1205, 1514) ---
  "120510","Rapeseed/canola LER, seed",
  "120590","Rapeseed, other than LER, seed",
  "151411","Canola/rapeseed oil LER, crude",
  "151419","Canola/rapeseed oil LER, other",
  "151491","Rapeseed oil excl. LER, crude",
  "151499","Rapeseed oil excl. LER, other",
  
  # --- Baked goods (1905) ---
  "190510","Crispbread",
  "190520","Gingerbread etc.",
  "190531","Sweet biscuits",
  "190532","Waffles/wafers",
  "190540","Rusks/toasted bread",
  "190590","Bakers’ wares n.e.c.; wafers etc.",
  
  # --- Ores (2601, 2603) ---
  "260111","Iron ore, non-agglomerated",
  "260112","Iron ore, agglomerated",
  "260120","Iron pyrites, roasted",
  "260300","Copper ores & conc.",
  
  # --- Coal (2701) ---
  "270111","Anthracite (not agglomerated)",
  "270112","Coal, bituminous (not agglomerated)",
  "270119","Coal, other (not agglomerated)",
  "270120","Coal briquettes/ovoid fuels",
  
  # --- Petroleum (2709, 2710, 2711, 2713) ---
  "270900","Crude petroleum oils",
  "271011","Refined petroleum, light oils/preps",
  "271012","Refined petroleum (no biodiesel), light oils/preps",
  "271019","Refined petroleum, other than light oils",
  "271020","Refined petroleum with biodiesel",
  "271099","Waste oils (petroleum), n.e.c.",
  "271111","LNG (natural gas, liquefied)",
  "271112","Propane, liquefied",
  "271113","Butanes, liquefied",
  "271114","Ethylene/propylene/butylene/butadiene, liquefied",
  "271119","Petroleum gases, liquefied n.e.c.",
  "271129","Petroleum gases, gaseous n.e.c.",
  "271311","Petroleum coke, not calcined",
  "271312","Petroleum coke, calcined",
  "271320","Petroleum bitumen",
  "271390","Petroleum residues n.e.c.",
  
  # --- Radioactive (2844) (kept short) ---
  "284410","Natural uranium & compounds etc.",
  "284420","Enriched U-235/plutonium & compounds",
  "284430","Depleted U-235/thorium & compounds",
  "284440","Radioactive elements/isotopes n.e.c.",
  
  # --- Medicaments (3004 + extras) ---
  "300410","Medicaments with penicillins/streptomycins",
  "300420","Medicaments with other antibiotics",
  "300431","Medicaments with insulin",
  "300432","Medicaments with corticosteroids",
  "300439","Medicaments with hormones (excl. insulin)",
  "300440","Medicaments with alkaloids (excl. hormones/antibiotics)",
  "300441","Medicaments with ephedrine",
  "300442","Medicaments with pseudoephedrine",
  "300449","Medicaments with alkaloids, other",
  "300450","Medicaments with vitamins",
  "300460","Antimalarial medicaments (per note)",
  "300490","Medicaments n.e.c.",
  "300443","Medicaments with norephedrine",
  
  # --- Fertilizers (3104) ---
  "310420","Potash fertilizer, KCl",
  "310430","Potash fertilizer, K2SO4",
  "310490","Potash fertilizers n.e.c.",
  
  # --- Polyethylene (3901) ---
  "390110","Polyethylene <0.94 sg",
  "390120","Polyethylene ≥0.94 sg",
  "390130","EVA copolymers",
  "390140","Ethylene-alpha-olefin copolymers <0.94 sg",
  "390190","Ethylene polymers n.e.c.",
  
  # --- Sawnwood (4407) ---
  "440710","Conifer sawnwood >6mm",
  "440711","Pine sawnwood >6mm",
  "440712","Fir/spruce sawnwood >6mm",
  "440713","S-P-F conifer sawnwood >6mm",
  "440714","Hem-fir sawnwood >6mm",
  "440719","Other conifer sawnwood >6mm",
  "440721","Tropical wood—mahogany >6mm",
  "440722","Tropical wood—virola/imbuia/balsa >6mm",
  "440725","Tropical wood—meranti >6mm",
  "440726","Tropical wood—white lauan/meranti/seraya >6mm",
  "440727","Tropical wood—sapelli >6mm",
  "440728","Tropical wood—iroko >6mm",
  "440729","Tropical wood n.e.c. >6mm",
  "440791","Oak sawnwood >6mm",
  "440792","Beech sawnwood >6mm",
  "440793","Maple sawnwood >6mm",
  "440794","Cherry sawnwood >6mm",
  "440795","Ash sawnwood >6mm",
  "440796","Birch sawnwood >6mm",
  "440797","Poplar/aspen sawnwood >6mm",
  "440799","Sawnwood n.e.c. >6mm",
  
  # --- Boards (4410) ---
  "441011","Particle board (wood)",
  "441012","OSB (wood)",
  "441019","Wafer/other board (wood)",
  "441090","Boards (non-wood, particle/OSB)",
  
  # --- Pulp (4703) ---
  "470311","Pulp, chemical (kraft), unbleached—conifer",
  "470319","Pulp, chemical (kraft), unbleached—non-conifer",
  "470321","Pulp, chemical (kraft), bleached—conifer",
  "470329","Pulp, chemical (kraft), bleached—non-conifer",
  
  # --- Diamonds (7102) ---
  "710210","Diamonds, unsorted",
  "710221","Industrial diamonds, unworked",
  "710229","Industrial diamonds, other",
  "710231","Non-industrial diamonds, unworked",
  "710239","Non-industrial diamonds, other",
  
  # --- Gold (7108) ---
  "710811","Gold, powder",
  "710812","Gold, unwrought",
  "710813","Gold, semi-manufactured",
  
  # --- Nickel (7501) ---
  "750110","Nickel mattes",
  "750120","Nickel oxide sinters",
  
  # --- Aluminium (7601) ---
  "760110","Aluminium, unwrought (not alloyed)",
  "760120","Aluminium, unwrought (alloy)",
  
  # --- Spark-ignition engines (8407) ---
  "840710","Aircraft piston engines (SI/rotary)",
  "840721","Outboard marine engines (SI)",
  "840729","Marine engines (non-outboard, SI)",
  "840731","Vehicle piston engines ≤50cc",
  "840732","Vehicle piston engines >50–250cc",
  "840733","Vehicle piston engines >250–1000cc",
  "840734","Vehicle piston engines >1000cc",
  "840790","Rotary piston engines (non-air/marine)",
  
  # --- Jet/turboprop/gasturbines (8411/8412 subset) ---
  "841111","Turbojets ≤25kN",
  "841112","Turbojets >25kN",
  "841121","Turboprops ≤1100kW",
  "841122","Turboprops >1100kW",
  "841181","Gas turbines ≤5000kW (excl. turbojet/prop)",
  "841182","Gas turbines >5000kW (excl. turbojet/prop)",
  "841191","Parts of turbojets/turboprops",
  "841199","Parts of gas turbines n.e.c.",
  
  # --- Vehicles (8703/8704/others) ---
  "870310","Snow/golf & similar vehicles",
  "870321","Passenger cars, SI ≤1000cc",
  "870322","Passenger cars, SI >1000–1500cc",
  "870323","Passenger cars, SI >1500–3000cc",
  "870324","Passenger cars, SI >3000cc",
  "870331","Passenger cars, diesel ≤1500cc",
  "870332","Passenger cars, diesel >1500–2500cc",
  "870333","Passenger cars, diesel >2500cc",
  "870340","Hybrid (SI+EV), non-plug-in",
  "870350","Hybrid (diesel+EV), non-plug-in",
  "870360","PHEV (SI+EV), plug-in",
  "870370","PHEV (diesel+EV), plug-in",
  "870380","Battery electric vehicles (BEV)",
  "870390","Passenger vehicles n.e.c.",
  "870410","Dumpers, off-highway",
  "870421","Goods vehicles, diesel ≤5t GVW",
  "870422","Goods vehicles, diesel >5–20t GVW",
  "870423","Goods vehicles, diesel >20t GVW",
  "870431","Goods vehicles, SI ≤5t GVW",
  "870432","Goods vehicles, SI >5t GVW",
  "870490","Goods vehicles n.e.c.",
  
  # --- Vehicle parts (8708) ---
  "870810","Bumpers & parts",
  "870821","Seat belts",
  "870822","Windscreens and windows",
  "870829","Body parts/accessories n.e.c.",
  "870830","Brakes & parts",
  "870840","Gear boxes & parts",
  "870850","Drive-axles/differentials & parts",
  "870870","Road wheels & parts",
  "870880","Suspension & shock absorbers",
  "870891","Radiators & parts",
  "870892","Mufflers/exhausts & parts",
  "870893","Clutches & parts",
  "870894","Steering assemblies & parts",
  "870895","Airbags (with inflator) & parts",
  "870899","Parts & accessories n.e.c. (8708)",
  
  # --- Aircraft/space (8802/880260) ---
  "880211","Helicopters ≤2000kg unladen",
  "880212","Helicopters >2000kg unladen",
  "880220","Aircraft ≤2000kg unladen",
  "880230","Aircraft >2000–15,000kg unladen",
  "880240","Aircraft >15,000kg unladen",
  "880260","Spacecraft & launch vehicles",
  
  # --- Furniture (9403) ---
  "940310","Office furniture, metal",
  "940320","Furniture, metal (non-office)",
  "940330","Office furniture, wood",
  "940340","Kitchen furniture, wood",
  "940350","Bedroom furniture, wood",
  "940360","Furniture, wood (other)",
  "940370","Furniture, plastic",
  "940381","Furniture, bamboo/rattan",
  "940382","Furniture, bamboo",
  "940383","Furniture, rattan",
  "940389","Furniture, cane/osier/similar",
  "940390","Furniture parts",
  "940391","Furniture; parts, of wood",
  "940399","Furniture; parts, other than wood"
)

# assign/replace the label
cca_all_exports <- cca_all_exports %>%
  left_join(hs6_lookup, by = "product_code") %>%
  mutate(product_label = coalesce(short_label, product_label)) %>%
  select(-short_label) %>% 
  select(year, product_code, product_level, hs4_code,
         product_label, hs4_rank, hs6_rank,  new_exporter,dropped_importer,
         dropped_exporter,  new_importer, Importer_ISO_country,
         cca_notau, cca_tau)



# db_host <- "127.0.0.1"
# db_user <- "gvcdtlab"
# db_password <- "gvcdtlab-datahub"
# db_name <- "gvcdtlab"
# db_port <- 3306
# 
# #Create a connection
# 
# con <- dbConnect(
#   drv = RMariaDB::MariaDB(),
#   host = db_host,
#   user = db_user,
#   password = db_password,
#   dbname = db_name,
#   port = db_port
# )
# 
# 
# # Write the data frame to the database
# copy_to(con, cca_all, "cca_h6", temporary = FALSE, overwrite = TRUE)
# 
# # Close the connection
# dbDisconnect(con)

qs::qsave(cca_all_exports, "export_cca.qs")





cca_hs6 <- merged_import_combos %>%   # has hs6_* and cca_hs6_{notau,tau}
  transmute(
    year,
    product_code  = hs6,
    product_level = "HS6",
    hs4_code      = substr(hs6, 1, 4),
    dropped_exporter, dropped_importer, new_exporter, new_importer,
    cca_notau     = as.numeric(cca_hs6_notau),
    cca_tau       = as.numeric(cca_hs6_tau)
  )

# ---- Harmonize HS4 to the same long format ----
cca_hs4 <- merged_import_combos_hs4 %>%      # has hs4_* and cca_hs4_{notau,tau}
  transmute(
    year,
    product_code  = hs4,
    product_level = "HS4",
    hs4_code      = hs4,
    dropped_exporter, dropped_importer, new_exporter, new_importer,
    cca_notau     = as.numeric(cca_hs4_notau),
    cca_tau       = as.numeric(cca_hs4_tau)
  )

# ---- Build product metadata (labels + ranks) ----
# HS6 labels/ranks
prod_hs6 <- merged_import_combos %>%
  transmute(
    year,
    product_code  = hs6,
    product_level = "HS6",
    hs4_code      = substr(hs6, 1, 4),
    product_label = hs6_desc,
    hs4_rank      = NA_integer_,
    hs6_rank      = hs6_rank
  ) %>% distinct()

# HS4 labels/ranks
prod_hs4 <- merged_import_combos_hs4 %>%
  transmute(
    year,
    product_code  = hs4,
    product_level = "HS4",
    hs4_code      = hs4,
    product_label = hs4_desc,
    hs4_rank      = hs4_rank,
    hs6_rank      = NA_integer_
  ) %>% distinct()

products_meta <- bind_rows(prod_hs4, prod_hs6) %>%
  group_by(year, product_code, product_level) %>%
  slice(1) %>%
  ungroup()

# propagate HS4 ranks to HS6 via hs4_code
hs4_ranks <- prod_hs4 %>% select(year, hs4_code, hs4_rank) %>% distinct()
products_meta <- products_meta %>%
  left_join(hs4_ranks, by = c("year","hs4_code")) %>%
  mutate(hs4_rank = coalesce(hs4_rank.x, hs4_rank.y)) %>%
  select(-hs4_rank.x, -hs4_rank.y)

# ---- Final one-long table that the module can use directly ----
cca_all_imports <- bind_rows(cca_hs4, cca_hs6) %>%
  left_join(products_meta,
            by = c("year", "product_code", "product_level", "hs4_code")) %>%
  select(
    year, product_code, product_level, hs4_code,
    product_label, hs4_rank, hs6_rank,
    dropped_exporter, dropped_importer, new_exporter, new_importer,
    cca_notau, cca_tau
  ) %>%
  mutate(
    year          = as.integer(year),
    product_code  = as.character(product_code),
    product_level = as.character(product_level),
    hs4_code      = as.character(hs4_code),
    hs4_rank      = as.integer(hs4_rank),
    hs6_rank      = as.integer(hs6_rank),
    cca_notau     = as.numeric(cca_notau),
    cca_tau       = as.numeric(cca_tau)
  ) %>%
  distinct()


cca_all_imports <- cca_all_imports %>%
  mutate(
    Exporter_ISO_country = countrycode(
      new_exporter, "country.name", "iso3c",
      custom_match = c(
        "United States"        = "USA",
        "Netherlands"    = "NLD",
        "Switzerland"      = "CHE",  # pick CHE for the combined name
        "Ivory Coast"                   = "CIV",
        "South Korea"              = "KOR",
        "Hong Kong"            = "HKG",
        "China, Macao SAR"                = "MAC",
        "Lao People's Dem. Rep."          = "LAO",
        "Iran (Islamic Republic of)"      = "IRN",
        "Moldova, Republic of"            = "MDA",
        "Tanzania, United Republic of"    = "TZA",
        "Turkiye"                         = "TUR",
        "Cabo Verde"                      = "CPV",
        "Bolivia"= "BOL",
        "Bahamas"                         = "BHS",
        "Brunei"               = "BRN",
        "DR Congo"         = "COD"
        # add more edge names here if you see NAs in a quick table(…)
      )
    )
  )



hs6_lookup <- tribble(
  ~product_code, ~short_label,
  # 2204 Wine
  "220410","Wine, sparkling",
  "220421","Wine, still ≤2L",
  "220422","Wine, still >2–10L",
  "220429","Wine, still >10L",
  "220430","Grape must",
  
  # 2709/2710/2711 petroleum & gases
  "270900","Crude petroleum oils",
  "271012","Refined petroleum, light oils/preps",
  "271019","Refined petroleum, other than light oils",
  "271020","Refined petroleum with biodiesel",
  "271091","Waste oils incl. PCBs/PCTs/PBBs",
  "271099","Waste oils n.e.c.",
  "271111","LNG (natural gas, liquefied)",
  "271112","Propane, liquefied",
  "271113","Butanes, liquefied",
  "271114","C2–C4 olefins, liquefied",
  "271119","Petroleum gases, liquefied n.e.c.",
  "271121","Natural gas, gaseous",
  
  # 3002 biologics / cell cultures
  "300212","Antisera & blood fractions",
  "300213","Immunological products, unmixed (not retail)",
  "300214","Immunological products, mixed (retail doses)",
  "300215","Immunological products, retail doses",
  "300249","Toxins & cultures n.e.c.",
  "300251","Cell therapy products",
  "300259","Cell cultures (other)",
  "300290","Toxins/cultures n.e.c.",
  
  # 3004 medicaments
  "300410","Medicaments with penicillins/streptomycins",
  "300431","Medicaments with insulin",
  "300432","Medicaments with corticosteroids",
  "300441","Medicaments with ephedrine",
  "300442","Medicaments with pseudoephedrine",
  "300443","Medicaments with norephedrine",
  "300449","Medicaments with alkaloids, other",
  "300460","Antimalarial medicaments",
  "300490","Medicaments n.e.c.",
  
  # 3923 plastic packaging
  "392310","Plastic boxes/crates",
  "392321","Sacks & bags, ethylene",
  "392329","Sacks & bags, other plastics",
  "392330","Carboys/bottles/flasks",
  "392340","Spools/cops/bobbins",
  "392350","Stoppers/lids/caps",
  "392390","Packing articles n.e.c.",
  
  # 4011 tyres
  "401110","Tyres, passenger cars",
  "401120","Tyres, buses/lorry",
  "401130","Tyres, aircraft",
  "401140","Tyres, motorcycles",
  "401150","Tyres, bicycles",
  "401170","Tyres, construction/mining/industrial",
  "401180","Tyres, light commercial",
  "401190","Tyres, n.e.c.",
  
  # 7108 gold
  "710811","Gold, powder",
  "710812","Gold, unwrought",
  "710813","Gold, semi-manufactured",
  
  # 7112 precious-metal scrap
  "711230","Gold, waste & scrap",
  "711291","Platinum, waste & scrap",
  "711292","Precious metal scrap, other",
  "711299","Precious metal scrap n.e.c.",
  
  # 8407 piston engines (SI/rotary)
  "840721","Outboard marine engines (SI)",
  "840729","Marine engines (other, SI)",
  "840732","Vehicle SI engines >50–250cc",
  "840733","Vehicle SI engines >250–1000cc",
  "840734","Vehicle SI engines >1000cc",
  "840790","Rotary piston engines (other)",
  
  # 8415 air-conditioning
  "841510","AC, window/wall/fixed",
  "841520","AC for motor vehicles",
  "841581","AC, reversible heat pump",
  "841582","AC, with refrigerating unit",
  "841583","AC, without refrigerating unit",
  "841590","AC parts",
  
  # 8419 heating/cooling/processing
  "841911","Instantaneous gas water heaters",
  "841912","Solar water heaters",
  "841919","Water heaters, other non-electric",
  "841920","Medical/lab sterilizers",
  "841934","Dryers, agricultural",
  "841935","Dryers, wood/pulp/paper",
  "841939","Dryers, other",
  "841940","Distilling/rectifying plant",
  "841950","Heat-exchange units",
  "841960","Liquefying air/gas machinery",
  "841981","Food/drink making & heating",
  
  # 8421 filters/centrifuges
  "842111","Cream separators",
  "842112","Clothes dryers (centrifugal)",
  "842119","Centrifuges n.e.c.",
  "842121","Water filters/purifiers",
  "842122","Beverage filters (non-water)",
  "842123","Oil/petrol filters (ICE)",
  "842131","Intake air filters (ICE)",
  "842132","Exhaust gas catalysts/DPF (ICE)",
  "842139","Gas filters/purifiers, other",
  "842199","Filter parts",
  
  # 8429 earthmoving/construction
  "842911","Bulldozers, track-laying",
  "842919","Bulldozers, other",
  "842920","Graders/levellers",
  "842940","Road rollers",
  "842951","Front-end loaders",
  "842952","Excavators, 360°",
  "842959","Shovels/excavators n.e.c.",
  
  # 8431 parts for lifting/earthmoving
  "843120","Parts for 8427 (forklifts etc.)",
  "843131","Lift/escalator parts",
  "843141","Buckets/grabs/grips",
  "843142","Bulldozer blades",
  "843149","Earthmoving parts n.e.c.",
  
  # 8471 computing
  "847130","Laptops/portables ≤10kg",
  "847141","ADP systems, CPU+I/O",
  "847149","ADP systems n.e.c.",
  "847150","Processing units (ADP)",
  "847170","Storage units",
  "847180","ADP units n.e.c.",
  "847190","Readers/transcribers etc.",
  
  # 8481/8483 valves & power-train parts
  "848110","Pressure-reducing valves",
  "848120","Hydraulic/pneumatic valves",
  "848130","Check (non-return) valves",
  "848180","Valves/taps n.e.c.",
  "848310","Transmission shafts/cranks",
  "848320","Bearing housings",
  "848340","Gears & gearing",
  "848350","Pulleys/flywheels",
  "848360","Clutches/couplings",
  
  # 8517 telecom/network
  "851713","Smartphones",
  "851714","Mobile phones (non-smart)",
  "851761","Base stations",
  "851762","Network transmission/reception",
  "851769","Telecom equipment n.e.c.",
  
  # 8528 monitors/projectors/TV
  "852842","CRT monitors (ADP)",
  "852852","Flat monitors (ADP)",
  "852859","Monitors n.e.c.",
  "852862","Projectors (ADP)",
  "852869","Projectors n.e.c.",
  "852871","TV receivers, no screen",
  "852872","TV receivers, with screen",
  
  # 8544 insulated conductors
  "854411","Winding wire, copper",
  "854419","Winding wire, other",
  "854430","Ignition & wiring sets (vehicles)",
  "854442","≤1000V with connectors",
  "854449","≤1000V without connectors",
  "854460",">1000V insulated conductors",
  
  # 8701 tractors
  "870110","Single-axle tractors",
  "870121","Road tractors, diesel",
  "870123","Road tractors, hybrid (SI+EV)",
  "870124","Road tractors, electric",
  "870129","Road tractors, other",
  "870130","Track-laying tractors",
  "870191","Tractors n.e.c. ≤18kW",
  "870192","Tractors >18–37kW",
  "870193","Tractors >37–75kW",
  "870194","Tractors >75–130kW",
  "870195","Tractors >130kW",
  
  # 8703 passenger vehicles
  "870310","Snow/golf & similar vehicles",
  "870321","Cars, SI ≤1000cc",
  "870322","Cars, SI >1000–1500cc",
  "870323","Cars, SI >1500–3000cc",
  "870324","Cars, SI >3000cc",
  "870331","Cars, diesel ≤1500cc",
  "870332","Cars, diesel >1500–2500cc",
  "870333","Cars, diesel >2500cc",
  "870340","Hybrid (SI+EV), non-plug-in",
  "870350","Hybrid (diesel+EV), non-plug-in",
  "870360","PHEV (SI+EV)",
  "870370","PHEV (diesel+EV)",
  "870380","Battery EV",
  "870390","Passenger vehicles n.e.c.",
  
  # 8704 goods vehicles
  "870410","Dumpers, off-highway",
  "870421","Goods veh., diesel ≤5t GVW",
  "870422","Goods veh., diesel >5–20t GVW",
  "870423","Goods veh., diesel >20t GVW",
  "870431","Goods veh., SI ≤5t GVW",
  "870432","Goods veh., SI >5t GVW",
  "870441","Goods veh., SI ≤5t GVW (box/bulk)",
  "870442","Goods veh., SI >5t GVW (box/bulk)",
  "870451","Goods veh., diesel ≤5t GVW (box/bulk)",
  "870460","Goods veh., with lifting/handling",
  "870490","Goods vehicles n.e.c.",
  
  # 8708 vehicle parts
  "870810","Bumpers & parts",
  "870821","Seat belts",
  "870822","Windscreens & windows",
  "870829","Body parts/accessories n.e.c.",
  "870830","Brakes & parts",
  "870840","Gear boxes & parts",
  "870850","Drive-axles/differentials & parts",
  "870870","Road wheels & parts",
  "870880","Suspension & shock absorbers",
  "870891","Radiators & parts",
  "870892","Mufflers/exhaust pipes",
  "870893","Clutches & parts",
  "870894","Steering assemblies & parts",
  "870895","Airbags (with inflator) & parts",
  "870899","Parts & accessories n.e.c. (8708)",
  
  # 8716 trailers etc.
  "871610","Caravan trailers",
  "871620","Agricultural self-loading trailers",
  "871631","Tank trailers",
  "871639","Trailers, other",
  "871640","Semi-trailers",
  "871680","Other non-motor vehicles",
  "871690","Parts for 8716",
  
  # 8802 aircraft
  "880211","Helicopters ≤2000kg",
  "880212","Helicopters >2000kg",
  "880220","Aircraft ≤2000kg",
  "880230","Aircraft >2000–15,000kg",
  "880240","Aircraft >15,000kg",
  
  # 9401 seats
  "940110","Seats for aircraft",
  "940120","Seats for motor vehicles",
  "940131","Swivel seats, wood",
  "940139","Swivel seats, other",
  "940141","Convertible beds, wood",
  "940149","Convertible beds, other",
  "940152","Seats of bamboo",
  "940153","Seats of rattan",
  "940159","Seats of cane/osier/etc.",
  "940161","Wooden frame seats, upholstered",
  "940169","Wooden frame seats, not upholstered",
  "940171","Metal frame seats, upholstered",
  "940179","Metal frame seats, not upholstered",
  "940180","Seats n.e.c.",
  "940191","Seat parts, wood",
  
  # 9403 furniture
  "940310","Office furniture, metal",
  "940320","Furniture, metal (non-office)",
  "940330","Office furniture, wood",
  "940340","Kitchen furniture, wood",
  "940350","Bedroom furniture, wood",
  "940360","Furniture, wood (other)",
  "940370","Furniture, plastic",
  "940382","Furniture, bamboo",
  "940383","Furniture, rattan",
  "940389","Furniture, cane/osier/similar",
  "940391","Furniture parts, wood",
  "940399","Furniture parts, other"
)

# assign/replace the label
cca_all_imports <- cca_all_imports %>%
  left_join(hs6_lookup, by = "product_code") %>%
  mutate(product_label = coalesce(short_label, product_label)) %>%
  select(-short_label) %>% 
  select(year, product_code, product_level, hs4_code,
         product_label, hs4_rank, hs6_rank,  new_exporter,dropped_importer,
         dropped_exporter,  new_importer, Exporter_ISO_country,
         cca_notau, cca_tau)


# 
# db_host <- "127.0.0.1"
# db_user <- "gvcdtlab"
# db_password <- "gvcdtlab-datahub"
# db_name <- "gvcdtlab"
# db_port <- 3306
# 
# #Create a connection
# 
# con <- dbConnect(
#   drv = RMariaDB::MariaDB(),
#   host = db_host,
#   user = db_user,
#   password = db_password,
#   dbname = db_name,
#   port = db_port
# )
# 
# 
# # Write the data frame to the database
# copy_to(con, cca_all, "cca_h6", temporary = FALSE, overwrite = TRUE)
# 
# # Close the connection
# dbDisconnect(con)

qs::qsave(cca_all_imports, "import_cca.qs")



