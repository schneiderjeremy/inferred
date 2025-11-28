library(readr)
library(dplyr)
library(tidyr)
library(purrr)
library(arrow) 
rm(list = ls())

years <- 2016:2022

#---------------------------------------
# Paths (adjust as needed)
#---------------------------------------
path_comtrade <- "~/models_v2/transportcanada/raw_data/comtrade"
path_wits     <- "~/models_v2/transportcanada/raw_data/wits"
path_oecd_tc  <- "~/models_v2/transportcanada/raw_data/oecd/transport_cost"

#---------------------------------------
# Constants / helpers
#---------------------------------------
toosmall_territories <- c("Åland Islands ","American Samoa","Antarctica","Bonaire","Bouvet Island","Br. Antarctic Terr.",
  "Br. Indian Ocean Terr.","Br. Virgin Isds","Christmas Isds","Cocos Isds","Cook Isds","Curaçao",
  "Europe EU, nes","Faeroe Isds","Falkland Isds (Malvinas)","Fr. South Antarctic Terr.","FS Micronesia",
  "Heard Island and McDonald Islands","French Polynesia","Gibraltar","Guam","Guernsey",
  "Heard Island and McDonald Islands","Holy See (Vatican City State)","Isle of Man ","Jersey","Libya",
  "Liechtenstein ","Marshall Isds","Martinique (Overseas France)","Metropolitan France","Montserrat",
  "Neutral Zone","New Caledonia","N. Mariana Isds","Norfolk Isds",
  "Norway, excluding Svalbard and Jan Mayen","Niue","Pitcairn","Puerto Rico ",
  "Saint Barthélemy","Saint Helena","Saint Maarten","Saint Martin (French part) ",
  "Saint Pierre and Miquelon","South Georgia and the South Sandwich Islands",
  "Svalbard and Jan Mayen Islands ","Switzerland ","Taiwan, Province of China","Tokelau",
  "Turks and Caicos Isds","United States Minor Outlying Islands","US Misc. Pacific Isds",
  "United States of America","Wallis and Futuna Isds","Western Sahara")

eu_countries <- c("Austria","Belgium","Bulgaria","Croatia","Cyprus","Czechia","Denmark","Estonia",
                  "Finland","France","Germany","Greece","Hungary","Ireland","Italy","Latvia","Lithuania",
                  "Luxembourg","Malta","Netherlands","Poland","Portugal","Romania","Slovakia","Slovenia","Spain","Sweden")

# Name normalizations
rename_map <- c(
  "USA"                              = "United States",
  "Russian Federation"               = "Russia",
  "United Rep. of Tanzania"          = "Tanzania",
  "Rep. of Korea"                    = "South Korea",
  "China, Hong Kong SAR"             = "Hong Kong",
  "China, Macao SAR"                 = "Macau",
  "Bolivia (Plurinational State of)" = "Bolivia",
  "Viet Nam"                         = "Vietnam",
  "Brunei Darussalam"                = "Brunei",
  "Lao People's Dem. Rep."           = "Laos",
  "Dem. People's Rep. of Korea"      = "North Korea",
  "Dem. Rep. of the Congo"           = "DR Congo",
  "Côte d'Ivoire"                    = "Ivory Coast",
  "Bosnia Herzegovina"               = "Bosnia and Herzegovina",
  "Rep. of Moldova"                  = "Moldova",
  "Dominican Rep."                   = "Dominican Republic",
  "Cayman Isds"                      = "Cayman Islands",
  "Faroe Isds"                       = "Faroe Islands",
  "Solomon Isds"                     = "Solomon Islands",
  "Cabo Verde"                       = "Cape Verde",
  "Timor-Leste"                      = "East Timor",
  "State of Palestine"               = "Palestine",
  "Central African Rep."             = "Central African Republic",
  "Saint Kitts and Nevis"            = "St. Kitts and Nevis",
  "Saint Vincent and the Grenadines" = "St. Vincent and the Grenadines",
  "Saint Lucia"                      = "St. Lucia"
)

rename_tariffs <- c(
  "Korea, Rep."          = "South Korea",
  "Slovak Republic"      = "Slovakia",
  "Czech Republic"       = "Czechia",
  "Russian Federation"   = "Russia",
  "Congo, Dem. Rep."     = "DR Congo",
  "Congo, Rep."          = "Congo",
  "Iran, Islamic Rep."   = "Iran",
  "Lao PDR"              = "Laos",
  "Kyrgyz Republic"      = "Kyrgyzstan",
  "Cote d'Ivoire"        = "Ivory Coast",
  "Hong Kong, China"     = "Hong Kong",
  "Macao"                = "Macau",
  "Egypt, Arab Rep."     = "Egypt",
  "Bahamas, The"         = "Bahamas",
  "Gambia, The"          = "Gambia",
  "Serbia, FR(Serbia/Montenegro)" = "Serbia",
  "Syrian Arab Republic" = "Syria",
  "Ethiopia(excludes Eritrea)" = "Ethiopia",
  "Turkey"               = "Türkiye",
  "Korea, Dem. Rep."     = "North Korea"
)

rename_tc <- c(
  "Myanmar (Burma)"              = "Myanmar",
  "Bosnia & Herzegovina"         = "Bosnia and Herzegovina",
  "Trinidad & Tobago"            = "Trinidad and Tobago",
  "Congo - Kinshasa"             = "DR Congo",
  "Congo - Brazzaville"          = "Congo",
  "Turkey"                       = "Türkiye",
  "Timor-Leste"                  = "East Timor",
  "St. Vincent & Grenadines"     = "St. Vincent and the Grenadines",
  "St. Kitts & Nevis"            = "St. Kitts and Nevis",
  "Hong Kong SAR China"          = "Hong Kong",
  "Antigua & Barbuda"            = "Antigua and Barbuda"
)

normalize_with_map <- function(x, map) {
  x_chr <- as.character(x)
  hits  <- map[x_chr]
  x_chr[!is.na(hits)] <- hits[!is.na(hits)]
  if (is.factor(x)) factor(x_chr) else x_chr
}

#---------------------------------------
# Load transport costs once & normalize
#---------------------------------------
tc <- arrow::read_parquet(file.path(path_oecd_tc, "transport_costs.parquet")) %>%
  rename(hs4 = hs) 
tc$exporter <- normalize_with_map(tc$exporter, rename_tc)
tc$importer <- normalize_with_map(tc$importer, rename_tc)

#---------------------------------------
# MAIN LOOP — creates flows_final_YYYY objects
#---------------------------------------
results <- vector("list", length(years))
names(results) <- as.character(years)

for (yy in years) {
  # 1) Flows for year yy
  flows_y <- arrow::read_parquet(file.path(path_comtrade, paste0("bulktrade_hs6_", yy, ".parquet"))) %>% 
    select(exporter, importer, period, hs6, hs6_desc, primary_value, kg) %>%
    filter(!exporter %in% toosmall_territories,
           !importer %in% toosmall_territories) %>%
    rename(year = period) %>%
    mutate(year = as.numeric(year)) %>%
    filter(!is.na(kg), kg > 0, !is.na(primary_value)) 
  
  flows_y$exporter <- normalize_with_map(flows_y$exporter, rename_map)
  flows_y$importer <- normalize_with_map(flows_y$importer, rename_map)
  
  # 2) Tariffs for year yy
  tariffs_y <- arrow::read_parquet(file.path(path_wits, paste0("tariffs_hs6_", yy, ".parquet"))) %>%  
    select(exporter, importer, year, hs, tau) 
  
  tariffs_y$exporter <- normalize_with_map(tariffs_y$exporter, rename_tariffs)
  tariffs_y$importer <- normalize_with_map(tariffs_y$importer, rename_tariffs)
  
  tariffs_y <- tariffs_y %>%
    mutate(
      tau = ifelse(exporter %in% eu_countries & importer %in% eu_countries, 0, tau),
      tau = ifelse(
        (exporter == "United Kingdom" & importer %in% eu_countries & year < 2021) |
          (importer == "United Kingdom" & exporter %in% eu_countries & year < 2021),
        0, tau
      )
    )
  
  eu_pool_y <- tariffs_y %>%
    filter(importer %in% eu_countries) %>%
    group_by(exporter, year, hs) %>%
    summarise(tau_eu = mean(tau, na.rm = TRUE), .groups = "drop") %>%
    mutate(tau_eu = ifelse(is.nan(tau_eu), NA_real_, tau_eu))
  
  tariffs_y <- tariffs_y %>%
    left_join(eu_pool_y, by = c("exporter","year","hs")) %>%
    mutate(tau = ifelse(importer %in% eu_countries & is.na(tau), tau_eu, tau)) %>%
    select(-tau_eu) %>%
    distinct(importer, exporter, year, hs, .keep_all = TRUE)
  
  # 3) Merge flows + tariffs + transport costs (year yy only)
  flows3_y <- flows_y %>%
    left_join(tariffs_y, by = c("exporter","importer","year","hs6" = "hs")) %>%
    mutate(hs4 = substr(hs6, 1, 4))
  
  flows_final_y <- flows3_y %>%
    left_join(filter(tc, year == yy), by = c("importer","exporter","year","hs4")) %>% 
    select(-hs4) 
  
  # Create the object flows_final_YYYY in the environment
  obj_name <- paste0("flows_final_", yy)
  assign(obj_name, flows_final_y, envir = .GlobalEnv)
  }

final_all <- bind_rows(mget(paste0("flows_final_", years))) 
final_all <- final_all %>% 
  subset(.,!is.na(freight)) %>% 
  mutate(freight = freight / 100) %>% 
  mutate(fob = ifelse(!importer %in% c("Canada","Bermuda","South Africa"), primary_value - (primary_value * freight),primary_value)) %>% 
  
  # For Canada Bermuda and SA
  mutate(cost = ifelse(importer %in% c("Canada","Bermuda","South Africa"), primary_value / (1 - freight),primary_value)) %>% 
  mutate(freight = cost - fob) %>% 
  select(-cost,-primary_value) 

final_all <- final_all %>% 
  group_by(exporter,year,hs6) %>% 
  mutate(exp_prodvalue_toworld = sum(fob,na.rm=TRUE), exp_prodkg_toworld = sum(kg)) %>%
  group_by(importer,year,hs6) %>%
  mutate(imp_prodvalue_fromworld = sum(fob,na.rm=TRUE), imp_prodkg_fromworld = sum(kg)) %>% 
  group_by(exporter,year) %>% 
  mutate(exp_totalvalue_toworld = sum(fob,na.rm=TRUE), exp_totalkg_toworld = sum(kg)) %>%
  group_by(importer,year) %>%
  mutate(imp_totalvalue_fromworld = sum(fob,na.rm=TRUE), imp_totalkg_fromworld = sum(kg)) %>%
  ungroup() %>% 
  group_by(exporter,importer,year) %>% 
  mutate(exp_totalvalue_toimp = sum(fob,na.rm=TRUE), exp_totalkg_toimp = sum(kg)) %>%
  ungroup()
  
  
rm(flows_final_2016,flows_final_2017,flows_final_2018,flows_final_2019,flows_final_2020,flows_final_2021,flows_final_2022,flows_final_y,flows_y,flows3_y,tariffs_y,tc,eu_pool_y)


### Fill down (then up) tau rates from non-missing years in order to fill in missing years ###
#---------------------------------------------------------------------------------------------
filled <- final_all %>%
  arrange(exporter, importer, hs6, year) %>%
  group_by(exporter, importer, hs6) %>%
  # --- Pass 1: fill DOWN from prior years
  mutate(tau_down_work = tau) %>%
  fill(tau_down_work, .direction = "down") %>%
  mutate(
    tau_after_down = if_else(is.na(tau), tau_down_work, tau),
    filled_down    = is.na(tau) & !is.na(tau_down_work)          # audit flag
  ) %>%
  # --- Pass 2: fill UP only where still missing after pass 1
  mutate(tau_up_work = tau_after_down) %>%
  fill(tau_up_work, .direction = "up") %>%
  mutate(
    tau_final   = if_else(is.na(tau_after_down), tau_up_work, tau_after_down),
    filled_up   = is.na(tau) & is.na(tau_down_work) & !is.na(tau_up_work)  # audit flag
  ) %>%
  ungroup() %>%
  select(-tau_down_work, -tau_up_work)

# Bring back into main frame
final_all2 <- filled %>% 
  mutate(tau = tau_final) %>%
  select(-tau_after_down, -tau_final,-filled_down,-filled_up) %>%
  mutate(tau = ifelse(is.na(tau),0,tau)) %>% 
  mutate(tau = tau / 100) %>% 
  select(1:5,9,8,6:7,10:19)


for(i in 2016:2022){
  flows_final_y <- final_all2 %>%
    filter(year == i)
  obj_name <- paste0("flows_final_", i)
  assign(obj_name, flows_final_y, envir = .GlobalEnv)

}

arrow::write_parquet(flows_final_2016,"~/models_v2/transportcanada/clean_data_app/cca/allflows_forCCAs_2016.parquet")
arrow::write_parquet(flows_final_2017,"~/models_v2/transportcanada/clean_data_app/cca/allflows_forCCAs_2017.parquet")
arrow::write_parquet(flows_final_2018,"~/models_v2/transportcanada/clean_data_app/cca/allflows_forCCAs_2018.parquet")
arrow::write_parquet(flows_final_2019,"~/models_v2/transportcanada/clean_data_app/cca/allflows_forCCAs_2019.parquet")
arrow::write_parquet(flows_final_2020,"~/models_v2/transportcanada/clean_data_app/cca/allflows_forCCAs_2020.parquet")
arrow::write_parquet(flows_final_2021,"~/models_v2/transportcanada/clean_data_app/cca/allflows_forCCAs_2021.parquet")
arrow::write_parquet(flows_final_2022,"~/models_v2/transportcanada/clean_data_app/cca/allflows_forCCAs_2022.parquet")
