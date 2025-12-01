#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Nov 30 19:19:20 2025

@author: jeremyschneider
"""

# script_1_build_intermediate.py

# -*- coding: utf-8 -*-
"""
Script 1: per-year merge of trade flows, tariffs, and transport costs.
Outputs one intermediate parquet per year, WITHOUT tau fill.
"""

import os
import numpy as np
import pandas as pd

#---------------------------------------
# Years
#---------------------------------------
os.chdir("/Users/jeremyschneider/Desktop/inferred")
base_path = os.path.join(".")
years = list(range(2016, 2023))

#---------------------------------------
# Paths (adjust as needed)
#---------------------------------------
path_comtrade = os.path.join(base_path, "raw", "comtrade")
path_wits     = os.path.join(base_path, "raw", "wits")
path_oecd_tc  = os.path.join(base_path, "raw", "oecd")
path_out      = os.path.join(base_path, "clean", "flows")
os.makedirs(path_out, exist_ok=True)

#---------------------------------------
# Constants / helpers
#---------------------------------------
toosmall_territories = [
    "Åland Islands ","American Samoa","Antarctica","Bonaire","Bouvet Island","Br. Antarctic Terr.",
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
    "United States of America","Wallis and Futuna Isds","Western Sahara"
]

eu_countries = [
    "Austria","Belgium","Bulgaria","Croatia","Cyprus","Czechia","Denmark","Estonia",
    "Finland","France","Germany","Greece","Hungary","Ireland","Italy","Latvia","Lithuania",
    "Luxembourg","Malta","Netherlands","Poland","Portugal","Romania",
    "Slovakia","Slovenia","Spain","Sweden"
]

rename_map = {
    "USA":                              "United States",
    "Russian Federation":               "Russia",
    "United Rep. of Tanzania":          "Tanzania",
    "Rep. of Korea":                    "South Korea",
    "China, Hong Kong SAR":             "Hong Kong",
    "China, Macao SAR":                 "Macau",
    "Bolivia (Plurinational State of)": "Bolivia",
    "Viet Nam":                         "Vietnam",
    "Brunei Darussalam":                "Brunei",
    "Lao People's Dem. Rep.":           "Laos",
    "Dem. People's Rep. of Korea":      "North Korea",
    "Dem. Rep. of the Congo":          "DR Congo",
    "Côte d'Ivoire":                    "Ivory Coast",
    "Bosnia Herzegovina":               "Bosnia and Herzegovina",
    "Rep. of Moldova":                  "Moldova",
    "Dominican Rep.":                   "Dominican Republic",
    "Cayman Isds":                      "Cayman Islands",
    "Faroe Isds":                       "Faroe Islands",
    "Solomon Isds":                     "Solomon Islands",
    "Cabo Verde":                       "Cape Verde",
    "Timor-Leste":                      "East Timor",
    "State of Palestine":               "Palestine",
    "Central African Rep.":             "Central African Republic",
    "Saint Kitts and Nevis":            "St. Kitts and Nevis",
    "Saint Vincent and the Grenadines": "St. Vincent and the Grenadines",
    "Saint Lucia":                      "St. Lucia"
}

rename_tariffs = {
    "Korea, Rep.":                 "South Korea",
    "Slovak Republic":             "Slovakia",
    "Czech Republic":              "Czechia",
    "Russian Federation":          "Russia",
    "Congo, Dem. Rep.":            "DR Congo",
    "Congo, Rep.":                 "Congo",
    "Iran, Islamic Rep.":          "Iran",
    "Lao PDR":                     "Laos",
    "Kyrgyz Republic":             "Kyrgyzstan",
    "Cote d'Ivoire":               "Ivory Coast",
    "Hong Kong, China":            "Hong Kong",
    "Macao":                       "Macau",
    "Egypt, Arab Rep.":            "Egypt",
    "Bahamas, The":                "Bahamas",
    "Gambia, The":                 "Gambia",
    "Serbia, FR(Serbia/Montenegro)": "Serbia",
    "Syrian Arab Republic":        "Syria",
    "Ethiopia(excludes Eritrea)":  "Ethiopia",
    "Turkey":                      "Türkiye",
    "Korea, Dem. Rep.":            "North Korea"
}

rename_tc = {
    "Myanmar (Burma)":          "Myanmar",
    "Bosnia & Herzegovina":     "Bosnia and Herzegovina",
    "Trinidad & Tobago":        "Trinidad and Tobago",
    "Congo - Kinshasa":         "DR Congo",
    "Congo - Brazzaville":      "Congo",
    "Turkey":                   "Türkiye",
    "Timor-Leste":              "East Timor",
    "St. Vincent & Grenadines": "St. Vincent and the Grenadines",
    "St. Kitts & Nevis":        "St. Kitts and Nevis",
    "Hong Kong SAR China":      "Hong Kong",
    "Antigua & Barbuda":        "Antigua and Barbuda"
}

def normalize_with_map(series: pd.Series, mapping: dict) -> pd.Series:
    return series.replace(mapping)

#---------------------------------------
# Load transport costs once & normalize
#---------------------------------------
tc_parquet_path = os.path.join(path_oecd_tc, "transport_costs.parquet")
tc = pd.read_parquet(tc_parquet_path)
tc = tc.rename(columns={"hs": "hs4"})
tc["exporter"] = normalize_with_map(tc["exporter"], rename_tc)
tc["importer"] = normalize_with_map(tc["importer"], rename_tc)

#---------------------------------------
# MAIN LOOP — per year, write intermediate file
#---------------------------------------
for yy in years:
    print(f"=== Year {yy} ===")

    # 1) Flows
    flows_y = pd.read_parquet(
        os.path.join(path_comtrade, f"bulktrade_hs6_{yy}.parquet")
    )

    flows_y = (
        flows_y[["exporter", "importer", "period", "hs6", "hs6_desc",
                 "primary_value", "kg"]]
        .rename(columns={"period": "year"})
    )
    flows_y["year"] = pd.to_numeric(flows_y["year"], errors="coerce")

    flows_y = flows_y[
        (~flows_y["exporter"].isin(toosmall_territories)) &
        (~flows_y["importer"].isin(toosmall_territories)) &
        (flows_y["kg"].notna()) &
        (flows_y["kg"] > 0) &
        (flows_y["primary_value"].notna())
    ].copy()

    flows_y["exporter"] = normalize_with_map(flows_y["exporter"], rename_map)
    flows_y["importer"] = normalize_with_map(flows_y["importer"], rename_map)

    # 2) Tariffs
    tariffs_y = pd.read_parquet(
        os.path.join(path_wits, f"tariffs_hs6_{yy}.parquet")
    )
    tariffs_y = tariffs_y[["exporter", "importer", "year", "hs", "tau"]].copy()

    tariffs_y["exporter"] = normalize_with_map(tariffs_y["exporter"], rename_tariffs)
    tariffs_y["importer"] = normalize_with_map(tariffs_y["importer"], rename_tariffs)

    mask_eu_eu = tariffs_y["exporter"].isin(eu_countries) & tariffs_y["importer"].isin(eu_countries)
    tariffs_y.loc[mask_eu_eu, "tau"] = 0

    mask_uk_eu = (
        ((tariffs_y["exporter"] == "United Kingdom") & tariffs_y["importer"].isin(eu_countries)) |
        ((tariffs_y["importer"] == "United Kingdom") & tariffs_y["exporter"].isin(eu_countries))
    ) & (tariffs_y["year"] < 2021)
    tariffs_y.loc[mask_uk_eu, "tau"] = 0

    eu_pool_y = (
        tariffs_y[tariffs_y["importer"].isin(eu_countries)]
        .groupby(["exporter", "year", "hs"], as_index=False)
        .agg(tau_eu=("tau", "mean"))
    )

    tariffs_y = tariffs_y.merge(eu_pool_y, on=["exporter", "year", "hs"], how="left")
    mask_eu_missing = tariffs_y["importer"].isin(eu_countries) & tariffs_y["tau"].isna()
    tariffs_y.loc[mask_eu_missing, "tau"] = tariffs_y.loc[mask_eu_missing, "tau_eu"]
    tariffs_y = tariffs_y.drop(columns=["tau_eu"])

    tariffs_y = tariffs_y.drop_duplicates(subset=["importer", "exporter", "year", "hs"], keep="first")

    # 3) Merge flows + tariffs + tc for this year
    flows3_y = flows_y.merge(
        tariffs_y,
        left_on=["exporter", "importer", "year", "hs6"],
        right_on=["exporter", "importer", "year", "hs"],
        how="left"
    )
    flows3_y["hs4"] = flows3_y["hs6"].str[:4]

    tc_yy = tc[tc["year"] == yy]
    flows_final_y = flows3_y.merge(
        tc_yy,
        on=["importer", "exporter", "year", "hs4"],
        how="left",
        suffixes=("", "_tc")
    ).drop(columns=["hs4", "hs"])

    # keep only rows with non-missing freight
    flows_final_y = flows_final_y[flows_final_y["freight"].notna()].copy()

    # freight proportion
    flows_final_y["freight"] = flows_final_y["freight"] / 100.0

    # fob
    mask_non_special = ~flows_final_y["importer"].isin(["Canada", "Bermuda", "South Africa"])
    flows_final_y["fob"] = flows_final_y["primary_value"]
    flows_final_y.loc[mask_non_special, "fob"] = (
        flows_final_y.loc[mask_non_special, "primary_value"]
        - flows_final_y.loc[mask_non_special, "primary_value"]
        * flows_final_y.loc[mask_non_special, "freight"]
    )

    # cost + freight for special importers
    flows_final_y["cost"] = flows_final_y["primary_value"]
    mask_special = flows_final_y["importer"].isin(["Canada", "Bermuda", "South Africa"])
    flows_final_y.loc[mask_special, "cost"] = (
        flows_final_y.loc[mask_special, "primary_value"]
        / (1 - flows_final_y.loc[mask_special, "freight"])
    )
    flows_final_y["freight"] = flows_final_y["cost"] - flows_final_y["fob"]

    flows_final_y = flows_final_y.drop(columns=["cost"])  # keep primary_value for tau step

    # Aggregates – all include year in groups, so safe per year
    flows_final_y["exp_prodvalue_toworld"] = (
        flows_final_y.groupby(["exporter", "year", "hs6"])["fob"].transform("sum")
    )
    flows_final_y["exp_prodkg_toworld"] = (
        flows_final_y.groupby(["exporter", "year", "hs6"])["kg"].transform("sum")
    )

    flows_final_y["imp_prodvalue_fromworld"] = (
        flows_final_y.groupby(["importer", "year", "hs6"])["fob"].transform("sum")
    )
    flows_final_y["imp_prodkg_fromworld"] = (
        flows_final_y.groupby(["importer", "year", "hs6"])["kg"].transform("sum")
    )

    flows_final_y["exp_totalvalue_toworld"] = (
        flows_final_y.groupby(["exporter", "year"])["fob"].transform("sum")
    )
    flows_final_y["exp_totalkg_toworld"] = (
        flows_final_y.groupby(["exporter", "year"])["kg"].transform("sum")
    )

    flows_final_y["imp_totalvalue_fromworld"] = (
        flows_final_y.groupby(["importer", "year"])["fob"].transform("sum")
    )
    flows_final_y["imp_totalkg_fromworld"] = (
        flows_final_y.groupby(["importer", "year"])["kg"].transform("sum")
    )

    flows_final_y["exp_totalvalue_toimp"] = (
        flows_final_y.groupby(["exporter", "importer", "year"])["fob"].transform("sum")
    )
    flows_final_y["exp_totalkg_toimp"] = (
        flows_final_y.groupby(["exporter", "importer", "year"])["kg"].transform("sum")
    )

    # Write intermediate per-year file (tau still raw, not filled or scaled)
    out_path = os.path.join(path_out, f"flows_intermediate_{yy}.parquet")
    flows_final_y.to_parquet(out_path, index=False)
    print(f"  Saved intermediate: {out_path}")
