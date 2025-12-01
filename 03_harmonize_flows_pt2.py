#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Nov 30 19:21:10 2025

@author: jeremyschneider
"""

# script_2_fill_tau_and_export.py

# -*- coding: utf-8 -*-
"""
Script 2: fill tau over time and write final per-year CCA parquet files.
"""

import os
import numpy as np
import pandas as pd

os.chdir("/Users/jeremyschneider/Desktop/inferred")
base_path = "."
years = list(range(2016, 2023))

path_out_intermediate = os.path.join(base_path, "clean", "flows")
path_out_final = os.path.join(base_path, "clean", "flows")

os.makedirs(path_out_final, exist_ok=True)

#---------------------------------------
# 1) Build slim tau panel across all years
#---------------------------------------
tau_frames = []

for yy in years:
    fpath = os.path.join(path_out_intermediate, f"flows_intermediate_{yy}.parquet")
    print(f"Reading tau cols from {fpath}")
    df = pd.read_parquet(
        fpath,
        columns=["exporter", "importer", "year", "hs6", "tau"]
    )
    tau_frames.append(df)

tau_panel = pd.concat(tau_frames, ignore_index=True)

# (Optional) compress memory
tau_panel["exporter"] = tau_panel["exporter"].astype("category")
tau_panel["importer"] = tau_panel["importer"].astype("category")
tau_panel["hs6"]      = tau_panel["hs6"].astype("category")

#---------------------------------------
# 2) Fill down, then up, tau by (exporter, importer, hs6) over time
#---------------------------------------
tau_panel = tau_panel.sort_values(
    ["exporter", "importer", "hs6", "year"]
).reset_index(drop=True)

group = tau_panel.groupby(["exporter", "importer", "hs6"], group_keys=False)

tau_panel["tau_down_work"] = group["tau"].ffill()
tau_panel["tau_after_down"] = np.where(
    tau_panel["tau"].isna(),
    tau_panel["tau_down_work"],
    tau_panel["tau"]
)

tau_panel["tau_up_work"] = group["tau_after_down"].bfill()
tau_panel["tau_final"] = np.where(
    tau_panel["tau_after_down"].isna(),
    tau_panel["tau_up_work"],
    tau_panel["tau_after_down"]
)

# clean up
tau_panel = tau_panel.drop(columns=["tau_down_work", "tau_up_work", "tau_after_down"])

# tau: NA -> 0, then /100
tau_panel["tau_final"] = tau_panel["tau_final"].fillna(0)
tau_panel["tau_final"] = tau_panel["tau_final"] / 100.0

#---------------------------------------
# 3) For each year: merge filled tau back & write final files
#---------------------------------------
for yy in years:
    print(f"=== Finalizing year {yy} ===")
    # tau for that year
    tau_y = tau_panel[tau_panel["year"] == yy][
        ["exporter", "importer", "year", "hs6", "tau_final"]
    ].copy()

    # read intermediate flows
    fpath = os.path.join(path_out_intermediate, f"flows_intermediate_{yy}.parquet")
    flows_y = pd.read_parquet(fpath)

    # merge tau
    flows_y = flows_y.drop(columns=["tau"], errors="ignore")
    flows_y = flows_y.merge(
        tau_y,
        on=["exporter", "importer", "year", "hs6"],
        how="left"
    )
    flows_y = flows_y.rename(columns={"tau_final": "tau"})

    # drop primary_value as in R final_all step
    flows_y = flows_y.drop(columns=["primary_value"], errors="ignore")

    # reorder columns similar to R select(1:5,9,8,6:7,10:19)
    cols_final = [
        "exporter", "importer", "year", "hs6", "hs6_desc",
        "freight", "tau", "fob", "kg",
        "exp_prodvalue_toworld", "exp_prodkg_toworld",
        "imp_prodvalue_fromworld", "imp_prodkg_fromworld",
        "exp_totalvalue_toworld", "exp_totalkg_toworld",
        "imp_totalvalue_fromworld", "imp_totalkg_fromworld",
        "exp_totalvalue_toimp", "exp_totalkg_toimp",
    ]
    cols_existing = [c for c in cols_final if c in flows_y.columns]
    flows_y = flows_y[cols_existing]

    out_path = os.path.join(
        path_out_final, f"allflows_forCCAs_{yy}.parquet"
    )
    flows_y.to_parquet(out_path, index=False)
    print(f"  Wrote {out_path}")
