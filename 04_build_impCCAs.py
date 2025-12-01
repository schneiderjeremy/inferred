"""
Build CCA export table from allflows_forCCAs_{year}.parquet
for years 2016–2022 (currently 2022 only).

Inputs (per year):
  clean/flows/allflows_forCCAs_YYYY.parquet

Outputs:
  clean/ccas/export_cca.parquet

Side effect:
  deletes clean/flows/flows_intermediate_YYYY.parquet if present.
"""

import os
import numpy as np
import pandas as pd
import pycountry  # assumes installed

# -------------------------------------------------------------------
# CONFIG
# -------------------------------------------------------------------
BASE_PATH = "/Users/jeremyschneider/Desktop/inferred"  # adjust if needed
os.chdir(BASE_PATH)

# years = list(range(2016, 2023))   # if you want 2016..2022
years = list(range(2016, 2020))     # currently only 2022

path_flows = os.path.join(BASE_PATH, "clean", "flows")
path_cca   = os.path.join(BASE_PATH, "clean", "ccas")
os.makedirs(path_cca, exist_ok=True)

# -------------------------------------------------------------------
# CONSTANTS
# -------------------------------------------------------------------
eu_countries = [
    "Austria","Belgium","Bulgaria","Croatia","Cyprus","Czechia","Denmark","Estonia",
    "Finland","France","Germany","Greece","Hungary","Ireland","Italy","Latvia","Lithuania",
    "Luxembourg","Malta","Netherlands (Kingdom of the)","Poland","Portugal","Romania",
    "Slovak Republic","Slovenia","Spain","Sweden"
]

# HS4 descriptions for exports
HS4_DESC_EXPORT = {
    "0203": "Meat of swine",
    "0306": "Crustaceans",
    "0713": "Dried pulses",
    "1001": "Wheat & meslin",
    "1205": "Rape or colza seeds",
    "1514": "Rape, colza, mustard oil",
    "1905": "Baked food",
    "2601": "Iron ores",
    "2603": "Copper ores",
    "2701": "Coal briquettes",
    "2709": "Crude oil",
    "2710": "Petroleum oils",
    "2711": "Petroleum gases",
    "2713": "Petroleum coke and bitumen",
    "3004": "Medicaments",
    "3104": "Mineral or chemical fertilizers",
    "3901": "Polymers of ethylene",
    "4407": "Wood products",
    "4410": "Particle board and OSB",
    "4703": "Chemical wood pulp",
    "7102": "Unmounted diamonds",
    "7108": "Unwrought / semi-manufactured gold",
    "7501": "Nickel mattes, sinters",
    "7601": "Unwrought aluminium",
    "8411": "Jet engines",
    "8703": "Passenger vehicles",
    "8704": "Cargo trucks",
    "8708": "Vehicle parts",
    "8802": "Aircrafts",
    "9403": "Furniture"
}

# -------------------------------------------------------------------
# HS6 SHORT LABELS (EXPORT SIDE)
# -------------------------------------------------------------------
hs6_lookup_exports = pd.DataFrame([
    # --- Pork ---
    {"product_code": "020311", "short_label": "Pork carcasses/half, fresh"},
    {"product_code": "020312", "short_label": "Pork ham/shoulder bone-in, fresh"},
    {"product_code": "020319", "short_label": "Pork, other cuts n.e.c., fresh"},
    {"product_code": "020321", "short_label": "Pork carcasses/half, frozen"},
    {"product_code": "020322", "short_label": "Pork ham/shoulder bone-in, frozen"},
    {"product_code": "020329", "short_label": "Pork, other cuts n.e.c., frozen"},

    # --- Crustaceans (0306) ---
    {"product_code": "030611", "short_label": "Rock lobster etc., frozen (any prep)"},
    {"product_code": "030612", "short_label": "Lobster, frozen (any prep)"},
    {"product_code": "030614", "short_label": "Crab, frozen (any prep)"},
    {"product_code": "030615", "short_label": "Norway lobster, frozen (any prep)"},
    {"product_code": "030616", "short_label": "Shrimp/prawn cold-water, frozen (any prep)"},
    {"product_code": "030617", "short_label": "Shrimp/prawn excl. cold-water, frozen (any prep)"},
    {"product_code": "030619", "short_label": "Crustaceans n.e.c., frozen/meal"},
    {"product_code": "030631", "short_label": "Rock lobster etc., live/fresh/chilled"},
    {"product_code": "030632", "short_label": "Lobster, live/fresh/chilled"},
    {"product_code": "030633", "short_label": "Crab, live/fresh/chilled"},
    {"product_code": "030634", "short_label": "Norway lobster, live/fresh/chilled"},
    {"product_code": "030635", "short_label": "Shrimp/prawn cold-water, live/fresh/chilled"},
    {"product_code": "030636", "short_label": "Shrimp/prawn excl. cold-water, live/fresh/chilled"},
    {"product_code": "030691", "short_label": "Rock lobster etc., smoked (any prep)"},
    {"product_code": "030692", "short_label": "Lobster, smoked (any prep)"},
    {"product_code": "030693", "short_label": "Crab, smoked (any prep)"},
    {"product_code": "030695", "short_label": "Shrimp/prawn, smoked (any prep)"},
    {"product_code": "030699", "short_label": "Crustaceans n.e.c., smoked/meal"},

    # --- Dried pulses (0713) ---
    {"product_code": "071310", "short_label": "Peas, dried"},
    {"product_code": "071320", "short_label": "Chickpeas, dried"},
    {"product_code": "071331", "short_label": "Mung/green gram, dried"},
    {"product_code": "071332", "short_label": "Adzuki beans, dried"},
    {"product_code": "071333", "short_label": "Kidney/white pea beans, dried"},
    {"product_code": "071334", "short_label": "Bambara beans, dried"},
    {"product_code": "071335", "short_label": "Cowpeas, dried"},
    {"product_code": "071339", "short_label": "Beans n.e.c., dried"},
    {"product_code": "071340", "short_label": "Lentils, dried"},
    {"product_code": "071350", "short_label": "Broad/horse beans, dried"},
    {"product_code": "071360", "short_label": "Pigeon peas, dried"},
    {"product_code": "071390", "short_label": "Legumes n.e.c., dried"},

    # --- Wheat (1001) ---
    {"product_code": "100111", "short_label": "Durum wheat, seed"},
    {"product_code": "100119", "short_label": "Durum wheat, other"},
    {"product_code": "100191", "short_label": "Other wheat/meslin, seed"},
    {"product_code": "100199", "short_label": "Other wheat/meslin, other"},

    # --- Rapeseed/Canola (1205, 1514) ---
    {"product_code": "120510", "short_label": "Rapeseed/canola LER, seed"},
    {"product_code": "120590", "short_label": "Rapeseed, other than LER, seed"},
    {"product_code": "151411", "short_label": "Canola/rapeseed oil LER, crude"},
    {"product_code": "151419", "short_label": "Canola/rapeseed oil LER, other"},
    {"product_code": "151491", "short_label": "Rapeseed oil excl. LER, crude"},
    {"product_code": "151499", "short_label": "Rapeseed oil excl. LER, other"},

    # --- Baked goods (1905) ---
    {"product_code": "190510", "short_label": "Crispbread"},
    {"product_code": "190520", "short_label": "Gingerbread etc."},
    {"product_code": "190531", "short_label": "Sweet biscuits"},
    {"product_code": "190532", "short_label": "Waffles/wafers"},
    {"product_code": "190540", "short_label": "Rusks/toasted bread"},
    {"product_code": "190590", "short_label": "Bakers’ wares n.e.c.; wafers etc."},

    # --- Ores (2601, 2603) ---
    {"product_code": "260111", "short_label": "Iron ore, non-agglomerated"},
    {"product_code": "260112", "short_label": "Iron ore, agglomerated"},
    {"product_code": "260120", "short_label": "Iron pyrites, roasted"},
    {"product_code": "260300", "short_label": "Copper ores & conc."},

    # --- Coal (2701) ---
    {"product_code": "270111", "short_label": "Anthracite (not agglomerated)"},
    {"product_code": "270112", "short_label": "Coal, bituminous (not agglomerated)"},
    {"product_code": "270119", "short_label": "Coal, other (not agglomerated)"},
    {"product_code": "270120", "short_label": "Coal briquettes/ovoid fuels"},

    # --- Petroleum (2709, 2710, 2711, 2713) ---
    {"product_code": "270900", "short_label": "Crude petroleum oils"},
    {"product_code": "271011", "short_label": "Refined petroleum, light oils/preps"},
    {"product_code": "271012", "short_label": "Refined petroleum (no biodiesel), light oils/preps"},
    {"product_code": "271019", "short_label": "Refined petroleum, other than light oils"},
    {"product_code": "271020", "short_label": "Refined petroleum with biodiesel"},
    {"product_code": "271099", "short_label": "Waste oils (petroleum), n.e.c."},
    {"product_code": "271111", "short_label": "LNG (natural gas, liquefied)"},
    {"product_code": "271112", "short_label": "Propane, liquefied"},
    {"product_code": "271113", "short_label": "Butanes, liquefied"},
    {"product_code": "271114", "short_label": "Ethylene/propylene/butylene/butadiene, liquefied"},
    {"product_code": "271119", "short_label": "Petroleum gases, liquefied n.e.c."},
    {"product_code": "271129", "short_label": "Petroleum gases, gaseous n.e.c."},
    {"product_code": "271311", "short_label": "Petroleum coke, not calcined"},
    {"product_code": "271312", "short_label": "Petroleum coke, calcined"},
    {"product_code": "271320", "short_label": "Petroleum bitumen"},
    {"product_code": "271390", "short_label": "Petroleum residues n.e.c."},

    # --- Radioactive (2844) ---
    {"product_code": "284410", "short_label": "Natural uranium & compounds etc."},
    {"product_code": "284420", "short_label": "Enriched U-235/plutonium & compounds"},
    {"product_code": "284430", "short_label": "Depleted U-235/thorium & compounds"},
    {"product_code": "284440", "short_label": "Radioactive elements/isotopes n.e.c."},

    # --- Medicaments (3004 + extras) ---
    {"product_code": "300410", "short_label": "Medicaments with penicillins/streptomycins"},
    {"product_code": "300420", "short_label": "Medicaments with other antibiotics"},
    {"product_code": "300431", "short_label": "Medicaments with insulin"},
    {"product_code": "300432", "short_label": "Medicaments with corticosteroids"},
    {"product_code": "300439", "short_label": "Medicaments with hormones (excl. insulin)"},
    {"product_code": "300440", "short_label": "Medicaments with alkaloids (excl. hormones/antibiotics)"},
    {"product_code": "300441", "short_label": "Medicaments with ephedrine"},
    {"product_code": "300442", "short_label": "Medicaments with pseudoephedrine"},
    {"product_code": "300449", "short_label": "Medicaments with alkaloids, other"},
    {"product_code": "300450", "short_label": "Medicaments with vitamins"},
    {"product_code": "300460", "short_label": "Antimalarial medicaments (per note)"},
    {"product_code": "300490", "short_label": "Medicaments n.e.c."},
    {"product_code": "300443", "short_label": "Medicaments with norephedrine"},

    # --- Fertilizers (3104) ---
    {"product_code": "310420", "short_label": "Potash fertilizer, KCl"},
    {"product_code": "310430", "short_label": "Potash fertilizer, K2SO4"},
    {"product_code": "310490", "short_label": "Potash fertilizers n.e.c."},

    # --- Polyethylene (3901) ---
    {"product_code": "390110", "short_label": "Polyethylene <0.94 sg"},
    {"product_code": "390120", "short_label": "Polyethylene ≥0.94 sg"},
    {"product_code": "390130", "short_label": "EVA copolymers"},
    {"product_code": "390140", "short_label": "Ethylene-alpha-olefin copolymers <0.94 sg"},
    {"product_code": "390190", "short_label": "Ethylene polymers n.e.c."},

    # --- Sawnwood (4407) ---
    {"product_code": "440710", "short_label": "Conifer sawnwood >6mm"},
    {"product_code": "440711", "short_label": "Pine sawnwood >6mm"},
    {"product_code": "440712", "short_label": "Fir/spruce sawnwood >6mm"},
    {"product_code": "440713", "short_label": "S-P-F conifer sawnwood >6mm"},
    {"product_code": "440714", "short_label": "Hem-fir sawnwood >6mm"},
    {"product_code": "440719", "short_label": "Other conifer sawnwood >6mm"},
    {"product_code": "440721", "short_label": "Tropical wood—mahogany >6mm"},
    {"product_code": "440722", "short_label": "Tropical wood—virola/imbuia/balsa >6mm"},
    {"product_code": "440725", "short_label": "Tropical wood—meranti >6mm"},
    {"product_code": "440726", "short_label": "Tropical wood—white lauan/meranti/seraya >6mm"},
    {"product_code": "440727", "short_label": "Tropical wood—sapelli >6mm"},
    {"product_code": "440728", "short_label": "Tropical wood—iroko >6mm"},
    {"product_code": "440729", "short_label": "Tropical wood n.e.c. >6mm"},
    {"product_code": "440791", "short_label": "Oak sawnwood >6mm"},
    {"product_code": "440792", "short_label": "Beech sawnwood >6mm"},
    {"product_code": "440793", "short_label": "Maple sawnwood >6mm"},
    {"product_code": "440794", "short_label": "Cherry sawnwood >6mm"},
    {"product_code": "440795", "short_label": "Ash sawnwood >6mm"},
    {"product_code": "440796", "short_label": "Birch sawnwood >6mm"},
    {"product_code": "440797", "short_label": "Poplar/aspen sawnwood >6mm"},
    {"product_code": "440799", "short_label": "Sawnwood n.e.c. >6mm"},

    # --- Boards (4410) ---
    {"product_code": "441011", "short_label": "Particle board (wood)"},
    {"product_code": "441012", "short_label": "OSB (wood)"},
    {"product_code": "441019", "short_label": "Wafer/other board (wood)"},
    {"product_code": "441090", "short_label": "Boards (non-wood, particle/OSB)"},

    # --- Pulp (4703) ---
    {"product_code": "470311", "short_label": "Pulp, chemical (kraft), unbleached—conifer"},
    {"product_code": "470319", "short_label": "Pulp, chemical (kraft), unbleached—non-conifer"},
    {"product_code": "470321", "short_label": "Pulp, chemical (kraft), bleached—conifer"},
    {"product_code": "470329", "short_label": "Pulp, chemical (kraft), bleached—non-conifer"},

    # --- Diamonds (7102) ---
    {"product_code": "710210", "short_label": "Diamonds, unsorted"},
    {"product_code": "710221", "short_label": "Industrial diamonds, unworked"},
    {"product_code": "710229", "short_label": "Industrial diamonds, other"},
    {"product_code": "710231", "short_label": "Non-industrial diamonds, unworked"},
    {"product_code": "710239", "short_label": "Non-industrial diamonds, other"},

    # --- Gold (7108) ---
    {"product_code": "710811", "short_label": "Gold, powder"},
    {"product_code": "710812", "short_label": "Gold, unwrought"},
    {"product_code": "710813", "short_label": "Gold, semi-manufactured"},

    # --- Nickel (7501) ---
    {"product_code": "750110", "short_label": "Nickel mattes"},
    {"product_code": "750120", "short_label": "Nickel oxide sinters"},

    # --- Aluminium (7601) ---
    {"product_code": "760110", "short_label": "Aluminium, unwrought (not alloyed)"},
    {"product_code": "760120", "short_label": "Aluminium, unwrought (alloy)"},

    # --- Jet/turboprop/gas turbines (8411) ---
    {"product_code": "841111", "short_label": "Turbojets ≤25kN"},
    {"product_code": "841112", "short_label": "Turbojets >25kN"},
    {"product_code": "841121", "short_label": "Turboprops ≤1100kW"},
    {"product_code": "841122", "short_label": "Turboprops >1100kW"},
    {"product_code": "841181", "short_label": "Gas turbines ≤5000kW (excl. turbojet/prop)"},
    {"product_code": "841182", "short_label": "Gas turbines >5000kW (excl. turbojet/prop)"},
    {"product_code": "841191", "short_label": "Parts of turbojets/turboprops"},
    {"product_code": "841199", "short_label": "Parts of gas turbines n.e.c."},

    # --- Vehicles (8703/8704/etc.) ---
    {"product_code": "870310", "short_label": "Snow/golf & similar vehicles"},
    {"product_code": "870321", "short_label": "Passenger cars, SI ≤1000cc"},
    {"product_code": "870322", "short_label": "Passenger cars, SI >1000–1500cc"},
    {"product_code": "870323", "short_label": "Passenger cars, SI >1500–3000cc"},
    {"product_code": "870324", "short_label": "Passenger cars, SI >3000cc"},
    {"product_code": "870331", "short_label": "Passenger cars, diesel ≤1500cc"},
    {"product_code": "870332", "short_label": "Passenger cars, diesel >1500–2500cc"},
    {"product_code": "870333", "short_label": "Passenger cars, diesel >2500cc"},
    {"product_code": "870340", "short_label": "Hybrid (SI+EV), non-plug-in"},
    {"product_code": "870350", "short_label": "Hybrid (diesel+EV), non-plug-in"},
    {"product_code": "870360", "short_label": "PHEV (SI+EV), plug-in"},
    {"product_code": "870370", "short_label": "PHEV (diesel+EV), plug-in"},
    {"product_code": "870380", "short_label": "Battery electric vehicles (BEV)"},
    {"product_code": "870390", "short_label": "Passenger vehicles n.e.c."},
    {"product_code": "870410", "short_label": "Dumpers, off-highway"},
    {"product_code": "870421", "short_label": "Goods vehicles, diesel ≤5t GVW"},
    {"product_code": "870422", "short_label": "Goods vehicles, diesel >5–20t GVW"},
    {"product_code": "870423", "short_label": "Goods vehicles, diesel >20t GVW"},
    {"product_code": "870431", "short_label": "Goods vehicles, SI ≤5t GVW"},
    {"product_code": "870432", "short_label": "Goods vehicles, SI >5t GVW"},
    {"product_code": "870490", "short_label": "Goods vehicles n.e.c."},

    # --- Vehicle parts (8708) ---
    {"product_code": "870810", "short_label": "Bumpers & parts"},
    {"product_code": "870821", "short_label": "Seat belts"},
    {"product_code": "870822", "short_label": "Windscreens and windows"},
    {"product_code": "870829", "short_label": "Body parts/accessories n.e.c."},
    {"product_code": "870830", "short_label": "Brakes & parts"},
    {"product_code": "870840", "short_label": "Gear boxes & parts"},
    {"product_code": "870850", "short_label": "Drive-axles/differentials & parts"},
    {"product_code": "870870", "short_label": "Road wheels & parts"},
    {"product_code": "870880", "short_label": "Suspension & shock absorbers"},
    {"product_code": "870891", "short_label": "Radiators & parts"},
    {"product_code": "870892", "short_label": "Mufflers/exhausts & parts"},
    {"product_code": "870893", "short_label": "Clutches & parts"},
    {"product_code": "870894", "short_label": "Steering assemblies & parts"},
    {"product_code": "870895", "short_label": "Airbags (with inflator) & parts"},
    {"product_code": "870899", "short_label": "Parts & accessories n.e.c. (8708)"},

    # --- Aircraft/space (8802/880260) ---
    {"product_code": "880211", "short_label": "Helicopters ≤2000kg unladen"},
    {"product_code": "880212", "short_label": "Helicopters >2000kg unladen"},
    {"product_code": "880220", "short_label": "Aircraft ≤2000kg unladen"},
    {"product_code": "880230", "short_label": "Aircraft >2000–15,000kg unladen"},
    {"product_code": "880240", "short_label": "Aircraft >15,000kg unladen"},
    {"product_code": "880260", "short_label": "Spacecraft & launch vehicles"},

    # --- Furniture (9403) ---
    {"product_code": "940310", "short_label": "Office furniture, metal"},
    {"product_code": "940320", "short_label": "Furniture, metal (non-office)"},
    {"product_code": "940330", "short_label": "Office furniture, wood"},
    {"product_code": "940340", "short_label": "Kitchen furniture, wood"},
    {"product_code": "940350", "short_label": "Bedroom furniture, wood"},
    {"product_code": "940360", "short_label": "Furniture, wood (other)"},
    {"product_code": "940370", "short_label": "Furniture, plastic"},
    {"product_code": "940381", "short_label": "Furniture, bamboo/rattan"},
    {"product_code": "940382", "short_label": "Furniture, bamboo"},
    {"product_code": "940383", "short_label": "Furniture, rattan"},
    {"product_code": "940389", "short_label": "Furniture, cane/osier/similar"},
    {"product_code": "940390", "short_label": "Furniture parts"},
    {"product_code": "940391", "short_label": "Furniture; parts, of wood"},
    {"product_code": "940399", "short_label": "Furniture; parts, other than wood"},
])

# -------------------------------------------------------------------
# COUNTRY → ISO3 HELPER (rough equivalent to countrycode + custom_match)
# -------------------------------------------------------------------
try:
    import pycountry
except ImportError:
    pycountry = None

CUSTOM_IMPORT_ISO = {
    "United States": "USA",
    "Netherlands": "NLD",
    "Switzerland": "CHE",
    "Ivory Coast": "CIV",
    "South Korea": "KOR",
    "Hong Kong": "HKG",
    "China, Macao SAR": "MAC",
    "Lao People's Dem. Rep.": "LAO",
    "Iran (Islamic Republic of)": "IRN",
    "Moldova, Republic of": "MDA",
    "Tanzania, United Republic of": "TZA",
    "Turkiye": "TUR",
    "Cabo Verde": "CPV",
    "Bolivia": "BOL",
    "Bahamas": "BHS",
    "Brunei": "BRN",
    "DR Congo": "COD",
}
# exports use same overrides for importer countries
CUSTOM_EXPORT_ISO = CUSTOM_IMPORT_ISO.copy()


def country_to_iso3(name: str, custom_map: dict) -> str:
    if pd.isna(name):
        return None
    if name in custom_map:
        return custom_map[name]
    if pycountry is None:
        return None
    try:
        c = pycountry.countries.lookup(name)
        return c.alpha_3
    except Exception:
        return None


# -------------------------------------------------------------------
# HELPERS
# -------------------------------------------------------------------
def ordinal_en(n: int) -> str:
    """English ordinal suffix, like R ordinal_en()"""
    n = int(n)
    if 11 <= (n % 100) <= 13:
        suf = "th"
    else:
        last = n % 10
        if last == 1:
            suf = "st"
        elif last == 2:
            suf = "nd"
        elif last == 3:
            suf = "rd"
        else:
            suf = "th"
    return f"{n}{suf}"


# For exports across all years:
cca_exports_list = []

# -------------------------------------------------------------------
# MAIN LOOP OVER YEARS
# -------------------------------------------------------------------
for yy in years:
    print(f"=== Year {yy} ===")
    flows_path = os.path.join(path_flows, f"allflows_forCCAs_{yy}.parquet")
    flows = pd.read_parquet(flows_path)

    # flows: columns assumed:
    # exporter, importer, year, hs6, hs6_desc, freight, tau, fob, kg, ...
    # tau is ALREADY in decimal form (we scaled by 1/100 earlier),
    # so DO NOT divide by 100 again.

    # Add CIF
    flows["cif"] = flows["fob"] + flows["freight"]

    # -------------------------------
    # Top HS4 exports for Canada
    # -------------------------------
    can_exports = flows[flows["exporter"] == "Canada"].copy()
    can_exports["hs4"] = can_exports["hs6"].str[:4]
    can_exports = can_exports[can_exports["hs4"] != "9999"]

    tmp = (
        can_exports.groupby("hs4", as_index=False)["fob"]
        .sum()
        .rename(columns={"fob": "total_fob_hs4"})
    )
    tmp["total_fob"] = tmp["total_fob_hs4"].sum()
    can_exports = tmp.sort_values("total_fob_hs4", ascending=False).head(30)
    top30_hs4_exports = can_exports["hs4"].tolist()

    # HS6 under those HS4
    mask_exp = flows["hs6"].str[:4].isin(top30_hs4_exports)
    top_hs6_exports = flows.loc[mask_exp, "hs6"].drop_duplicates()

    # -------------------------------
    # Add per-kg values, exports (HS6)
    # -------------------------------
    exp_flows = flows[flows["hs6"].isin(top_hs6_exports)].copy()
    exp_flows = exp_flows[exp_flows["freight"].notna()]
    exp_flows["fob_perkg"]      = exp_flows["fob"] / exp_flows["kg"]
    exp_flows["freight_perkg"]  = exp_flows["freight"] / exp_flows["kg"]
    exp_flows["cif_perkg"]      = exp_flows["cif"] / exp_flows["kg"]
    exp_flows = exp_flows[exp_flows["fob_perkg"] > 0]
    exp_flows = exp_flows[exp_flows["cif_perkg"] > 0]
    exp_flows = exp_flows[exp_flows["exporter"] != exp_flows["importer"]]

    exp_flows = exp_flows[
        ["exporter", "importer", "hs6", "year", "fob_perkg", "freight_perkg", "cif_perkg", "tau"]
    ].copy()

    # ------------------------------------------------------------------
    # HS6 EXPORT SIDE COMBINATIONS
    # ------------------------------------------------------------------
    # Trades not involving Canada (deni)
    nocan_deni = exp_flows[
        (exp_flows["exporter"] != "Canada") & (exp_flows["importer"] != "Canada")
    ].copy()
    nocan_deni = nocan_deni.rename(
        columns={
            "exporter": "dropped_exporter",
            "importer": "new_importer",
            "fob_perkg": "fob_perkg_deni",
            "freight_perkg": "freight_perkg_deni",
            "cif_perkg": "cif_perkg_deni",
            "tau": "tau_deni",
        }
    )

    # Canada as exporter (nedi)
    can_export = exp_flows[exp_flows["exporter"] == "Canada"].copy()
    can_export = can_export.rename(
        columns={
            "exporter": "new_exporter",
            "importer": "dropped_importer",
            "fob_perkg": "fob_perkg_nedi",
            "freight_perkg": "freight_perkg_nedi",
            "cif_perkg": "cif_perkg_nedi",
            "tau": "tau_nedi",
        }
    )

    # Unique freight when Canada is exporter (neni)
    canada_exportfreight_neni = can_export[
        ["new_exporter", "dropped_importer", "hs6", "year", "freight_perkg_nedi", "tau_nedi"]
    ].rename(
        columns={
            "dropped_importer": "new_importer",
            "freight_perkg_nedi": "freight_perkg_neni",
            "tau_nedi": "tau_neni",
        }
    )

    # Merge combos: nocan_deni x can_export on (hs6, year)
    merged_export_combos = pd.merge(
        nocan_deni, can_export, on=["hs6", "year"], how="outer"
    )

    merged_export_combos = merged_export_combos.merge(
        canada_exportfreight_neni,
        on=["new_importer", "hs6", "new_exporter", "year"],
        how="left",
    )

    # EU zero tau_neni
    eu_mask = (
        merged_export_combos["new_exporter"].isin(eu_countries)
        & merged_export_combos["new_importer"].isin(eu_countries)
    )
    merged_export_combos.loc[eu_mask, "tau_neni"] = 0

    merged_export_combos = merged_export_combos[
        merged_export_combos["freight_perkg_neni"].notna()
        & merged_export_combos["tau_neni"].notna()
    ].copy()

    merged_export_combos["cca_notau"] = (
        merged_export_combos["cif_perkg_deni"]
        - merged_export_combos["freight_perkg_neni"]
        - merged_export_combos["fob_perkg_nedi"]
    )

    merged_export_combos["cca_tau"] = (
        (merged_export_combos["cif_perkg_deni"] * (1 + merged_export_combos["tau_deni"]))
        / (1 + merged_export_combos["tau_neni"])
        - merged_export_combos["freight_perkg_neni"]
        - (merged_export_combos["fob_perkg_nedi"] / (1 + merged_export_combos["tau_neni"]))
    )

    merged_export_combos["hs4"] = merged_export_combos["hs6"].str[:4]
    merged_export_combos = merged_export_combos[
        merged_export_combos["cca_notau"].notna()
        & merged_export_combos["cca_tau"].notna()
    ].copy()

    # ------------------------------------------------------------------
    # STUB IMPORT COMBOS (EMPTY) – EXPORT-ONLY SCRIPT
    # ------------------------------------------------------------------
    merged_import_combos = pd.DataFrame(columns=merged_export_combos.columns)

    # ------------------------------------------------------------------
    # THRESHOLDS (per year)
    # ------------------------------------------------------------------
    thresholds = flows.copy()
    thresholds["hs4"] = thresholds["hs6"].str[:4]
    thresholds = (
        thresholds[["exporter", "importer", "hs4", "year", "fob"]]
        .groupby(["exporter", "importer", "hs4", "year"], as_index=False)["fob"]
        .sum()
    )
    thresholds = thresholds[(thresholds["fob"] > 0) & thresholds["fob"].notna()]

    thresholds = thresholds.merge(
        thresholds.groupby(["importer", "hs4", "year"], as_index=False)["fob"]
        .max()
        .rename(columns={"fob": "maximum_flow"}),
        on=["importer", "hs4", "year"],
        how="left",
    )
    thresholds["threshold"] = 0.10 * thresholds["maximum_flow"]
    thresholds = thresholds[["exporter", "importer", "hs4", "year", "fob", "threshold"]]

    # Apply thresholds to merged_combos (exports only; import side stubbed)
    merged_combos = pd.concat([merged_export_combos, merged_import_combos], ignore_index=True)

    merged_combos = merged_combos.merge(
        thresholds,
        left_on=["dropped_exporter", "new_importer", "hs4", "year"],
        right_on=["exporter", "importer", "hs4", "year"],
        how="left",
    )

    cond = (
        ((merged_combos["fob"] > merged_combos["threshold"]) &
         (merged_combos["dropped_exporter"] != "Canada"))
        | (merged_combos["dropped_exporter"] == "Rest of World")
        | (merged_combos["dropped_importer"] == "Rest of World")
    )
    merged_combos = merged_combos[cond].copy()
    merged_combos = merged_combos.drop(columns=["exporter", "importer", "fob", "threshold"])

    # drop if dropped_importer == new_importer
    merged_combosnew = merged_combos[
        merged_combos["dropped_importer"] != merged_combos["new_importer"]
    ].copy()

    # ------------------------------------------------------------------
    # Attach HS6 descriptions
    # ------------------------------------------------------------------
    hs6_desc_map = (
        flows[["hs6", "hs6_desc"]]
        .drop_duplicates(subset=["hs6"])
        .set_index("hs6")["hs6_desc"]
        .to_dict()
    )
    merged_combosnew["hs6_desc"] = merged_combosnew["hs6"].map(hs6_desc_map)

    # ------------------------------------------------------------------
    # CANADA HS6 EXPORT RANKS
    # ------------------------------------------------------------------
    can_exports_hs6 = flows[flows["exporter"] == "Canada"][["hs6", "year", "fob"]].copy()
    can_exports_hs6 = (
        can_exports_hs6.groupby(["hs6", "year"], as_index=False)["fob"]
        .sum()
        .rename(columns={"fob": "can_export_fob"})
    )

    # rank by year
    can_exports_hs6["rank"] = (
        can_exports_hs6.sort_values(["year", "can_export_fob"], ascending=[True, False])
        .groupby("year")["can_export_fob"]
        .rank(method="first", ascending=False)
        .astype(int)
    )

    can_exports_hs6["rank_suffix"] = np.where(
        can_exports_hs6["rank"] == 1,
        "Most exported HS6 product",
        can_exports_hs6["rank"].apply(lambda r: f"{ordinal_en(r)} most exported HS6 product"),
    )

    # ------------------------------------------------------------------
    # EXPORT-SIDE HS6: new_exporter == "Canada"
    # ------------------------------------------------------------------
    merged_export_combos_hs6 = merged_combosnew[merged_combosnew["new_exporter"] == "Canada"].copy()
    merged_export_combos_hs6 = merged_export_combos_hs6.merge(
        can_exports_hs6[["hs6", "year", "rank", "rank_suffix"]],
        on=["hs6", "year"],
        how="left",
    )

    merged_export_combos_hs6 = merged_export_combos_hs6.rename(
        columns={
            "rank": "hs6_rank",
            "rank_suffix": "hs6_rank_suffix",
            "cca_notau": "cca_hs6_notau",
            "cca_tau": "cca_hs6_tau",
        }
    )

    merged_export_combos_hs6 = merged_export_combos_hs6[
        [
            "new_exporter",
            "dropped_importer",
            "dropped_exporter",
            "new_importer",
            "year",
            "hs6",
            "hs6_desc",
            "hs6_rank",
            "hs6_rank_suffix",
            "cca_hs6_notau",
            "cca_hs6_tau",
        ]
    ].copy()

    # ------------------------------------------------------------------
    # HS4 AGGREGATIONS (EXPORT SIDE)
    # ------------------------------------------------------------------
    exp_flows_hs4 = flows[flows["hs6"].isin(top_hs6_exports)].copy()
    exp_flows_hs4["hs4"] = exp_flows_hs4["hs6"].str[:4]
    exp_flows_hs4["tau_weights"] = exp_flows_hs4["tau"] * exp_flows_hs4["cif"]

    g = exp_flows_hs4.groupby(["exporter", "importer", "year", "hs4"], as_index=False)
    agg = g.agg(
        fob_hs4=("fob", "sum"),
        cif_hs4=("cif", "sum"),
        freight_hs4=("freight", "sum"),
        kg_hs4=("kg", "sum"),
        tau_sum=("tau_weights", "sum"),
    )
    agg["tau_hs4"] = agg["tau_sum"] / agg["fob_hs4"]

    exp_flows_hs4 = agg.copy()
    exp_flows_hs4["cif_perkg"] = exp_flows_hs4["cif_hs4"] / exp_flows_hs4["kg_hs4"]
    exp_flows_hs4["freight_perkg"] = exp_flows_hs4["freight_hs4"] / exp_flows_hs4["kg_hs4"]
    exp_flows_hs4["fob_perkg"] = exp_flows_hs4["fob_hs4"] / exp_flows_hs4["kg_hs4"]

    exp_flows2_hs4 = exp_flows_hs4[
        (exp_flows_hs4["freight_perkg"].notna()) &
        (exp_flows_hs4["fob_perkg"] > 0) &
        (exp_flows_hs4["cif_perkg"] > 0)
    ].copy()
    exp_flows2_hs4 = exp_flows2_hs4.rename(columns={"fob_hs4": "fob"})

    # HS4 non-Canada export combos
    noncan_export_combos4 = exp_flows2_hs4[
        (exp_flows2_hs4["exporter"] != "Canada") &
        (exp_flows2_hs4["importer"] != "Canada")
    ].copy()
    noncan_export_combos4 = noncan_export_combos4.rename(
        columns={
            "exporter": "dropped_exporter",
            "importer": "new_importer",
            "fob_perkg": "fob_perkg_deni",
            "freight_perkg": "freight_perkg_deni",
            "cif_perkg": "cif_perkg_deni",
            "tau_hs4": "tau_hs4_deni",
        }
    )

    can_export_combos4 = exp_flows2_hs4[exp_flows2_hs4["exporter"] == "Canada"].copy()
    can_export_combos4 = can_export_combos4.rename(
        columns={
            "exporter": "new_exporter",
            "importer": "dropped_importer",
            "fob_perkg": "fob_perkg_nedi",
            "freight_perkg": "freight_perkg_nedi",
            "cif_perkg": "cif_perkg_nedi",
            "tau_hs4": "tau_hs4_nedi",
        }
    )

    canada_freight_combos4 = can_export_combos4[
        ["new_exporter", "dropped_importer", "hs4", "year", "freight_perkg_nedi", "tau_hs4_nedi"]
    ].rename(
        columns={
            "dropped_importer": "new_importer",
            "freight_perkg_nedi": "freight_perkg_neni",
            "tau_hs4_nedi": "tau_hs4_neni",
        }
    )

    merged_export_combos4 = pd.merge(
        noncan_export_combos4, can_export_combos4, on=["hs4", "year"], how="outer"
    )
    merged_export_combos4 = merged_export_combos4.merge(
        canada_freight_combos4,
        on=["new_importer", "hs4", "new_exporter", "year"],
        how="left",
    )

    merged_export_combos4["cca_notau"] = (
        merged_export_combos4["cif_perkg_deni"]
        - merged_export_combos4["freight_perkg_neni"]
        - merged_export_combos4["fob_perkg_nedi"]
    )
    merged_export_combos4["cca_tau"] = (
        (merged_export_combos4["cif_perkg_deni"] * (1 + merged_export_combos4["tau_hs4_deni"]))
        / (1 + merged_export_combos4["tau_hs4_neni"])
        - merged_export_combos4["freight_perkg_neni"]
        - (merged_export_combos4["fob_perkg_nedi"] / (1 + merged_export_combos4["tau_hs4_neni"]))
    )

    merged_export_combos4 = merged_export_combos4[
        [
            "new_exporter",
            "dropped_importer",
            "dropped_exporter",
            "new_importer",
            "hs4",
            "year",
            "cca_notau",
            "cca_tau",
        ]
    ].dropna(subset=["cca_notau", "cca_tau"])
    merged_export_combos4 = merged_export_combos4.drop_duplicates()

    # ------------------------------------------------------------------
    # STUB IMPORT HS4 COMBOS (EMPTY) – EXPORT-ONLY SCRIPT
    # ------------------------------------------------------------------
    merged_import_combos4 = pd.DataFrame(columns=merged_export_combos4.columns)

    merged_combos4 = pd.concat([merged_export_combos4, merged_import_combos4], ignore_index=True)

    # Apply thresholds (HS4)
    merged_combos4 = merged_combos4.merge(
        thresholds,
        left_on=["dropped_exporter", "new_importer", "hs4", "year"],
        right_on=["exporter", "importer", "hs4", "year"],
        how="left",
    )

    cond = (
        ((merged_combos4["fob"] > merged_combos4["threshold"]) &
         (merged_combos4["dropped_exporter"] != "Canada"))
        | (merged_combos4["dropped_exporter"] == "Rest of World")
        | (merged_combos4["dropped_importer"] == "Rest of World")
    )
    merged_combos4 = merged_combos4[cond].copy()
    merged_combos4 = merged_combos4.drop(columns=["exporter", "importer", "fob", "threshold"])

    # HS4 export ranks for Canada
    can_exports4 = flows[flows["exporter"] == "Canada"].copy()
    can_exports4["hs4"] = can_exports4["hs6"].str[:4]
    can_exports4 = (
        can_exports4.groupby(["hs4", "year"], as_index=False)["fob"]
        .sum()
        .rename(columns={"fob": "can_export_fob"})
    )
    can_exports4["rank"] = (
        can_exports4.sort_values(["year", "can_export_fob"], ascending=[True, False])
        .groupby("year")["can_export_fob"]
        .rank(method="first", ascending=False)
        .astype(int)
    )
    can_exports4["rank_suffix"] = np.where(
        can_exports4["rank"] == 1,
        "Most exported HS4 product",
        can_exports4["rank"].apply(lambda r: f"{ordinal_en(r)} most exported HS4 product"),
    )

    # EXPORT HS4 combos – new_exporter == "Canada"
    merged_export_combos_hs4 = merged_combos4[merged_combos4["new_exporter"] == "Canada"].copy()
    merged_export_combos_hs4 = merged_export_combos_hs4.merge(
        can_exports4[["hs4", "year", "rank", "rank_suffix"]],
        on=["hs4", "year"],
        how="left",
    )
    merged_export_combos_hs4 = merged_export_combos_hs4.rename(
        columns={
            "rank": "hs4_rank",
            "rank_suffix": "hs4_rank_suffix",
            "cca_notau": "cca_hs4_notau",
            "cca_tau": "cca_hs4_tau",
        }
    )
    merged_export_combos_hs4 = merged_export_combos_hs4[
        merged_export_combos_hs4["dropped_importer"] != merged_export_combos_hs4["new_importer"]
    ]
    merged_export_combos_hs4 = merged_export_combos_hs4[
        merged_export_combos_hs4["new_exporter"] != merged_export_combos_hs4["dropped_exporter"]
    ]
    merged_export_combos_hs4["hs4_desc"] = merged_export_combos_hs4["hs4"].map(HS4_DESC_EXPORT)

    merged_export_combos_hs4 = merged_export_combos_hs4[
        [
            "new_exporter",
            "dropped_importer",
            "dropped_exporter",
            "new_importer",
            "year",
            "hs4",
            "hs4_desc",
            "hs4_rank",
            "hs4_rank_suffix",
            "cca_hs4_notau",
            "cca_hs4_tau",
        ]
    ].dropna(subset=["cca_hs4_notau", "cca_hs4_tau"])

    # ------------------------------------------------------------------
    # LONG FORMAT: EXPORT SIDE (HS6 + HS4)
    # ------------------------------------------------------------------
    # HS6 rows (Canada as new_exporter)
    cca_hs6_exp = merged_export_combos_hs6.assign(
        product_code=lambda df: df["hs6"],
        product_level="HS6",
        hs4_code=lambda df: df["hs6"].str[:4],
        cca_notau=lambda df: df["cca_hs6_notau"].astype(float),
        cca_tau=lambda df: df["cca_hs6_tau"].astype(float),
    )[
        [
            "year",
            "product_code",
            "product_level",
            "hs4_code",
            "dropped_exporter",
            "dropped_importer",
            "new_exporter",
            "new_importer",
            "cca_notau",
            "cca_tau",
            "hs6_desc",
            "hs6_rank",
        ]
    ].copy()

    # HS4 rows (Canada as new_exporter)
    cca_hs4_exp = merged_export_combos_hs4.assign(
        product_code=lambda df: df["hs4"],
        product_level="HS4",
        hs4_code=lambda df: df["hs4"],
        cca_notau=lambda df: df["cca_hs4_notau"].astype(float),
        cca_tau=lambda df: df["cca_hs4_tau"].astype(float),
    )[
        [
            "year",
            "product_code",
            "product_level",
            "hs4_code",
            "dropped_exporter",
            "dropped_importer",
            "new_exporter",
            "new_importer",
            "cca_notau",
            "cca_tau",
            "hs4_desc",
            "hs4_rank",
        ]
    ].copy()

    # debug: count HS4 vs HS6 rows
    print(
        f"Year {yy}: HS6 export rows = {len(cca_hs6_exp)}, "
        f"HS4 export rows = {len(cca_hs4_exp)}"
    )

    # ------------------------------------------------------------------
    # PRODUCT METADATA (EXPORTS)
    # ------------------------------------------------------------------
    prod_hs6_exp = cca_hs6_exp[
        ["year", "product_code", "product_level", "hs4_code", "hs6_desc", "hs6_rank"]
    ].drop_duplicates().rename(
        columns={"hs6_desc": "product_label"}
    )
    prod_hs6_exp["hs4_rank"] = np.nan

    prod_hs4_exp = cca_hs4_exp[
        ["year", "product_code", "product_level", "hs4_code", "hs4_desc", "hs4_rank"]
    ].drop_duplicates().rename(
        columns={"hs4_desc": "product_label"}
    )
    prod_hs4_exp["hs6_rank"] = np.nan

    products_meta_exp = pd.concat([prod_hs4_exp, prod_hs6_exp], ignore_index=True)
    products_meta_exp = (
        products_meta_exp.sort_values(["year", "product_code", "product_level"])
        .groupby(["year", "product_code", "product_level"], as_index=False)
        .first()
    )

    # propagate hs4_rank from HS4 rows to HS6 via hs4_code
    hs4_ranks_exp = prod_hs4_exp[["year", "hs4_code", "hs4_rank"]].drop_duplicates()
    products_meta_exp = products_meta_exp.merge(
        hs4_ranks_exp,
        on=["year", "hs4_code"],
        how="left",
        suffixes=("", "_from_hs4"),
    )
    products_meta_exp["hs4_rank"] = products_meta_exp["hs4_rank"].fillna(
        products_meta_exp["hs4_rank_from_hs4"]
    )
    products_meta_exp = products_meta_exp.drop(columns=["hs4_rank_from_hs4"])

    # ------------------------------------------------------------------
    # COMBINE HS4 + HS6 EXPORT ROWS
    # ------------------------------------------------------------------
    cca_exp_all = pd.concat([cca_hs4_exp, cca_hs6_exp], ignore_index=True)

    # drop any pre-existing rank columns so we don't get _x/_y on merge
    for col in ["hs4_rank", "hs6_rank"]:
        if col in cca_exp_all.columns:
            cca_exp_all = cca_exp_all.drop(columns=[col])

    # attach product_label + canonical ranks
    cca_exp_all = cca_exp_all.merge(
        products_meta_exp,
        on=["year", "product_code", "product_level", "hs4_code"],
        how="left",
    )

    # final export-year block
    cca_exp_all = cca_exp_all[
        [
            "year",
            "product_code",
            "product_level",
            "hs4_code",
            "product_label",
            "hs4_rank",
            "hs6_rank",
            "new_exporter",
            "dropped_importer",
            "dropped_exporter",
            "new_importer",
            "cca_notau",
            "cca_tau",
        ]
    ].copy()

    # importer ISO for exports
    cca_exp_all["Importer_ISO_country"] = cca_exp_all["new_importer"].apply(
        lambda x: country_to_iso3(x, CUSTOM_IMPORT_ISO)
    )

    # tiny check: HS4 vs HS6
    print("Export product_code lengths:",
          cca_exp_all["product_code"].str.len().value_counts().to_dict())

    cca_exports_list.append(cca_exp_all)

# -------------------------------------------------------------------
# CONCAT ALL YEARS
# -------------------------------------------------------------------
cca_all_exports = pd.concat(cca_exports_list, ignore_index=True)

# -------------------------------------------------------------------
# ATTACH HS6 SHORT LABELS (OVERRIDE product_label FOR HS6 ROWS)
# -------------------------------------------------------------------
cca_all_exports = cca_all_exports.merge(
    hs6_lookup_exports, on="product_code", how="left"
)

cca_all_exports["product_label"] = np.where(
    (cca_all_exports["product_level"] == "HS6") & cca_all_exports["short_label"].notna(),
    cca_all_exports["short_label"],
    cca_all_exports["product_label"],
)
cca_all_exports = cca_all_exports.drop(columns=["short_label"])

# -------------------------------------------------------------------
# SAVE
# -------------------------------------------------------------------
export_path = os.path.join(path_cca, "export_cca.parquet")
cca_all_exports.to_parquet(export_path, index=False)

print(f"Saved {export_path}")

# -------------------------------------------------------------------
# OPTIONAL: REMOVE INTERMEDIATE FLOWS
# -------------------------------------------------------------------
for yy in years:
    inter_path = os.path.join(path_flows, f"flows_intermediate_{yy}.parquet")
    if os.path.exists(inter_path):
        os.remove(inter_path)
        print(f"Deleted intermediate {inter_path}")
