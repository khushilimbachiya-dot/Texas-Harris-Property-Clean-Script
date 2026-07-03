# """
# QA Report - Building Details cleaning pipeline
# Converted from PySpark to pandas / numpy.
#
# Original notebook: QA_Report_building_details.ipynb
# """
#
# import os
# import re
# import numpy as np
# import pandas as pd
#
# pd.set_option('display.max_columns', None)
# pd.set_option('display.max_colwidth', None)
#
# # ---------------------------------------------------------------------------
# # CONFIG - update these paths for your machine
# # ---------------------------------------------------------------------------
# INPUT_PATH = r"D:\Khushi\Cleaning_&_Transformation_Phase_Traning_Files\us_tx_48201_data_prop_building-details_bronze_20260501.csv"
# OUTPUT_PATH = r"D:\Khushi\Cleaning_&_Transformation_Phase_Traning_Files\us_tx_48201_data_prop_building-details_silver_20260501.csv"
#
#
# # ---------------------------------------------------------------------------
# # Column groups (copied from the notebook)
# # ---------------------------------------------------------------------------
#
# # Columns that get comma / trailing ".0" formatting cleanup, and all-zero
# # pipe-delimited values blanked out.
# COLE = [
#     "elev_elect_frght", "frame_overhang_pri", "dock_level_floor_c", "area_bank_office_c",
#     "masonry_overhang_upr", "half_story_frame_upr", "room_full_bath", "area_hirise_ofc_c",
#     "frame_bay_upr", "masonry_bay_upr", "fireplace_direct_vent", "encl_frame_porch_upr",
#     "porch_enclosed_c", "ldng_ramp_conc_c", "enclosed_entry_c", "unfin_basement_lwr",
#     "one_story_mas_lwr", "elevator_stops", "room_rec", "room_half_bath", "stories",
#     "wall_height_enclosure", "perimeter", "interior_finish_percent", "fireplace_metal_prefab",
#     "craneway_5_ton", "fireplace_masonry_firebrick", "unfin_l_a_primary", "room_bedroom",
#     "base_area_pri", "cnpy_roof_ss_eco_c", "basement_c", "frame_overhang_upr",
#     "mas_brk_garage_pri", "elev_elect_pass", "util_bldg_mtl_c", "masonry_bay_pri",
#     "base_area_lwr", "open_mas_porch_upr", "apt_3_bedroom_unit", "mas_conc_patio_pri",
#     "encl_frame_porch_pri", "wood_deck_upr", "frame_garage_lwr", "masonry_overhang_pri",
#     "one_story_frame_upr", "frame_garage_pri", "store_front_metal", "frame_bay_pri",
#     "parking_garage_upper_level_c", "stone_tile_patio_pri", "parking_spaces",
#     "garage_att_mas_c", "apt_2_bedroom_unit", "oh_door_wood_mtl", "masonry_terrace_pri",
#     "frame_util_bldg_upr", "cnpy_only_c", "carport_lwr", "carport_c", "one_story_mas_upr",
#     "mas_conc_patio_lwr", "half_story_mas_upr", "attic_unfinished", "room_total",
#     "porch_open_upr_c", "open_mas_porch_pri", "garage_att_frm_c", "office_warehouse_ratio",
#     "cnpy_roof_ss_gd_c", "frame_util_bldg_lwr", "attic_finished", "one_story_mas_pri",
#     "unfin_l_a_upper", "cnpy_roof_ss_ave_c", "open_mas_porch_lwr", "frame_util_bldg_pri",
#     "open_frame_porch_pri", "encl_mas_porch_pri", "porch_open_c", "wood_deck_lwr",
#     "net_rentable_area", "util_bldg_brk_c", "masnry_util_bldg_pri", "mobile_home_8_11_width",
#     "truck_train_wells_c", "basement", "open_frame_porch_lwr", "mobile_home_12_14_width",
#     "oh_door_roll_steel", "stone_tile_patio_upper", "apt_1_bedroom_unit", "mas_brk_garage_lwr",
#     "wood_deck_pri", "canopy_lwr", "open_frame_porch_upr", "bank_canopy_c", "parking_garage_c",
#     "one_story_frame_pri", "util_bldg_frm_c", "garage_interior_parking_apartments_c",
#     "mobile_home_15_19_width", "carport_pri", "franchise_code", "canopy_pri",
#     "economic_obsolescence",
# ]
#
# # Columns that get cast to numeric (int) after stripping ".0"/commas, then 0 -> NaN
# COLDS = [
#     "Unnamed: 3", "masonry_trim", "oh_door_motor_w_m", "basement_top", "bank_vault_no_door",
#     "medical_beds", "mobile_home_20_29_width", "apt_effcncy_unit", "mobile_home_30_width",
#     "bank_drive_thru", "atrium", "number_of_apartment_units", "year", "store_front_wood",
#     "fireplace_adl_open", "escalator_48", "elev_hydro_pass", "building", "building_number",
# ]
#
# # Columns where stray "None" tokens inside pipe-delimited values are stripped
# COLD = [
#     "cooling_type", "heating_ac", "functional_utility", "heating_type",
#     "sprinkler_type", "partition_type", "plumbing_type",
# ]
#
# # Columns where "0" tokens inside pipe-delimited multi-value strings are removed
# COLV = [
#     "frame_overhang_upr", "mas_brk_garage_pri", "apt_3_bedroom_unit", "frame_garage_pri",
#     "apt_2_bedroom_unit", "oh_door_wood_mtl", "attic_unfinished", "office_warehouse_ratio",
#     "one_story_mas_pri", "wall_height", "open_frame_porch_pri", "interior_finish_percent",
#     "porch_open_c", "apt_1_bedroom_unit", "canopy_lwr", "one_story_frame_pri",
#     "base_area_pri", "garage_interior_parking_apartments_c",
# ]
#
# FAKE_NULLS_LOWER = [x.lower() for x in
#                      ['', 'null', 'none', 'nan', 'na', 'n/a', 'n.a.', 'nil', 'NONE']]
#
#
# # ---------------------------------------------------------------------------
# # Helper transforms (each mirrors one PySpark F.withColumn step)
# # ---------------------------------------------------------------------------
#
# def load_data(path: str) -> pd.DataFrame:
#     """Read CSV as all-string columns (mirrors inferSchema=False)."""
#     df = pd.read_csv(path, dtype=str, keep_default_na=False, na_values=[])
#     df["parcel_number"] = df["parcel_number"].astype(str).str.replace(r"\.0$", "", regex=True)
#     return df
#
#
# def drop_zero_building_units(df: pd.DataFrame) -> pd.DataFrame:
#     """Remove rows where building_details=='Displayed', element=='Units',
#     building==0 and building_number==0."""
#     building_num = pd.to_numeric(df["building"], errors="coerce")
#     building_number_num = pd.to_numeric(df["building_number"], errors="coerce")
#
#     mask = (
#         (df["building_details"] == "Displayed")
#         & (df["element"] == "Units")
#         & (building_num == 0)
#         & (building_number_num == 0)
#     )
#     return df[~mask].copy()
#
#
# def trim_whitespace(df: pd.DataFrame) -> pd.DataFrame:
#     """Trim and collapse internal whitespace on every string (object) column."""
#     for c in df.select_dtypes(include="object").columns:
#         s = df[c]
#         cleaned = s.str.strip().str.replace(r"\s+", " ", regex=True)
#         df[c] = cleaned.where(s.notna(), s)
#     return df
#
#
# def replace_fake_nulls(df: pd.DataFrame) -> pd.DataFrame:
#     """Replace common 'fake null' text values with real NaN, for string columns."""
#     for c in df.select_dtypes(include="object").columns:
#         s = df[c]
#         lowered = s.str.strip().str.lower()
#         mask = lowered.isin(FAKE_NULLS_LOWER)
#         df[c] = s.mask(mask, np.nan)
#     return df
#
#
# # def clean_cole_columns(df: pd.DataFrame) -> pd.DataFrame:
# #     """Strip commas and a trailing '.0' (before '||' or end of string)."""
# #     for c in COLE:
# #         if c not in df.columns:
# #             continue
# #         s = df[c].astype("string")
# #         s = s.str.replace(",", "", regex=False)
# #         s = s.str.replace(r"\.0(?=\s*(\|\||$))", "", regex=True)
# #         df[c] = s
# #     return df
#
# def clean_cole_columns(df: pd.DataFrame) -> pd.DataFrame:
#     """
#     Remove commas, backticks and trailing .0 from ALL string columns.
#     Also convert all-zero values in COLE columns to NaN.
#     """
#
#     obj_cols = df.select_dtypes(include="object").columns
#
#     pattern = r"^0(\s*\|\|\s*0)*$"
#
#     for c in obj_cols:
#
#         s = df[c].astype("string")
#
#         # Remove backticks
#         s = s.str.replace("`", "", regex=False)
#
#         # Remove commas
#         s = s.str.replace(",", "", regex=False)
#
#         # Remove trailing .0
#         s = s.str.replace(r"\.0(?=\s*(\|\||$))", "", regex=True)
#
#         # Only apply zero masking to COLE columns
#         if c in COLE:
#             s = s.mask(s.str.fullmatch(pattern).fillna(False), pd.NA)
#
#         df[c] = s
#
#     return df
#
#
# # def blank_all_zero_pipe_values(df: pd.DataFrame) -> pd.DataFrame:
# #     """Blank out values that are entirely zero tokens, e.g. '0', '0||0'."""
# #     pattern = r"^0(\|\|0)*(\|\|)?$"
# #     for c in COLE:
# #         if c not in df.columns:
# #             continue
# #         s = df[c].astype("string")
# #         matches = s.str.fullmatch(pattern).fillna(False)
# #         df[c] = s.mask(matches, np.nan)
# #     return df
#
#
# import re
# import pandas as pd
#
# # def clean_none_tokens(df: pd.DataFrame):
# #
# #     remove_tokens = {
# #         "none", "unknown", "null", "nan",
# #         "na", "n/a", "0", "0.0", ""
# #     }
# #
# #     for c in COLD:          # <-- use COLD instead of all object columns
# #         if c not in df.columns:
# #             continue
# #
# #         def clean(x):
# #             if pd.isna(x):
# #                 return pd.NA
# #
# #             parts = re.split(r"\s*\|\|\s*", str(x))
# #
# #             parts = [
# #                 p.strip()
# #                 for p in parts
# #                 if p.strip().lower() not in remove_tokens
# #             ]
# #
# #             return pd.NA if not parts else " || ".join(parts)
# #
# #         df[c] = df[c].apply(clean)
# #
# #     return df
# import re
# import pandas as pd
#
# def clean_pipe_values(df: pd.DataFrame):
#
#     remove_tokens = {
#         "",
#         "none",
#         "unknown",
#         "null",
#         "nan",
#         "na",
#         "n/a",
#         "nil",
#         "0",
#         "0.0",
#     }
#
#     obj_cols = df.select_dtypes(include="object").columns
#
#     for c in obj_cols:
#
#         def clean(x):
#
#             if pd.isna(x):
#                 return pd.NA
#
#             value = str(x).strip()
#
#             if "||" not in value:
#                 if value.casefold() in remove_tokens:
#                     return pd.NA
#                 return value
#
#             tokens = [
#                 t.strip()
#                 for t in re.split(r"\s*\|\|\s*", value)
#             ]
#
#             tokens = [
#                 t for t in tokens
#                 if t.casefold() not in remove_tokens
#             ]
#
#             if not tokens:
#                 return pd.NA
#
#             return " || ".join(tokens)
#
#         df[c] = df[c].apply(clean)
#
#     return df
#
# def drop_qa_columns(df: pd.DataFrame) -> pd.DataFrame:
#     return df.drop(columns=["element", "building_details"], errors="ignore")
#
#
# def clean_colds_columns(df: pd.DataFrame) -> pd.DataFrame:
#     """Strip formatting, cast to nullable Int64, then blank out zeros."""
#     for c in COLDS:
#         if c not in df.columns:
#             continue
#         s = df[c].astype(str)
#         s = s.str.replace(r"\.0$", "", regex=True)
#         s = s.str.replace(",", "", regex=False)
#         numeric = pd.to_numeric(s, errors="coerce").astype("Int64")
#         numeric = numeric.mask(numeric == 0, pd.NA)
#         df[c] = numeric
#     return df
#
#
# def _strip_pipe_zeros(value):
#     if pd.isna(value):
#         return np.nan
#     tokens = re.split(r"\s*\|\|\s*", str(value))
#     tokens = [t.strip() for t in tokens if t is not None and t.strip() not in ("", "0")]
#     if not tokens:
#         return np.nan
#     return " || ".join(tokens)
#
#
# def clean_colv_columns(df: pd.DataFrame) -> pd.DataFrame:
#     """Remove pipe-delimited '0' tokens from multi-value strings."""
#     for c in COLV:
#         if c not in df.columns:
#             continue
#         df[c] = df[c].apply(_strip_pipe_zeros)
#     return df
#
#
# def clean_wall_height(df: pd.DataFrame) -> pd.DataFrame:
#     if "wall_height" in df.columns:
#         df["wall_height"] = df["wall_height"].mask(df["wall_height"].astype(str) == "0", np.nan)
#     return df
#
#
# def save_data(df: pd.DataFrame, path: str) -> None:
#     os.makedirs(os.path.dirname(path), exist_ok=True)
#     df.to_csv(path, index=False)
#     print(f"DataFrame saved successfully to:\n{path}")
#
#
# # ---------------------------------------------------------------------------
# # Pipeline
# # ---------------------------------------------------------------------------
#
# def run_pipeline(input_path, output_path):
#
#     chunk_size = 500000
#
#     if os.path.exists(output_path):
#         os.remove(output_path)
#
#     first_chunk = True
#     total_rows = 0
#
#     for i, df in enumerate(
#         pd.read_csv(
#             input_path,
#             dtype=str,
#             keep_default_na=False,
#             na_values=[],
#             chunksize=chunk_size,
#         )
#     ):
#
#         print(f"\nProcessing Chunk {i+1}")
#
#         df["parcel_number"] = (
#             df["parcel_number"]
#             .astype(str)
#             .str.replace(r"\.0$", "", regex=True)
#         )
#
#         df = drop_zero_building_units(df)
#
#         df = trim_whitespace(df)
#
#         df = clean_cole_columns(df)
#
#         df = clean_none_tokens(df)
#
#         df = replace_fake_nulls(df)
#
#         df = drop_qa_columns(df)
#
#         df = clean_colds_columns(df)
#
#         df = clean_colv_columns(df)
#
#         df = clean_wall_height(df)
#
#         df.to_csv(
#             output_path,
#             mode="w" if first_chunk else "a",
#             header=first_chunk,
#             index=False,
#         )
#
#         total_rows += len(df)
#
#         print(f"Chunk {i+1} exported ({len(df)} rows)")
#         print(f"Total exported: {total_rows}")
#
#         first_chunk = False
#
#         del df
#
#     print("\nCompleted!")
#     print("Total rows exported:", total_rows)
#
#
# if __name__ == "__main__":
#     run_pipeline(INPUT_PATH, OUTPUT_PATH)


import re
import pandas as pd

INPUT = r"D:\Khushi\Cleaning_&_Transformation_Phase_Traning_Files\us_tx_48201_data_prop_building-details_silver_20260501.csv"
OUTPUT = r"D:\Khushi\Cleaning_&_Transformation_Phase_Traning_Files\us_tx_48201_data_prop_building-details_silver_20260501_fixed.csv"

CHUNK_SIZE = 500000

remove_tokens = {
    "",
    "none",
    "unknown",
    "null",
    "nan",
    "na",
    "n/a",
    "nil",
    "0",
    "0.0",
}

first_chunk = True
total_rows = 0

for i, df in enumerate(
    pd.read_csv(
        INPUT,
        dtype=str,
        keep_default_na=False,
        chunksize=CHUNK_SIZE,
    )
):

    print(f"Processing Chunk {i + 1}...")

    obj_cols = df.select_dtypes(include="object").columns

    for col in obj_cols:

        # Skip columns that don't contain pipe values
        mask = df[col].str.contains(r"\|\|", na=False)

        if not mask.any():
            continue

        df.loc[mask, col] = (
            df.loc[mask, col]
            .str.split(r"\s*\|\|\s*")
            .apply(
                lambda x: " || ".join(
                    t.strip()
                    for t in x
                    if t.strip().casefold() not in remove_tokens
                )
            )
        )

        # Replace empty results with NA
        df[col] = df[col].replace("", pd.NA)

    df.to_csv(
        OUTPUT,
        mode="w" if first_chunk else "a",
        header=first_chunk,
        index=False,
    )

    total_rows += len(df)
    first_chunk = False

    print(f"Chunk {i + 1} completed ({len(df)} rows)")
    print(f"Total rows processed: {total_rows}")

print("\nDone!")

INPUT = r"D:\Khushi\Cleaning_&_Transformation_Phase_Traning_Files\us_tx_48201_data_prop_building-details_silver_20260501.csv"
OUTPUT = r"D:\Khushi\Cleaning_&_Transformation_Phase_Traning_Files\us_tx_48201_data_prop_building-details_silver_20260501_fixed.csv"

remove_tokens = {
    "",
    "none",
    "unknown",
    "null",
    "nan",
    "na",
    "n/a",
    "nil",
    "0",
    "0.0",
}

df = pd.read_csv(INPUT, dtype=str, keep_default_na=False)

for col in df.columns:

    def clean(x):

        value = str(x).strip()

        if value == "":
            return pd.NA

        if "||" not in value:
            return pd.NA if value.casefold() in remove_tokens else value

        tokens = [
            t.strip()
            for t in re.split(r"\s*\|\|\s*", value)
        ]

        tokens = [
            t for t in tokens
            if t.casefold() not in remove_tokens
        ]

        return pd.NA if not tokens else " || ".join(tokens)

    df[col] = df[col].apply(clean)

df.replace("", pd.NA, inplace=True)

df.to_csv(OUTPUT, index=False)

print("Done!")