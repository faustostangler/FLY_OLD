import os
import re
import sqlite3
import time
from threading import Lock

import pandas as pd
from tqdm import tqdm
from typing import Generator, Tuple, List

from utils import intel
from utils.base_processor import BaseProcessor


class IntelProcessor(BaseProcessor):
    """docstrings."""

    def __init__(self):
        """docstrings."""
        super().__init__()

        # Initialize a threading Lock
        self.db_lock = Lock()

        # Store compiled regex to avoid redundant compilation
        self.regex_cache = {}

        # Initialize database and table names
        self.tbl_statements_raw = self.config.databases["raw"]["table"]["statements_raw"]
        self.idx_statements_raw = self.config.databases["raw"]["index"]["statements_raw"]
        self.tbl_statements_normalized = self.config.databases["raw"]["table"]["statements_normalized"]
        self.primary_key_columns = self.config.domain["statements_sheet_columns"]

        self.db_filepath = self.config.databases["raw"]["filepath"]

        self.section_criterias = {
            "Composição do Capital": intel.section_0_criteria,
            "Balanço Patrimonial Ativo": intel.section_1_criteria,
            "Balanço Patrimonial Passivo": intel.section_2_criteria,
            "Demonstração do Resultado": intel.section_3_criteria,
            "Demonstração de Fluxo de Caixa": intel.section_6_criteria,
            "Demonstração de Valor Adiconado": intel.section_7_criteria,
        }

    def process_instance(self, sub_batch, payload, progress, msg):
        """Process a single batch by delegating from abstract base_processor
        method to this class process_batch (true process info method) via this
        process_instance method (create instance method).

        sub_batch progress

        return result from process_batch
        """
        result = pd.DataFrame()  # Return an empty DataFrame on failure

        try:
            if msg:
                print(
                    f"Starting batch {progress['batch_index']+1}/{progress['total_batches']} {100 * (progress['batch_index']+1) / progress['total_batches']:.02f}%"
                )
            batch_processor = IntelProcessor()

            # Delegate to process_batch for the actual batch processing
            result, benchmark_results = batch_processor.benchmark_function(
                batch_processor.process_batch, sub_batch, payload, progress, benchmark_mode=False
            )

            # # Save result to database
            # self.save_to_db(
            #     dataframe=result,
            #     table_name=self.tbl_statements_normalized,
            #     db_filepath=self.db_filepath,
            #     alert=False,
            #     update=False,
            # )

            # first = f"{sub_batch['company_name'].iloc[0]}"
            # last = f"{sub_batch['company_name'].iloc[-1]}"
            # extra_info = [f"Worker download {progress['thread_id']}"]
            # self.print_info(progress['batch_index'], progress['total_batches'], progress['start_time'], extra_info)

        except Exception as e:
            self.log_error(f"Error in process_instance: {e}")

        return result

    def process_batch(self, sub_batch, payload, progress):
        """"""
        result = pd.DataFrame(columns=sub_batch.columns)

        try:
            companies = sub_batch["company_name"].unique()
            total_companies = len(companies)

            start_time = time.time()
            for i, company_name in enumerate(companies):
                mask = sub_batch["company_name"] == company_name
                df = sub_batch[mask]

                # version control
                result, df_older, df_duplicates = self._filter_newer_versions(df)

                # standardization
                result = self.generate_standard_financial_statements(df, progress)

                # sanitize db
                result = self.adjust_columns(result)

                # Outlier detection
                result = self.detect_and_correct_outliers(result)

                # math transformation
                result = self._transform_quarterly_values(result)

                # save results
                self.save_to_db(
                    dataframe=result,
                    table_name=self.tbl_statements_normalized,
                    db_filepath=self.db_filepath,
                    alert=False,
                    update=False,
                )

                # Atualiza a coluna processed para todas as linhas pertinentes na tbl_statements_raw
                sql_update = """
                    UPDATE tbl_statements_raw
                    SET processed = version
                    WHERE company_name = ?
                    AND (processed IS NULL OR processed <> version);
                """

                self.save_to_db(
                    dataframe=df,  # No dataframe needed
                    table_name="tbl_statements_raw",
                    db_filepath=self.db_filepath,
                    alert=False,
                    sql_update=sql_update,
                    sql_update_params=(company_name,),  # Pass company name
                )

                # Log progress
                actual_item = progress["batch_start"] + i + 1
                total_items = progress["scrape_size"] + 0
                worker_info = f"Worker download {progress['thread_id']} Item {100 * actual_item / total_items:.02f}% ({actual_item+0}/{total_items})"

                # Retrieve quarter max and min values
                quarter_min = (
                    pd.to_datetime(df["quarter"].min(), errors="coerce").strftime("%Y-%m")
                    if not df["quarter"].isna().all()
                    else None
                )
                quarter_max = (
                    pd.to_datetime(df["quarter"].max(), errors="coerce").strftime("%Y-%m")
                    if not df["quarter"].isna().all()
                    else None
                )

                # Retrieve first row values for sector, subsector, and segment
                sector = df["sector"].iloc[0] if not df.empty else None
                subsector = df["subsector"].iloc[0] if not df.empty else None
                segment = df["segment"].iloc[0] if not df.empty else None

                quarter_info = f"from {quarter_min} to {quarter_max}"

                extra_info = [worker_info, sector, subsector, segment, company_name, quarter_info]
                extra_info = [worker_info, company_name, quarter_info]
                # self.print_info(i, total_companies, start_time, extra_info, indent_level=0)

        except Exception as e:
            self.log_error(e)

        return result

    def generate_standard_financial_statements(self, sub_batch, progress):
        """Generates various sections of the financial statements based on the
        provided DataFrame.

        Parameters
        ----------
        sub_batch : pandas.DataFrame
            The input DataFrame containing financial data.

        Returns
        -------
        pd.DataFrame
            Combined DataFrame of all generated financial statement sections.
        """
        try:
            # # Loop through each section in the standardization pack
            # sector = sub_batch.iloc[0]["sector"]
            # subsector = sub_batch.iloc[0]["subsector"]
            # segment = sub_batch.iloc[0]["segment"]
            # company_name = sub_batch.iloc[0]["company_name"]
            # start_time = time.time()

            # Precompute lowercase and level columns to accelerate filtering
            sub_batch["account_lower"] = sub_batch["account"].astype(str).str.lower().str.strip()
            sub_batch["description_lower"] = sub_batch["description"].astype(str).str.lower().str.strip()
            sub_batch["account_level"] = sub_batch["account"].astype(str).str.count(r"\.") + 1

            for i, (section_name, section_criteria) in enumerate(self.section_criterias.items()):
                sub_batch = self.apply_section_criteria(sub_batch, section_name, section_criteria)

                # extra_info = [progress['thread_id'], progress['batch_index'], company_name, section_name.upper(), ]
                # self.print_info(i, len(self.section_criterias), start_time, extra_info, indent_level=3)

        except Exception as e:
            print(f"criteria error {e}")

        return sub_batch

    def apply_section_criteria(self, df, section_name, section_criteria, output_file="output.txt"):
        """Main function to apply a criteria tree to the DataFrame. Allow
        recursivity for subcriteria.

        Parameters:
            df (pd.DataFrame): The DataFrame to be modified.
            section_criteria (list): List of criteria in a tree format.
            output_file (str): Path to the output file.

        Returns:
            pd.DataFrame: The modified DataFrame after applying all criteria.
        """
        try:
            start_time = time.time()
            for i, criteria_item in enumerate(section_criteria):
                df, section_name, account, description = self.apply_criteria(
                    df, section_name, criteria_item, output_file=output_file
                )
                # extra_info = [section_name.upper(), account, description]
                # self.print_info(i, len(section_criteria), start_time, extra_info, indent_level=4)

        except Exception as e:
            self.print_info(e)

        return df

    # Helper: compile or retrieve regex for union of terms (ANY of the terms)
    def get_regex_union(self, values):
        pattern_key = ("ANY", tuple(values))
        if pattern_key not in self.regex_cache:
            pattern = "|".join(map(re.escape, values))
            self.regex_cache[pattern_key] = re.compile(pattern, re.IGNORECASE)
        return self.regex_cache[pattern_key]
    
    # Helper: compile or retrieve regex for all terms (ALL must appear)
    def get_regex_all(self, values):
        pattern_key = ("ALL", tuple(values))
        if pattern_key not in self.regex_cache:
            # Lookahead for each term ensures all are present
            pattern = "^" + "".join(f"(?=.*{re.escape(val)})" for val in values)
            self.regex_cache[pattern_key] = re.compile(pattern, re.IGNORECASE)
        return self.regex_cache[pattern_key]
    
    # Helper: compile regex for includes (any or all) and excludes combined
    def get_regex_include_exclude(self, includes, excludes, require_all=False):
        key = ("ALL_NONE" if require_all else "ANY_NONE", tuple(includes), tuple(excludes))
        if key not in self.regex_cache:
            # Build pattern with negative lookahead for each exclude and positive for includes
            neg_part = "".join(f"(?!.*{re.escape(term)})" for term in excludes)
            if require_all:
                # All 'includes' must be present
                pos_part = "".join(f"(?=.*{re.escape(term)})" for term in includes)
            else:
                # At least one of 'includes' present
                pos_part = f"(?=.*(?:{'|'.join(map(re.escape, includes))}))"
            pattern = "^" + neg_part + pos_part
            self.regex_cache[key] = re.compile(pattern, re.IGNORECASE)
        return self.regex_cache[key]

    def apply_criteria(
        self, df, section_name, criteria_item,
        parent_mask=None, output_file="output.txt", level=1
    ):
        """Applies a criterion (and its sub-criteria) to the DataFrame with optimized string filtering."""
        target = criteria_item["target_line"]
        filters = criteria_item["criteria"]
        sub_criteria = criteria_item.get("sub_criteria", [])
        df = df.reset_index(drop=True)  # ensure a clean index
        
        # Start with all rows or the provided parent mask
        mask = parent_mask.copy() if parent_mask is not None else pd.Series(True, index=df.index)
        
        # Combine multiple filters on the same column for efficiency
        combined_filters = []
        seen_col = {}
        for col, cond, val in filters:
            # Normalize value to list if needed for certain conditions
            if cond in {"contains_any", "contains_none", "contains_all", "not_contains", "not_contains_all"}:
                if not isinstance(val, list):
                    val = [val]
            # Merge conditions on the same column
            if col in seen_col:
                prev_idx = seen_col[col]
                prev_col, prev_cond, prev_val = combined_filters[prev_idx]
                # Merge multiple "contains_any" on same column
                if prev_cond == "contains_any" and cond == "contains_any":
                    prev_list = prev_val if isinstance(prev_val, list) else [prev_val]
                    new_list = prev_list + (val if isinstance(val, list) else [val])
                    combined_filters[prev_idx] = (col, "contains_any", list({v.lower() for v in new_list}))
                    continue
                # Merge multiple "contains_none" on same column
                if prev_cond == "contains_none" and cond == "contains_none":
                    prev_list = prev_val if isinstance(prev_val, list) else [prev_val]
                    new_list = prev_list + (val if isinstance(val, list) else [val])
                    combined_filters[prev_idx] = (col, "contains_none", list({v.lower() for v in new_list}))
                    continue
                # Combine a positive and a negative filter on the same column
                positive_conds = {"contains_any", "contains_all"}
                negative_conds = {"contains_none", "not_contains"}
                if (prev_cond in positive_conds and cond in negative_conds) or (prev_cond in negative_conds and cond in positive_conds):
                    # Determine includes vs excludes sets
                    if prev_cond in positive_conds:
                        includes = prev_val if isinstance(prev_val, list) else [prev_val]
                        excludes = val if isinstance(val, list) else [val]
                        require_all = (prev_cond == "contains_all")
                    else:
                        includes = val if isinstance(val, list) else [val]
                        excludes = prev_val if isinstance(prev_val, list) else [prev_val]
                        require_all = (cond == "contains_all")
                    combined_filters[prev_idx] = (col, "combined_include_exclude", (includes, excludes, require_all))
                    continue
            # New filter (no merge)
            seen_col[col] = len(combined_filters)
            combined_filters.append((col, cond, val))
        
        # Apply each filter condition vectorically
        for filter_col, filter_cond, filter_val in combined_filters:
            # Use precomputed lowercase columns for 'account' and 'description'
            if filter_col in ("account", "description"):
                col_series = df[f"{filter_col}_lower"]
            else:
                col_series = df[filter_col].astype(str).str.lower().str.strip()
            # Determine condition mask using vectorized operations
            if filter_cond == "equals":
                condition_mask = (col_series == str(filter_val).lower())
            elif filter_cond == "not_equals":
                condition_mask = (col_series != str(filter_val).lower())
            elif filter_cond == "startswith":
                condition_mask = col_series.str.startswith(str(filter_val).lower())
            elif filter_cond == "not_startswith":
                condition_mask = ~col_series.str.startswith(str(filter_val).lower())
            elif filter_cond == "endswith":
                condition_mask = col_series.str.endswith(str(filter_val).lower())
            elif filter_cond == "not_endswith":
                condition_mask = ~col_series.str.endswith(str(filter_val).lower())
            elif filter_cond == "contains_any":
                regex = self.get_regex_union([v.lower() for v in filter_val])
                condition_mask = col_series.str.contains(regex, na=False)
            elif filter_cond == "contains_none":
                regex = self.get_regex_union([v.lower() for v in filter_val])
                condition_mask = ~col_series.str.contains(regex, na=False)
            elif filter_cond == "contains_all":
                regex = self.get_regex_all([v.lower() for v in filter_val])
                condition_mask = col_series.str.contains(regex, na=False)
            elif filter_cond in ("not_contains", "not_contains_all"):
                regex = self.get_regex_all([v.lower() for v in filter_val])
                condition_mask = ~col_series.str.contains(regex, na=False)
            elif filter_cond == "level":
                # Use precomputed numeric level
                condition_mask = (df["account_level"] == int(filter_val))
            elif filter_cond == "combined_include_exclude":
                includes, excludes, require_all = filter_val
                regex = self.get_regex_include_exclude([v.lower() for v in includes], [w.lower() for w in excludes], require_all)
                condition_mask = col_series.str.contains(regex, na=False)
            else:
                raise ValueError(f"Unknown filter condition: {filter_cond}")
            # Refine the mask
            mask &= condition_mask
            if not mask.any():  # short-circuit: no rows left
                break
        # Ensure output columns exist
        for col in ("account_standard", "description_standard", "standard_criteria", "items_match"):
            if col not in df.columns:
                df[col] = ""
        # Assign standardized account/description to rows matching all criteria
        std_account, std_desc = target.split(" - ")
        df.loc[mask, "account_standard"] = std_account
        df.loc[mask, "description_standard"] = std_desc
        df.loc[mask, "standard_criteria"] = " | ".join(f"{c} {cond} {val}" for c, cond, val in filters)
        # Record which original items matched this criterion
        if mask.any():
            matched_items = (
                df.loc[mask, ["account", "description"]]
                .drop_duplicates()
                .apply(lambda row: f"{row['account']} - {row['description']}", axis=1)
                .tolist()
            )
            df.loc[mask, "items_match"] = " | ".join(matched_items)
        # Recurse into sub-criteria (refine mask for child items without copying DataFrame)
        for sub in sub_criteria:
            # Identify accounts of rows that matched this criterion, then find all rows starting with those accounts
            parent_accounts = df.loc[mask, "account"].astype(str).unique()
            if len(parent_accounts) == 0:
                continue
            # Vectorized prefix match for any of the parent account prefixes
            child_mask = df["account"].astype(str).str.startswith(tuple(parent_accounts))
            df, section_name, _, _ = self.apply_criteria(df, section_name, sub, parent_mask=child_mask, level=level+1)
        return df, section_name, std_account, std_desc


    def apply_criteria_old(
        self,
        df,
        section_name,
        criteria_item,
        parent_mask=None,
        output_file="output.txt",
        parent_criteria_info=None,
        level=1,
    ):
        """Applies a criterion and its sub-criteria to the DataFrame."""
        try:
            target = criteria_item["target_line"]
            filters = criteria_item["criteria"]
            sub_criteria = criteria_item.get("sub_criteria", [])

            if parent_criteria_info is None:
                parent_criteria_info = []

            df = df.reset_index(drop=True)
            base_mask = pd.Series([True] * len(df)) if parent_mask is None else parent_mask.copy()
            mask = base_mask.copy()

            parent_criteria_info.append({"target": target, "filters": filters})

            # Precompile regex patterns and store in cache
            def get_precompiled_regex(values):
                """Fetch precompiled regex if exists, otherwise compile and
                store it."""
                pattern_key = tuple(values)  # Convert list to tuple for immutability
                if pattern_key not in self.regex_cache:
                    self.regex_cache[pattern_key] = re.compile("|".join(map(re.escape, values)), re.IGNORECASE)
                return self.regex_cache[pattern_key]

            # Define optimized condition mapping with precompiled regex
            condition_map = {
                "equals": lambda col, val: col == val.lower(),
                "not_equals": lambda col, val: col != val.lower(),
                "startswith": lambda col, val: col.astype(str).str.startswith(val),
                "not_startswith": lambda col, val: ~col.str.startswith(val),
                "endswith": lambda col, val: col.str.endswith(val),
                "not_endswith": lambda col, val: ~col.str.endswith(val),
                "contains_all": lambda col, val: col.apply(
                    lambda x: (all(term in x.lower() for term in val) if pd.notna(x) else False)
                ),
                "contains_any": lambda col, val: col.str.contains(get_precompiled_regex(val), na=False),
                "contains_none": lambda col, val: ~col.str.contains(get_precompiled_regex(val), na=False),
                "not_contains": lambda col, val: col.apply(
                    lambda x: (not all(term in x.lower() for term in val) if pd.notna(x) else True)
                ),
                "not_contains_any": lambda col, val: col.apply(
                    lambda x: (all(term not in x.lower() for term in val) if pd.notna(x) else True)
                ),
                "level": lambda col, val: (col.str.count(r"\.") + 1) == int(val),
            }

            crits = []

            for filter_column, filter_condition, filter_value in filters:
                crits.append([filter_column, filter_condition, filter_value])

                # Convert filter values to lists for conditions that need multiple values
                if filter_condition in ["contains_any", "contains_none", "contains_all", "not_contains_all"]:
                    if not isinstance(filter_value, list):
                        filter_value = [filter_value]

                df_column_lower = df[filter_column].astype(str).str.lower().str.strip()

                # Apply filter condition using mapping
                if filter_condition in condition_map:
                    condition_mask = condition_map[filter_condition](df_column_lower, filter_value)
                    mask &= condition_mask
                else:
                    raise ValueError(f"Unknown filter condition: {filter_condition}")

            # Ensure necessary columns exist
            for col in ["account_standard", "description_standard", "standard_criteria", "items_match"]:
                if col not in df.columns:
                    df[col] = ""

            # Apply the target modifications to the DataFrame for the current mask
            account, description = target.split(" - ")
            df.loc[mask, "account_standard"] = account
            df.loc[mask, "description_standard"] = description
            df.loc[mask, "standard_criteria"] = " | ".join([f"{c[0]} {c[1]} {c[2]}" for c in crits])

            # Capture items that match the mask
            items_example = (
                df.loc[mask, ["account", "description"]]
                .drop_duplicates()
                .apply(lambda row: f"{row['account']} - {row['description']}", axis=1)
                .tolist()
            )
            df.loc[mask, "items_match"] = " | ".join(items_example)

            # Recursively apply subcriteria
            for sub in sub_criteria:
                sub_accounts = df.loc[mask, "account"].unique()
                sub_mask = (
                    df["account"].astype(str).apply(lambda x: any(x.startswith(str(acct)) for acct in sub_accounts))
                )

                df, section_name, account, description = self.apply_criteria(
                    df,
                    section_name,
                    sub,
                    parent_mask=sub_mask,
                    output_file=output_file,
                    parent_criteria_info=parent_criteria_info.copy(),
                    level=level + 1,
                )

        except Exception as e:
            print(f"criteria error {e}")

        return df, section_name, account, description

    def _filter_newer_versions(self, df):
        """Filter and keep only the newest data from groups of (company_name,
        quarter, type, frame, account).

        Args:
            df (pd.DataFrame): DataFrame containing the financial statements data.

        Returns:
            pd.DataFrame: Filtered DataFrame with only the latest versions for each group.
        """
        group_columns = ["company_name", "quarter", "type", "frame", "account"]
        version_column = "version"

        try:
            df_sorted = df.sort_values(
                by=group_columns + [version_column], ascending=[True] * len(group_columns) + [False]
            )
            df_filtered = df_sorted.drop_duplicates(subset=group_columns, keep="first")

            # Todas as outras (não mais recentes)
            mask_other = ~df_sorted.index.isin(df_filtered.index)
            df_older = df_sorted[mask_other]

            # Also return duplicates as an optional output
            df_duplicates = df_sorted[
                df_sorted.duplicated(subset=group_columns, keep=False)
                & (df_sorted.duplicated(subset=group_columns + [version_column], keep=False) == False)
            ]

            return df_filtered, df_older, df_duplicates

        except Exception as e:
            self.log_error(f"Error during filtering newer versions: {e}")
            columns, dtypes, primary_keys = self._get_table_structure(table_name=self.tbl_statements_raw, db_filepath=self.db_filepath)
            return pd.DataFrame(columns=columns), pd.DataFrame(columns=columns)

    def _transform_quarterly_values(self, df):
        """
        Apply B3 quarterly-value adjustments.

        • Prefix **03 / 04** (income-statement): December is cumulative; derive Q4 by
        subtracting Q1–Q3 (“year_end” logic).

        • Prefix **06 / 07** (cash-flow): each quarter after Q1 is converted from
        cumulative to single-period value (“cumulative” logic).

        Original dates are preserved; only the numeric “value” column is updated.
        """
        index_cols = ["company_name", "type", "frame", "account", "year"]
        merge_cols = index_cols + ["quarter_num"]

        try:
            # Work on a copy to avoid side effects
            df = df.copy()

            # Derive year and quarter number from the 'quarter' date column
            df["quarter"] = pd.to_datetime(df["quarter"])
            df["year"] = df["quarter"].dt.year
            df["quarter_num"] = df["quarter"].dt.quarter

            # Guarantee numeric dtype for arithmetic operations
            df["value"] = pd.to_numeric(df["value"], errors="coerce").fillna(0)

            # Normalize account code and capture its prefix
            df["account"] = df["account"].astype(str).str.strip()
            df["account_prefix"] = df["account"].str.split(".").str[0]

            # Keep only the latest NSD for each unique quarterly key
            df = (
                df.sort_values("nsd")
                .drop_duplicates(subset=merge_cols, keep="last")
            )

            # Log any surviving duplicates for offline inspection
            dup = df[df.duplicated(subset=merge_cols, keep=False)]
            if not dup.empty:
                self.log_error(f"{len(dup)} rows duplicated in quarterly index")
                filename = "rows_duplicated_in_quarterly_index.csv"
                temp_path = os.path.join(self.config.paths["temp_folder"], filename)
                dup.groupby(merge_cols).size().to_csv(temp_path)

            # Helper: pivot to wide, adjust, then melt back to long
            def pivot_and_adjust(group_df, tipo):
                # Convert quarterly values to wide format: one column per quarter (1 to 4)
                pivot = (
                    group_df.pivot_table(
                        index=index_cols,
                        columns="quarter_num",
                        values="value",
                        aggfunc="first",
                    )
                    .reset_index()
                    .fillna(0)
                )

                # Apply account-specific adjustment logic
                if tipo == "year_end":
                # For 'year_end': recalculate Q4 as Dec - (Q1 + Q2 + Q3)
                    pivot[4] = (
                        pivot.get(4, 0)
                        - pivot.get(1, 0)
                        - pivot.get(2, 0)
                        - pivot.get(3, 0)
                    )
                elif tipo == "cumulative":
                # For 'cumulative': convert each Q to single-period value by subtracting previous quarters
                    pivot[2] = pivot.get(2, 0) - pivot.get(1, 0)
                    pivot[3] = pivot.get(3, 0) - (pivot.get(1, 0) + pivot.get(2, 0))
                    pivot[4] = pivot.get(4, 0) - (
                        pivot.get(1, 0) + pivot.get(2, 0) + pivot.get(3, 0)
                    )

                # Convert back to long format (one row per quarter)
                melted = pivot.melt(
                    id_vars=index_cols,
                    value_vars=[1, 2, 3, 4],
                    var_name="quarter_num",
                    value_name="value",
                )

                # Identify all remaining non-index columns to preserve metadata
                other_cols = [c for c in df.columns if c not in merge_cols]

                # Get distinct rows of original metadata (including NSD, document info, etc.)
                df_temp = df[merge_cols + other_cols].drop_duplicates(subset=merge_cols)

                # Merge metadata back into melted adjusted values
                melted = (
                    melted.merge(
                        df_temp,
                        on=merge_cols,
                        how="left",
                        suffixes=("", "_original"),
                    )
                    .drop_duplicates(subset=merge_cols)
                )

                # Remove entries with no NSD at all (which should not be saved)
                melted = melted.dropna(subset=["nsd"], how="all")

                return melted
            # Loop through account prefix groups and apply their specific adjustment rules
            for prefix, tipo in [(("03", "04"), "year_end"), (("06", "07"), "cumulative")]:
                
                # Filter only the rows matching the current account prefix group
                target = df[df["account_prefix"].isin(prefix)]
                
                # Proceed only if there's data to transform
                if not target.empty:
                    
                    # Transform the quarterly values based on the account type rule
                    transformed = pivot_and_adjust(target, tipo)

                    # Extract adjusted values and align them by index for merging
                    adjusted = (
                        transformed.set_index(merge_cols)["value"]
                                .rename("adjusted_value")
                    )

                    # Replace original values with adjusted ones where available
                    df = df.set_index(merge_cols)
                    df["value"] = adjusted.combine_first(df["value"])
                    df = df.reset_index()

            # Remove helper columns
            df.drop(
                columns=["account_prefix", "year", "quarter_num"],
                inplace=True,
                errors="ignore",
            )

            return df

        except Exception as e:
            self.log_error(f"Erro ao transformar valores trimestrais: {e}")
            return df

    def adjust_columns(self, df0):
        """docstring."""
        try:
            # Step 1: Drop rows where 'account_standard' is empty or NaN
            df = df0.dropna(subset=["account_standard"])

            df = df[df["account_standard"].str.strip() != ""]

            # Step 2: Drop the unnecessary columns
            df = df.drop(columns=["account", "description"])

            # Step 3: Rename 'account_standard' to 'account' and 'description_standard' to 'description'
            df = df.rename(columns={"account_standard": "account", "description_standard": "description"})

            # Step 4: Reorder the DataFrame columns
            df = df[self.config.domain["statements_columns"]]

            # Step 5: Sort by the specified columns
            df = df.sort_values(by=self.config.domain["statements_order"])

        except Exception as e:
            self.log_error(e)

        return df

    def get_targets(self, use_index=True, process_new=True, limit=False, max_retries=None, wait_time=None):
        """Retrieve new rows from tbl_statements_raw that are not in
        tbl_statements_normalized.

        Returns:
            pd.DataFrame: Filtered DataFrame containing only the new records.
        """
        try:
            max_retries = max_retries or self.config.selenium["max_retries"]
            wait_time = wait_time or self.dynamic_sleep()

            # Gerar condição de JOIN baseada nas primary keys
            on_conditions = " AND ".join([f"r.{col} = n.{col}" for col in self.primary_key_columns])

            # Construção da SQL otimizada usando NOT EXISTS
            query_base = f"""
            FROM {self.tbl_statements_raw} AS r
            {f"INDEXED BY {self.idx_statements_raw}" if use_index else ""}
            WHERE NOT EXISTS (
                SELECT 1 FROM {self.tbl_statements_normalized} AS n
                WHERE {on_conditions}
            )
            """

            # If process_all is False (default), exclude already processed records
            if process_new:
                query_base += " AND (r.processed IS NULL OR r.processed <> r.version)"

            # Add limit (for debug speed)
            limit_rows = 100000
            limit_clause = f"LIMIT {limit_rows}" if limit else ""

            # Queries completas
            query = f"SELECT r.* {query_base} {limit_clause};"
            query_count = f"SELECT COUNT(*) {query_base};"

            chunk_size = int(self.config.scraping["chunk_size"] / 10)  # Ajuste para processamento em batches

            df_list = []
            attempts = 0
            print("should use load_data!!!! adjust")
            while attempts < max_retries:
                with self.db_lock:  # Garantia de acesso seguro ao banco
                    try:
                        with sqlite3.connect(self.db_filepath) as conn:
                            if not limit:
                                # Contar total de registros para processar
                                total_rows = pd.read_sql_query(query_count, conn).iloc[0, 0]
                            else:
                                total_rows = limit_rows

                            if total_rows == 0:
                                return pd.DataFrame()  # Retorna um DataFrame vazio

                            # Processamento por chunks com barra de progresso
                            with tqdm(total=total_rows, unit="rows", desc="") as pbar:
                                for chunk in pd.read_sql_query(query, conn, chunksize=chunk_size):
                                    df_list.append(chunk)
                                    pbar.update(len(chunk))  # Atualiza a barra de progresso

                            df = pd.concat(df_list, ignore_index=True)
                        return df

                    except sqlite3.OperationalError as e:
                        if "database is locked" in str(e):
                            attempts += 1
                            time.sleep(wait_time)  # Aguarda antes de tentar novamente
                        else:
                            raise  # Repassa qualquer outro erro inesperado

            raise Exception(f"Falha ao buscar novos registros após {max_retries} tentativas.")

        except Exception as e:
            self.log_error(e)

    def iter_statements_by_company(self, db_path: str):
        """
        Itera sobre os dados de tbl_statements_raw agrupados por company_name,
        retornando apenas os registros que ainda não foram processados
        ou cuja versão foi atualizada.
        """
        try:
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            # Seleciona empresas com dados pendentes de processamento
            query_companies = """
                SELECT DISTINCT company_name
                FROM tbl_statements_raw
                WHERE processed IS NULL OR processed <> version
                ORDER BY company_name
            """
            for row in cursor.execute(query_companies):
                company_name = row[0]

                # Coleta apenas os dados brutos ainda não processados ou com nova versão
                query_data = """
                    SELECT *
                    FROM tbl_statements_raw
                    WHERE company_name = ?
                    AND (processed IS NULL OR processed <> version)
                """
                cursor2 = conn.cursor()
                cursor2.execute(query_data, (company_name,))
                statements = cursor2.fetchall()
                columns = [desc[0] for desc in cursor2.description]

                yield company_name, statements, columns

            conn.close()
        except Exception as e:
            self.log_error(e)

    def main(self, thread=True):
        """docstring."""
        try:
            # start_time = time.time()
            # targets = self.load_data(table_name=self.tbl_statements_raw, db_filepath=self.db_filepath)
            # end_time = time.time()

            # start_time_2 = time.time()
            # targets = self.load_data(table_name=self.tbl_statements_raw, db_filepath=self.db_filepath, multi_thread=False)
            # end_time_2 = time.time()


            # Get updated targets
            # targets = self.get_targets(process_new=True, limit=False)

            company_names = [row[0] for row in sqlite3.connect(self.db_filepath).execute(
                "SELECT DISTINCT company_name FROM tbl_company_info"
            )]

            start_time = time.time()
            statement_iterator = self.iter_statements_by_company(self.db_filepath)
            for i, (company_name, statements, columns) in enumerate(statement_iterator):
                targets = pd.DataFrame.from_records(statements, columns=columns)

                # Chama run para processar os dados dessa empresa
                result = self.run(
                    targets,
                    thread=thread,
                    module_name=self.inspect.getmodule(self.inspect.currentframe()).__name__, 
                    msg=False
                )

                # Salva se houver resultado
                if not result.empty:
                    self.save_to_db(
                        dataframe=result,
                        table_name=self.tbl_statements_normalized,
                        db_filepath=self.db_filepath, 
                        alert=False
                    )

                extra_info = [f'{company_name} {len(result)} registros normalizados']
                self.print_info(i, len(company_names), start_time, extra_info=extra_info)

            self.db_optimize(self.db_filepath)
            

        except Exception as e:
            self.log_error(e)

        return True
