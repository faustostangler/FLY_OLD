import re
import sqlite3
import time
from threading import Lock

import pandas as pd
from tqdm import tqdm
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
        self.tbl_statements_raw = self.config.databases["raw"]["table"][
            "statements_raw"
        ]
        self.idx_statements_raw = self.config.databases["raw"]["index"][
            "statements_raw"
        ]
        self.tbl_statements_normalized = self.config.databases["raw"]["table"][
            "statements_normalized"
        ]
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

    def process_instance(self, sub_batch, progress):
        """Process a single batch by delegating from abstract base_processor
        method to this class process_batch (true process info method) via this
        process_instance method (create instance method).

        sub_batch progress

        return result from process_batch
        """
        result = pd.DataFrame()  # Return an empty DataFrame on failure

        try:
            print(
                f'Starting batch {progress["batch_index"]}/{progress["total_batches"]} {100*progress["batch_index"]/progress["total_batches"]:.02f}%'
            )
            batch_processor = IntelProcessor()

            # Delegate to process_batch for the actual batch processing
            result, benchmark_results = batch_processor.benchmark_function(
                batch_processor.process_batch, sub_batch, progress, benchmark_mode=False
            )

            # Save result to database
            self.save_to_db(
                dataframe=result,
                table_name=self.tbl_statements_normalized,
                db_filepath=self.db_filepath,
                alert=False, 
                update=False, 
            )

            # first = f"{sub_batch['company_name'].iloc[0]}"
            # last = f"{sub_batch['company_name'].iloc[-1]}"
            # extra_info = [f"Worker {progress['thread_id']}"]
            # self.print_info(progress['batch_index'], progress['total_batches'], progress['start_time'], extra_info)

        except Exception as e:
            self.log_error(f"Error in process_instance: {e}")

        return result

    def process_batch(self, sub_batch, progress):
        """"""
        result = ""

        try:
            companies = sub_batch["company_name"].unique()
            total_companies = len(companies)

            start_time = time.time()
            for i, company in enumerate(companies):
                mask = sub_batch["company_name"] == company
                df = sub_batch[mask]

                result = self.generate_standard_financial_statements(df, progress)

                # sanitize db
                result = self.adjust_columns(result)

                # Outlier detection
                result = self.detect_and_correct_outliers(result)

                # save results
                self.save_to_db(
                    dataframe=result,
                    table_name=self.tbl_statements_normalized,
                    db_filepath=self.db_filepath,
                    alert=False,
                    update=False, 
                )

                # Atualiza a coluna processed para todas as linhas na tbl_statements_raw
                sql_update = """
                    UPDATE tbl_statements_raw
                    SET processed = version
                    WHERE company_name = ?;
                """

                self.save_to_db(
                    dataframe=df,  # No dataframe needed
                    table_name="tbl_statements_raw",
                    db_filepath=self.db_filepath,
                    alert=False,
                    sql_update=sql_update,
                    sql_update_params=(company,)  # Pass company name
                )

                # Log progress
                actual_item = progress["batch_start"] + i
                total_items = progress["scrape_size"] + 1
                worker_info = f"Worker {progress['thread_id']} Item {100*actual_item/total_items:.02f}% ({actual_item}/{total_items})"

                # Retrieve quarter max and min values
                quarter_min = (
                    pd.to_datetime(df["quarter"].min(), errors="coerce").strftime(
                        "%Y-%m"
                    )
                    if not df["quarter"].isna().all()
                    else None
                )
                quarter_max = (
                    pd.to_datetime(df["quarter"].max(), errors="coerce").strftime(
                        "%Y-%m"
                    )
                    if not df["quarter"].isna().all()
                    else None
                )

                # Retrieve first row values for sector, subsector, and segment
                sector = df["sector"].iloc[0] if not df.empty else None
                subsector = df["subsector"].iloc[0] if not df.empty else None
                segment = df["segment"].iloc[0] if not df.empty else None

                quarter_info = f"from {quarter_min} to {quarter_max}"

                extra_info = [
                    worker_info,
                    company,
                    quarter_info,
                ]
                self.print_info(
                    i, len(companies), start_time, extra_info, indent_level=0
                )

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
            sector = sub_batch.iloc[0]["sector"]
            subsector = sub_batch.iloc[0]["subsector"]
            segment = sub_batch.iloc[0]["segment"]
            company_name = sub_batch.iloc[0]["company_name"]
            # Loop through each section in the standardization pack
            start_time = time.time()
            for i, (section_name, section_criteria) in enumerate(
                self.section_criterias.items()
            ):
                sub_batch = self.apply_section_criteria(
                    sub_batch, section_name, section_criteria
                )

                # extra_info = [progress['thread_id'], progress['batch_index'], company_name, section_name.upper(), ]
                # self.print_info(i, len(self.section_criterias), start_time, extra_info, indent_level=3)

        except Exception as e:
            print(f"criteria error {e}")

        return sub_batch

    def apply_section_criteria(
        self, df, section_name, section_criteria, output_file="output.txt"
    ):
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

    def apply_criteria(
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
            base_mask = (
                pd.Series([True] * len(df))
                if parent_mask is None
                else parent_mask.copy()
            )
            mask = base_mask.copy()

            parent_criteria_info.append({"target": target, "filters": filters})

            # Precompile regex patterns and store in cache
            def get_precompiled_regex(values):
                """Fetch precompiled regex if exists, otherwise compile and
                store it."""
                pattern_key = tuple(values)  # Convert list to tuple for immutability
                if pattern_key not in self.regex_cache:
                    self.regex_cache[pattern_key] = re.compile(
                        "|".join(map(re.escape, values)), re.IGNORECASE
                    )
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
                    lambda x: (
                        all(term in x.lower() for term in val) if pd.notna(x) else False
                    )
                ),
                "contains_any": lambda col, val: col.str.contains(
                    get_precompiled_regex(val), na=False
                ),
                "contains_none": lambda col, val: ~col.str.contains(
                    get_precompiled_regex(val), na=False
                ),
                "not_contains": lambda col, val: col.apply(
                    lambda x: (
                        not all(term in x.lower() for term in val)
                        if pd.notna(x)
                        else True
                    )
                ),
                "not_contains_any": lambda col, val: col.apply(
                    lambda x: (
                        all(term not in x.lower() for term in val)
                        if pd.notna(x)
                        else True
                    )
                ),
                "level": lambda col, val: (col.str.count(r"\.") + 1) == int(val),
            }

            crits = []

            for filter_column, filter_condition, filter_value in filters:
                crits.append([filter_column, filter_condition, filter_value])

                # Convert filter values to lists for conditions that need multiple values
                if filter_condition in [
                    "contains_any",
                    "contains_none",
                    "contains_all",
                    "not_contains_all",
                ]:
                    if not isinstance(filter_value, list):
                        filter_value = [filter_value]

                df_column_lower = df[filter_column].astype(str).str.lower().str.strip()

                # Apply filter condition using mapping
                if filter_condition in condition_map:
                    condition_mask = condition_map[filter_condition](
                        df_column_lower, filter_value
                    )
                    mask &= condition_mask
                else:
                    raise ValueError(f"Unknown filter condition: {filter_condition}")

            # Ensure necessary columns exist
            for col in [
                "account_standard",
                "description_standard",
                "standard_criteria",
                "items_match",
            ]:
                if col not in df.columns:
                    df[col] = ""

            # Apply the target modifications to the DataFrame for the current mask
            account, description = target.split(" - ")
            df.loc[mask, "account_standard"] = account
            df.loc[mask, "description_standard"] = description
            df.loc[mask, "standard_criteria"] = " | ".join(
                [f"{c[0]} {c[1]} {c[2]}" for c in crits]
            )

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
                    df["account"]
                    .astype(str)
                    .apply(
                        lambda x: any(x.startswith(str(acct)) for acct in sub_accounts)
                    )
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
        """Applies a criterion and its sub-criteria to the DataFrame.

        Parameters:
            df (pd.DataFrame): The DataFrame to be modified.
            criteria_item (dict): A dictionary containing 'target', 'filter', and 'sub_criteria'.
            parent_mask (pd.Series): Optional parent level mask to apply.
            output_file (str): Path to the output file.
            parent_criteria_info (list): List to keep track of parent criteria for output purposes.

        Returns:
            pd.DataFrame: The modified DataFrame.
        """
        try:
            target = criteria_item["target_line"]
            filters = criteria_item["criteria"]
            sub_criteria = criteria_item.get("sub_criteria", [])

            # Initialize the parent criteria info list if not provided
            if parent_criteria_info is None:
                parent_criteria_info = []

            # Initialize the mask for the entire DataFrame or use the parent mask
            df = df.reset_index(
                drop=True
            )  # Reset df index for correct df and mask alignment
            base_mask = (
                pd.Series([True] * len(df))
                if parent_mask is None
                else parent_mask.copy()
            )
            mask = base_mask.copy()

            # Append the current criteria to the parent criteria info
            parent_criteria_info.append({"target": target, "filters": filters})

            # Define a dictionary to map filter conditions to functions
            condition_map = {
                "equals": lambda col, val: col == val.lower(),
                "not_equals": lambda col, val: col != val.lower(),
                # 'startswith': lambda col, val: col.str.startswith(tuple(map(str.lower, val))),
                "startswith": lambda col, val: col.astype(str).str.startswith(val),
                "not_startswith": lambda col, val: ~col.str.startswith(
                    tuple(map(str.lower, val))
                ),
                "endswith": lambda col, val: col.str.endswith(
                    tuple(map(str.lower, val))
                ),
                "not_endswith": lambda col, val: ~col.str.endswith(
                    tuple(map(str.lower, val))
                ),
                "contains_all": lambda col, val: col.apply(
                    lambda x: (
                        all(term in x.lower() for term in val) if pd.notna(x) else False
                    )
                ),
                "contains_any": lambda col, val: col.str.contains(
                    "|".join(map(re.escape, val)), case=False, na=False
                ),
                "contains_none": lambda col, val: ~col.str.contains(
                    "|".join(map(re.escape, val)), case=False, na=False
                ),
                "not_contains": lambda col, val: col.apply(
                    lambda x: (
                        not all(term in x.lower() for term in val)
                        if pd.notna(x)
                        else True
                    )
                ),
                "not_contains_any": lambda col, val: col.apply(
                    lambda x: (
                        all(term not in x.lower() for term in val)
                        if pd.notna(x)
                        else True
                    )
                ),
                "level": lambda col, val: (col.str.count(r"\.") + 1)
                == int(val),  # Merged level filter for exact levels
            }

            crits = []

            # Apply each filter to the mask
            for filter_column, filter_condition, filter_value in filters:
                crits.append([filter_column, filter_condition, filter_value])

                # Convert filter values to lists for conditions that need lists
                if filter_condition in [
                    "contains_any",
                    "contains_none",
                    "contains_all",
                    "not_contains_all",
                ]:
                    if not isinstance(filter_value, list):
                        filter_value = [filter_value]

                # df_column_lower = df[filter_column].str.lower() if df[filter_column].dtype == 'O' else df[filter_column]
                # df_column_lower = df[filter_column].str.lower().str.strip() if df[filter_column].dtype == 'O' else df[filter_column]
                # df_column_lower = df[filter_column].astype(str).str.lower().str.strip()
                df_column_lower = (
                    df[filter_column].astype(str).str.lower().str.strip()
                    if df[filter_column].dtype == "O"
                    else df[filter_column]
                )

                # Apply the filter condition using the mapping
                if filter_condition in condition_map:
                    condition_mask = condition_map[filter_condition](
                        df_column_lower, filter_value
                    )
                    mask &= condition_mask
                else:
                    raise ValueError(f"Unknown filter condition: {filter_condition}")

            # Ensure 'account_standard', 'description_standard', 'standard_criteria', and 'items_match' columns exist
            if "account_standard" not in df.columns:
                df["account_standard"] = ""
            if "description_standard" not in df.columns:
                df["description_standard"] = ""
            if "standard_criteria" not in df.columns:
                df["standard_criteria"] = ""
            if "items_match" not in df.columns:
                df["items_match"] = ""

            # Apply the target modifications to the DataFrame for the current mask
            account, description = target.split(" - ")
            df.loc[mask, "account_standard"] = account
            df.loc[mask, "description_standard"] = description
            df.loc[mask, "standard_criteria"] = self.config.domain["sep_pipe"].join(
                [f"{c[0]} {c[1]} {c[2]}" for c in crits]
            )  # Add criteria details

            # Capture "contém itens como" (items that match the mask)
            items_example = (
                df.loc[mask, ["account", "description"]]
                .drop_duplicates()
                .apply(
                    lambda row: f"{row['account']}{self.config.domain['sep_dash']}{row['description']}",
                    axis=1,
                )
                .tolist()
            )
            df.loc[mask, "items_match"] = self.config.domain["sep_pipe"].join(
                items_example
            )

            # Recursively apply subcriteria using a new refined mask
            for sub in sub_criteria:
                # Create a new mask for sub-criteria by filtering the df with 'startswith' of the current filtered results
                sub_accounts = df.loc[mask, "account"].unique()
                # sub_mask = df['account'].apply(lambda x: any(x.startswith(acct) for acct in sub_accounts))
                sub_mask = (
                    df["account"]
                    .astype(str)
                    .apply(
                        lambda x: any(x.startswith(str(acct)) for acct in sub_accounts)
                    )
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
            pass
            print(f"criteria error {e}")

        return df, section_name, account, description

    def adjust_columns(self, df0):
        """docstring."""
        try:
            # Step 1: Drop rows where 'account_standard' is empty or NaN
            df = df0.dropna(subset=["account_standard"])

            df = df[df["account_standard"].str.strip() != ""]

            # Step 2: Drop the unnecessary columns
            df = df.drop(columns=["account", "description"])

            # Step 3: Rename 'account_standard' to 'account' and 'description_standard' to 'description'
            df = df.rename(
                columns={
                    "account_standard": "account",
                    "description_standard": "description",
                }
            )

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
            wait_time = wait_time or self.config.selenium["wait_time"]

            # Gerar condição de JOIN baseada nas primary keys
            on_conditions = " AND ".join(
                [f"r.{col} = n.{col}" for col in self.primary_key_columns]
            )

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

            chunk_size = int(
                self.config.scraping["chunk_size"] / 10
            )  # Ajuste para processamento em batches

            df_list = []
            attempts = 0

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
                                for chunk in pd.read_sql_query(
                                    query, conn, chunksize=chunk_size
                                ):
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

            raise Exception(
                f"Falha ao buscar novos registros após {max_retries} tentativas."
            )

        except Exception as e:
            self.log_error(e)

    def main(self, thread=True):
        """docstring."""
        try:
            # Get updated targets
            targets = self.get_targets(process_new=True, limit=False)

            # Exit if no targets
            if targets.empty:
                self.db_optimize(self.config.databases["raw"]["filepath"])
                return True

            # Process targets using threading or sequential logic
            result = self.run(
                targets,
                thread=thread,
                module_name=self.inspect.getmodule(
                    self.inspect.currentframe()
                ).__name__,
            )

            # Save processed data
            if not result.empty:
                self.save_to_db(
                    dataframe=result,
                    table_name=self.tbl_statements_normalized,
                    db_filepath=self.db_filepath,
                )

            # optimize db before return
            self.db_optimize(self.db_filepath)

        except Exception as e:
            self.log_error(e)

        return True
