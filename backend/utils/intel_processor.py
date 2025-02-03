from threading import Lock
import pandas as pd
import time
import re

from utils.base_processor import BaseProcessor
from utils import intel

class IntelProcessor(BaseProcessor):
    '''
    docstrings
    '''
    def __init__(self):
        '''
        docstrings
        '''
        super().__init__()
        self.db_lock = Lock()  # Initialize a threading Lock

        self.section_criterias = {
            'Composição do Capital': intel.section_0_criteria, 
            'Balanço Patrimonial Ativo': intel.section_1_criteria,
            'Balanço Patrimonial Passivo': intel.section_2_criteria,
            'Demonstração do Resultado': intel.section_3_criteria,
            'Demonstração de Fluxo de Caixa': intel.section_6_criteria,
            'Demonstração de Valor Adiconado': intel.section_7_criteria,
        }

    def process_instance(self, sub_batch, progress):
        """
        Process a single batch by delegating 
        from abstract base_processor method 
        to this class process_batch (true process info method) 
        via this process_instance method (create instance method).
        
        sub_batch
        progress

        return result from process_batch
        """
        try:
            print(f'Starting batch {progress["batch_index"]}/{progress["total_batches"]} {100*progress["batch_index"]/progress["total_batches"]:.02f}%')
            intel_processor = IntelProcessor()
            
            # Delegate to process_batch for the actual batch processing
            result = intel_processor.process_batch(sub_batch, progress)
            self.save_to_db(dataframe=result, table_name=self.config.standart_table, db_filepath=self.config.standart_filepath)

            # Clean up driver after processing
            intel_processor.close_driver()

            # first = f"{sub_batch['company_name'].iloc[0]}"
            # last = f"{sub_batch['company_name'].iloc[-1]}"
            # extra_info = [f"Worker {progress['thread_id']}"]
            # self.print_info(progress['batch_index'], progress['total_batches'], progress['start_time'], extra_info)

        except Exception as e:
            self.log_error(f"Error in process_instance: {e}")
            self.close_driver()  # Ensure driver is closed even on errors

        return result

    def process_batch(self, sub_batch, progress):
        '''
        '''
        result = ''

        try:
            companies = sub_batch['company_name'].unique()

            start_time = time.time()
            for i, company in enumerate(companies):
                mask = sub_batch['company_name'] == company
                df = sub_batch[mask]
                result = self.generate_standard_financial_statements(df, progress)

                # sanitize db
                result = self.adjust_columns(result)
                self.save_to_db(dataframe=result, table_name=self.config.standart_table, db_filepath=self.config.standart_filepath)

                extra_info = [progress['thread_id'], progress['batch_index'], company]
                self.print_info(i, len(companies), start_time, extra_info, indent_level=1)

        except Exception as e:
            self.log_error(e)

        return result

    def generate_standard_financial_statements(self, sub_batch, progress):
        """
        Generates various sections of the financial statements based on the provided DataFrame.

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
            sector = sub_batch.iloc[0]['sector']
            subsector = sub_batch.iloc[0]['subsector']
            segment = sub_batch.iloc[0]['segment']
            company_name = sub_batch.iloc[0]['company_name']
            start_time = time.time()
            # Loop through each section in the standardization pack
            for i, (section_name, section_criteria) in enumerate(self.section_criterias.items()):
                sub_batch = self.apply_section_criteria(sub_batch, section_name, section_criteria)

                extra_info = [progress['thread_id'], progress['batch_index'], company_name, section_name.upper(), ]
                self.print_info(i, len(self.section_criterias), start_time, extra_info, indent_level=3)

        except Exception as e:
            print(f'criteria error {e}')
        
        return sub_batch

    def apply_section_criteria(self, df, section_name, section_criteria, output_file='output.txt'):
        """
        Main function to apply a criteria tree to the DataFrame. Allow recursivity for subcriteria

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
                df, section_name, account, description = self.apply_criteria(df, section_name, criteria_item, output_file=output_file)
                # extra_info = [section_name.upper(), account, description]
                # self.print_info(i, len(section_criteria), start_time, extra_info, indent_level=4)

        except Exception as e:
            self.print_info(e)

        return df

    def apply_criteria(self, df, section_name, criteria_item, parent_mask=None, output_file='output.txt', parent_criteria_info=None, level=1):
        """
        Applies a criterion and its sub-criteria to the DataFrame.

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
            target = criteria_item['target_line']
            filters = criteria_item['criteria']
            sub_criteria = criteria_item.get('sub_criteria', [])
            
            # Initialize the parent criteria info list if not provided
            if parent_criteria_info is None:
                parent_criteria_info = []

            # Initialize the mask for the entire DataFrame or use the parent mask
            df = df.reset_index(drop=True)  # Reset df index for correct df and mask alignment
            base_mask = pd.Series([True] * len(df)) if parent_mask is None else parent_mask.copy()
            mask = base_mask.copy()

            # Append the current criteria to the parent criteria info
            parent_criteria_info.append({'target': target, 'filters': filters})

            # Define a dictionary to map filter conditions to functions
            condition_map = {
                'equals': lambda col, val: col == val.lower(),
                'not_equals': lambda col, val: col != val.lower(),
                # 'startswith': lambda col, val: col.str.startswith(tuple(map(str.lower, val))),
                'startswith': lambda col, val: col.astype(str).str.startswith(val),
                'not_startswith': lambda col, val: ~col.str.startswith(tuple(map(str.lower, val))),
                'endswith': lambda col, val: col.str.endswith(tuple(map(str.lower, val))),
                'not_endswith': lambda col, val: ~col.str.endswith(tuple(map(str.lower, val))),
                'contains_all': lambda col, val: col.apply(lambda x: all(term in x.lower() for term in val) if pd.notna(x) else False),
                'contains_any': lambda col, val: col.str.contains('|'.join(map(re.escape, val)), case=False, na=False),
                'contains_none': lambda col, val: ~col.str.contains('|'.join(map(re.escape, val)), case=False, na=False),
                'not_contains': lambda col, val: col.apply(lambda x: not all(term in x.lower() for term in val) if pd.notna(x) else True),
                'not_contains_any': lambda col, val: col.apply(lambda x: all(term not in x.lower() for term in val) if pd.notna(x) else True),
                'level': lambda col, val: (col.str.count(r'\.') + 1) == int(val)  # Merged level filter for exact levels
            }

            crits = []
            
            # Apply each filter to the mask
            for filter_column, filter_condition, filter_value in filters:
                crits.append([filter_column, filter_condition, filter_value])

                # Convert filter values to lists for conditions that need lists
                if filter_condition in ['contains_any', 'contains_none', 'contains_all', 'not_contains_all']:
                    if not isinstance(filter_value, list):
                        filter_value = [filter_value]

                # df_column_lower = df[filter_column].str.lower() if df[filter_column].dtype == 'O' else df[filter_column]
                # df_column_lower = df[filter_column].str.lower().str.strip() if df[filter_column].dtype == 'O' else df[filter_column]
                # df_column_lower = df[filter_column].astype(str).str.lower().str.strip()
                df_column_lower = df[filter_column].astype(str).str.lower().str.strip() if df[filter_column].dtype == 'O' else df[filter_column]

                # Apply the filter condition using the mapping
                if filter_condition in condition_map:
                    condition_mask = condition_map[filter_condition](df_column_lower, filter_value)
                    mask &= condition_mask
                else:
                    raise ValueError(f"Unknown filter condition: {filter_condition}")

            # Ensure 'account_standard', 'description_standard', 'standard_criteria', and 'items_match' columns exist
            if 'account_standard' not in df.columns:
                df['account_standard'] = ''
            if 'description_standard' not in df.columns:
                df['description_standard'] = ''
            if 'standard_criteria' not in df.columns:
                df['standard_criteria'] = ''
            if 'items_match' not in df.columns:
                df['items_match'] = ''

            # Apply the target modifications to the DataFrame for the current mask
            account, description = target.split(' - ')
            df.loc[mask, 'account_standard'] = account
            df.loc[mask, 'description_standard'] = description
            df.loc[mask, 'standard_criteria'] = self.config.joint2.join([f"{c[0]} {c[1]} {c[2]}" for c in crits])  # Add criteria details
            
            # Capture "contém itens como" (items that match the mask)
            items_example = df.loc[mask, ['account', 'description']].drop_duplicates().apply(lambda row: f"{row['account']}{self.config.joint}{row['description']}", axis=1).tolist()
            df.loc[mask, 'items_match'] = self.config.joint2.join(items_example)

            # Recursively apply subcriteria using a new refined mask
            for sub in sub_criteria:
                # Create a new mask for sub-criteria by filtering the df with 'startswith' of the current filtered results
                sub_accounts = df.loc[mask, 'account'].unique()
                # sub_mask = df['account'].apply(lambda x: any(x.startswith(acct) for acct in sub_accounts))
                sub_mask = df['account'].astype(str).apply(lambda x: any(x.startswith(str(acct)) for acct in sub_accounts))

                df, section_name, account, description = self.apply_criteria(df, section_name, sub, parent_mask=sub_mask, output_file=output_file, parent_criteria_info=parent_criteria_info.copy(), level=level+1)

        except Exception as e:
            pass
            print(f'criteria error {e}')
        
        return df, section_name, account, description

    def adjust_columns(self, df0):
        '''
        docstring
        '''
        try:
            # Step 1: Drop rows where 'account_standard' is empty or NaN
            df = df0.dropna(subset=['account_standard'])

            df = df[df['account_standard'].str.strip() != '']

            # Step 2: Drop the unnecessary columns
            df = df.drop(columns=['account', 'description'])

            # Step 3: Rename 'account_standard' to 'account' and 'description_standard' to 'description'
            df = df.rename(columns={'account_standard': 'account', 'description_standard': 'description'})

            # Step 4: Reorder the DataFrame columns
            df = df[self.config.statements_columns]

            # Step 5: Sort by the specified columns
            df = df.sort_values(by=self.config.statements_order)

        except Exception as e:
            self.log_error(e)

        return df

    def get_scrape_targets(self, existing_data, new_data):
        """
        Filters out existing primary keys from new data.
        
        Args:
            existing_data (pd.DataFrame): Existing financial statements data.
            new_data (pd.DataFrame): Newly scraped financial statements data.
            
        Returns:
            pd.DataFrame: Filtered DataFrame containing only the new records.
        """
        # Define the primary key columns
        primary_key_columns = self.config.statements_sheet_columns
        result = pd.DataFrame()
        try:
            # Check if new_data is empty
            if new_data.empty:
                return existing_data

            # Ensure the primary key columns exist in both datasets
            if not all(col in existing_data.columns for col in primary_key_columns):
                raise ValueError("Missing primary key columns in existing_data.")
            if not all(col in new_data.columns for col in primary_key_columns):
                raise ValueError("Missing primary key columns in new_data.")
            
            # Standardize data types for primary key columns
            existing_data = existing_data.copy()  # Ensure it's a full copy
            new_data = new_data.copy()  # Ensure it's a full copy
            for col in primary_key_columns:
                if col in existing_data.columns and col in new_data.columns:
                    # Convert both columns to string (or other appropriate types)
                    existing_data[col] = existing_data[col].astype(str)
                    new_data[col] = new_data[col].astype(str)

            chunk_size = self.config.chunk_size
            result = pd.concat(
                (
                    pd.merge(
                        existing_data.iloc[i:i + chunk_size], 
                        new_data[primary_key_columns],  
                        on=primary_key_columns,  
                        how='left',  
                        indicator=True
                    ).query("_merge == 'left_only'").drop(columns=['_merge'])
                    for i in range(0, len(existing_data), chunk_size)
                ),
                ignore_index=True
            )

        except Exception as e:
            self.log_error(e)
            result = pd.DataFrame()

        return result

    def main(self, thread=True):
        '''
        docstring
        '''
        try:
            # optimize db before return
            self.db_optimize(self.config.standart_filepath)

            # Load necessary data as scrape targets
            financial_statements = self.load_data(table_name=self.config.statements_file, db_filepath=self.config.initial_filepath)
            # # pre-debug
            # financial_statements[:1000000].to_csv('financial_statements.csv', index=False)

            standart_statements = self.load_data(table_name=self.config.standart_table, db_filepath=self.config.standart_filepath)
            # # pre-debug
            # standart_statements[:1000000].to_csv('standart_statements.csv', index=False)

            # # debug
            # financial_statements = pd.read_csv('financial_statements.csv')
            # standart_statements = pd.read_csv('standart_statements.csv')

            # load statements and process intel
            scrape_targets = self.get_scrape_targets(financial_statements, standart_statements[:100000])

            # Exit if no scrape_targets
            if scrape_targets.empty:
                self.db_optimize(self.config.standart_filepath)
                return True

            # Process targets using threading or sequential logic
            processed_data = self.run(scrape_targets, thread=thread, module_name=self.inspect.getmodule(self.inspect.currentframe()).__name__)

            # Save processed data
            if not processed_data.empty:
                self.save_to_db(dataframe=processed_data, table_name=self.config.standart_table, db_filepath=self.config.standart_filepath)

            # optimize db before return
            self.db_optimize(self.config.standart_filepath)

        except Exception as e:
            self.log_error(e)

        return True
