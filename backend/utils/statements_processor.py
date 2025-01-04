import pandas as pd
from threading import Lock
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from io import StringIO
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support import expected_conditions as EC

from utils.base_processor import BaseProcessor

class StatementsProcessor(BaseProcessor):
    '''
    Financial Statements
    '''
    def __init__(self):
        super().__init__()
        self.db_lock = Lock()  # Initialize a threading Lock

    def get_scrape_targets(self, company_info, existing_nsd, financial_statements):
        '''
        '''
        last_order = 'ZZZZZZZZZZ'
        scrape_order = ['sector', 'subsector', 'segment', 'company_name', 'quarter', 'version']

        try:
            # Assuming `existing_nsd` is your DataFrame and `statements_types` is the list of desired nsd_types
            nsd_df = existing_nsd[existing_nsd['nsd_type'].isin(self.config.statements_types)]

            # merge nsd and company info
            nsd_company_df = pd.merge(nsd_df, company_info, on='company_name', how='inner')
            scrape_targets = nsd_company_df[~nsd_company_df['nsd'].isin(financial_statements['nsd'].unique())]

            # Custom sorting to place empty fields last
            scrape_targets.loc[scrape_targets['sector'] == '', 'sector'] = last_order
            scrape_targets.loc[scrape_targets['subsector'] == '', 'subsector'] = last_order
            scrape_targets.loc[scrape_targets['segment'] == '', 'segment'] = last_order

            # Order the list by sector, subsector, segment, company_name, quarter, and version
            scrape_targets = scrape_targets.sort_values(by=scrape_order, ascending=True)

            # Restore empty fields
            scrape_targets.loc[scrape_targets['sector'] == last_order, 'sector'] = ''
            scrape_targets.loc[scrape_targets['subsector'] == last_order, 'subsector'] = ''
            scrape_targets.loc[scrape_targets['segment'] == last_order, 'segment'] = ''

            print(f'{len(scrape_targets)} items to download')
            return scrape_targets

        except Exception as e:
            self.log_error(e)

        return scrape_targets

    def process_instance_old(self, sub_batch, progress):
        """
        Create a new instance of StatementsDataprocess_batch and run the process_batch.
        This ensures each batch has its own WebDriver instance.
        """
        try:
            processor = StatementsProcessor()

            processed_batch = processor.process_batch(sub_batch, progress)

            processor.close_driver()

        except Exception as e:
            self.log_error(e)

        return processed_batch

    def process_instance(self, sub_batch, progress):
        """
        Process a single batch by delegating to process_batch.
        """
        try:
            return self.process_batch(sub_batch, progress)
        except Exception as e:
            self.log_error(f"Error in process_instance: {e}")
            return pd.DataFrame()  # Return an empty DataFrame on failure

    def process_batch(self, sub_batch, progress):
        """
        Process a batch of financial data by iterating over rows and scraping statements.
        """
        processed_data = []

        start_time = time.time()
        for i, (_, row) in enumerate(sub_batch.iterrows()):
            try:
                # Extract and process data for each row
                row_data = self._process_company_quarter_data(row)
                processed_data.extend(row_data)

                # Log progress
                extra_info = [f"{i+1}/{len(sub_batch)}", row['company_name'], row['quarter'], f"v{row['version']}"]
                self.print_info(progress['batch_start'] + i, progress['scrape_size'], start_time, extra_info)

            except Exception as e:
                self.log_error(f"Error processing row {i}: {e}")
                row_data = 'empty'

        # Combine results into a single DataFrame
        if processed_data:
            result = pd.concat(processed_data, ignore_index=True)
        else:
            result = pd.DataFrame(columns=self.config.statements_columns)

        return result

    def process_batch_old(self, sub_batch, progress):
        """
        Run the entire scraping process for the identified NSD entries, iterating over all financial data statements.
        """
        try:
            # Initialize the overall counter
            start_time = time.time()  # Record the start time for the entire process
            total_items = len(sub_batch)  # Total number of items across all sectors

            # Initialize a counter to track the total number of processed items
            processed_items = 0
            # Process data sector by sector, processing sectors with empty strings last
            all_data = []  # List to store all the processed data
            for i, (_, row) in enumerate(sub_batch.iterrows()):
                try:
                    # Process each company-quarter data using the refactored function

                    company_quarter_data = self._process_company_quarter_data(row)
                    all_data.extend(company_quarter_data)  # Add all processed DataFrames to all_data

                    # Print progress information
                    index_number = progress['batch_start'] + progress['sub_batch_start'] + i
                    index_number_b = (progress['batch_start'] * progress['batch_counter']) + (progress['sub_batch_start'] * progress['sub_batch_counter']) + i
                    extra_info = [f'{i+1}/{len(sub_batch)} in batch', progress['sub_batch_counter'], row['nsd'], row['company_name'], pd.to_datetime(row['quarter'], dayfirst=False, errors='coerce').strftime('%Y-%m-%d'), f"v{row['version']}"]
                    self.print_info(index_number, progress['scrape_size'], progress['start_time'], extra_info)

                except Exception as e:
                    # Log any errors encountered during processing of individual rows
                    self.log_error(f"Error processing row {i}: {e}")

            batch_df = pd.concat(all_data, ignore_index=True)
            batch_df = batch_df[self.config.statements_columns].sort_values(by=self.config.statements_order)

            return batch_df

        except Exception as e:
            # Log any errors encountered during the main scraping process
            self.log_error(f"Error in run_process_batch: {e}")
            return None  # Return None to indicate that the scraping process did not complete

    def _process_company_quarter_data(self, row):
        """
        Internal method to process financial and statements data for a specific company and quarter.

        Args:
            row (pd.Series): A row of data containing NSD, company name, quarter, sector, and other metadata.

        Returns:
            list: A list of DataFrames with the processed financial and statements data for the company.
        """
        try:
            company_quarter_data = []  # List to store data for the company in the current quarter

            # Extract data from the row
            nsd = row['nsd']
            company_name = row['company_name']
            quarter = pd.to_datetime(row['quarter'], dayfirst=False, errors='coerce').strftime('%Y-%m-%d')
            sector = row['sector']
            subsector = row['subsector']
            segment = row['segment']
            version = row['version']

            # Construct the URL for the NSD entry
            url = f"https://www.rad.cvm.gov.br/ENET/frmGerenciaPaginaFRE.aspx?NumeroSequencialDocumento={nsd}&CodigoTipoInstituicao=1"
            self.test_internet()
            self.driver.get(url)

            # Define all statements to be scraped
            statements = self.config.financial_data_statements + self.config.statements_data_statements

            for cmbGrupo, cmbQuadro in statements:
                # Determine which scraping method to use
                if [cmbGrupo, cmbQuadro] in self.config.financial_data_statements:
                    df = self._scrape_financial_data(cmbGrupo, cmbQuadro)
                else:
                    df = self._scrape_statements_data(cmbGrupo, cmbQuadro)

                if df is not None:
                    # Add necessary metadata columns to the DataFrame
                    df = df.assign(
                        nsd=nsd,
                        company_name=company_name,
                        quarter=quarter,
                        version=version,
                        segment=segment,
                        subsector=subsector,
                        sector=sector,
                        type=cmbGrupo,
                        frame=cmbQuadro
                    )
                    # Append the processed DataFrame to the list
                    company_quarter_data.append(df[self.config.statements_columns])

            return company_quarter_data

        except Exception as e:
            # Log any errors encountered during processing
            self.log_error(f"Error processing company quarter data: {e}")
            return []  # Return an empty list to prevent the process from stopping

    def _scrape_financial_data(self, cmbGrupo, cmbQuadro):
        """
        Scrapes statements data from the specified page.

        Parameters:
        - group_value: The group combo box value.
        - quadro_value: The frame combo box value.

        Returns:
        - DataFrame: A DataFrame containing the scraped data.
        """
        try:
            drop_items = '3.99'
            xpath_grupo = '//*[@id="cmbGrupo"]'
            xpath_quadro = '//*[@id="cmbQuadro"]'
            xpath_frame = '//*[@id="iFrameFormulariosFilho"]'

            # Select the correct options for cmbGrupo and cmbQuadro
            grupo = self.select(xpath_grupo, cmbGrupo, self.driver, self.driver_wait)
            quadro = self.select(xpath_quadro, cmbQuadro, self.driver, self.driver_wait)

            # selenium enter frame
            frame = self.wait_forever(self.driver_wait, xpath_frame)
            frame = self.driver.find_elements(By.XPATH, xpath_frame)
            self.driver.switch_to.frame(frame[0])

            # read and clean quadro
            xpath = '//*[@id="ctl00_cphPopUp_tbDados"]'
            thousand = self.wait_forever(self.driver_wait, xpath)

            xpath = '//*[@id="TituloTabelaSemBorda"]'
            thousand = self.driver_wait.until(EC.presence_of_element_located((By.XPATH, xpath))).text
            thousand = 1000 if "Mil" in thousand else 1

            html_content = self.driver.page_source
            df1 = pd.read_html(StringIO(html_content), header=0)[0]
            df2 = pd.read_html(StringIO(html_content), header=0, thousands='.')[0].fillna(0)

            df1 = df1.iloc[:,0:3]
            df2 = df2.iloc[:,0:3]
            df1.columns = self.config.financial_statements_columns
            df2.columns = self.config.financial_statements_columns
            df = pd.concat([df1.iloc[:, :2], df2.iloc[:, 2:3]], axis=1)

            col = df.iloc[:, 2].astype(str)
            col = col.str.replace('.', '', regex=False)
            col = col.str.replace(',', '.', regex=False)
            col = pd.to_numeric(col, errors='coerce')
            col = col * thousand
            df.iloc[:, 2] = col

            try:
                df = df[~df[self.config.financial_statements_columns[0]].str.startswith(drop_items)]
            except Exception as e:
                pass

            # selenium exit frame
            self.driver.switch_to.parent_frame()

            return df
        
        except Exception as e:
            # self.log_error(e)
            return None

    def _scrape_statements_data(self, cmbGrupo, cmbQuadro):
        """
        Process the scraped statements data into a DataFrame.

        Parameters:
        - cmbGrupo: The group combo box value.
        - cmbQuadro: The frame combo box value.

        Returns:
        DataFrame: A Pandas DataFrame containing the processed data.
        """
        try:
            # Define the XPaths
            xpath_grupo = '//*[@id="cmbGrupo"]' 
            xpath_quadro = '//*[@id="cmbQuadro"]'
            xpath_frame = '//*[@id="iFrameFormulariosFilho"]'
            xpath_thousand = '//*[@id="UltimaTabela"]/table/tbody[1]/tr[1]/td[1]/b'
            thousand_word = 'Mil'
            
            # XPaths for the different data points
            acoes_on_xpath = '//*[@id="QtdAordCapiItgz_1"]'
            acoes_pn_xpath = '//*[@id="QtdAprfCapiItgz_1"]'
            acoes_on_tesouraria_xpath = '//*[@id="QtdAordTeso_1"]'
            acoes_pn_tesouraria_xpath = '//*[@id="QtdAprfTeso_1"]'

            # Select the correct options for cmbGrupo and cmbQuadro
            grupo = self.select(xpath_grupo, cmbGrupo, self.driver, self.driver_wait)
            quadro = self.select(xpath_quadro, cmbQuadro, self.driver, self.driver_wait)

            # Selenium enter frame
            frame = self.wait_forever(self.driver_wait, xpath_frame)
            self.driver.switch_to.frame(frame)

            # Check if the values are in thousands
            thousand_text = self.wait_forever(self.driver_wait, xpath_thousand).text
            thousand = 1000 if thousand_word in thousand_text else 1

            # Extract the required values
            data = {
                self.config.financial_statements_columns[0]: [],  # 'account'
                self.config.financial_statements_columns[1]: [],  # 'description'
                self.config.financial_statements_columns[2]: []   # 'value'
            }

            # Extract values using the XPaths
            acoes_on = self.driver.find_element(By.XPATH, acoes_on_xpath).text.strip().replace('.', '').replace(',', '.')
            acoes_pn = self.driver.find_element(By.XPATH, acoes_pn_xpath).text.strip().replace('.', '').replace(',', '.')
            acoes_on_tesouraria = self.driver.find_element(By.XPATH, acoes_on_tesouraria_xpath).text.strip().replace('.', '').replace(',', '.')
            acoes_pn_tesouraria = self.driver.find_element(By.XPATH, acoes_pn_tesouraria_xpath).text.strip().replace('.', '').replace(',', '.')

            # Populate the data dictionary using settings values
            data[self.config.financial_statements_columns[0]] = [
                self.config.accounts['acoes_on'], 
                self.config.accounts['acoes_pn'], 
                self.config.accounts['acoes_on_tesouraria'], 
                self.config.accounts['acoes_pn_tesouraria']
            ]
            data[self.config.financial_statements_columns[1]] = [
                self.config.descriptions['acoes_on'], 
                self.config.descriptions['acoes_pn'], 
                self.config.descriptions['acoes_on_tesouraria'], 
                self.config.descriptions['acoes_pn_tesouraria']
            ]
            data[self.config.financial_statements_columns[2]] = [
                float(acoes_on) * thousand, 
                float(acoes_pn) * thousand, 
                float(acoes_on_tesouraria) * thousand, 
                float(acoes_pn_tesouraria) * thousand
            ]

            df = pd.DataFrame(data)

            # Selenium exit frame
            self.driver.switch_to.parent_frame()
            
            return df
            
        except Exception as e:
            # self.log_error(f"Error processing statements data: {e}")
            return None

    def main_thread_old(self, batch, progress):
        """
        Process the batches of statements data using concurrent workers for sub-batches.

        Parameters:
        - progress (dict): Progress details including current batch, size, and total length.
        - batch (DataFrame): The current batch to be processed.
        - batch_number (int): Batch number for logging or debugging.

        Returns:
        DataFrame: Combined results of processed sub-batches.
        """
        try:
            all_results = []  # To collect results from all futures
            sub_batch_counter = 0  # Initialize thread counter

            with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
                sub_batch_size = max(1, len(batch) // self.config.max_workers)
                futures = []

                for sub_batch_start in range(0, len(batch), sub_batch_size):
                    sub_batch_progress = progress.copy()
                    sub_batch_progress['sub_batch_counter'] = sub_batch_counter
                    sub_batch_progress['sub_batch_start'] = sub_batch_start

                    # Define the sub-batch
                    sub_batch = batch.iloc[sub_batch_start:sub_batch_start + sub_batch_size]
    
                    sub_batch_counter += 1  # Increment thread counter

                    future = executor.submit(self.process_instance, sub_batch, sub_batch_progress)
                    futures.append(future)
                    time.sleep(1)

                # Collect results as sub-batches complete
                for future in as_completed(futures):
                    result = future.result()  # Will raise exceptions if any occurred during processing
                    if not result.empty:
                        all_results.append(result)

            # Combine all results into a single DataFrame
            if all_results:
                processed_batch = pd.concat(all_results, ignore_index=True)
            else:
                processed_batch = pd.DataFrame(columns=self.config.statements_columns)

        except Exception as e:
            self.log_error(f"Error during threaded batch processing: {e}")
            processed_batch = pd.DataFrame(columns=self.config.statements_columns)

        return processed_batch

    def main_sequential_old(self, batch, progress):
        """
        Sequentially process all scrape targets at once.
        
        Parameters:
        - batch (DataFrame): DataFrame containing targets to scrape.
        """
        progress['sub_batch_counter'] = 0 # not incremental, sequential thread
        progress['sub_batch_start'] = 0
        try:
            # Process all scrape targets at once without batching
            processed_batch = self.process_instance(batch, progress)

        except Exception as e:
            self.log_error(f"Error during sequential processing: {e}")

        return processed_batch

    def main(self, thread=True):
        """
        Main method to process data.
        """
        # # Initialize the WebDriver
        # self.driver, self.driver_wait = self._initialize_driver()

        try:
            # Load necessary data
            company_info = self.load_data(table_name=self.config.company_table, db_filepath=self.config.metadados_filepath)
            existing_nsd = self.load_data(table_name=self.config.nsd_table, db_filepath=self.config.metadados_filepath)
            financial_statements = self.load_data(table_name=self.config.statements_file, db_filepath=self.config.initial_filepath)

            # Identify scrape targets
            scrape_targets = self.get_scrape_targets(company_info, existing_nsd, financial_statements)

            # Exit if no scrape_targets
            if scrape_targets.empty:
                self.db_optimize(self.config.initial_filepath)
                return True

            # Process targets using threading or sequential logic
            processed_data = self.run(scrape_targets, thread=thread)

            # Save processed data
            if not processed_data.empty:
                self.save_to_db(dataframe=processed_data, table_name=self.config.statements_file, db_filepath=self.config.initial_filepath)

        except Exception as e:
            self.log_error(f"Error in main: {e}")

        return True

    def main_old(self, thread=True):
        """
        The main method to scrape NSD data, parse it, and save it to the database.
        """
        self.close_driver()
        try:
            company_info = self.load_data(table_name=self.config.company_table, db_filepath=self.config.metadados_filepath)
            existing_nsd = self.load_data(table_name=self.config.nsd_table, db_filepath=self.config.metadados_filepath)
            financial_statements = self.load_data(table_name=self.config.statements_file, db_filepath=self.config.initial_filepath)

            scrape_targets = self.get_scrape_targets(company_info, existing_nsd, financial_statements)

            progress = {}
            progress['scrape_size'] = len(scrape_targets)
            progress['batch_size'] = self.config.batch_size

            start_time = time.time()
            progress['start_time'] = start_time

            for batch_counter, batch_start in enumerate(range(0, progress['scrape_size'], progress['batch_size'])):
                progress['batch_counter'] = batch_counter
                progress['batch_start'] = batch_start

                # Slice the DataFrame for the current batch
                batch = scrape_targets.iloc[batch_start:batch_start + progress['batch_size']]

                if thread:
                    # Run with threading
                    processed_batch = self.main_thread(batch, progress)
                else:
                    # Run sequentially
                    processed_batch = self.main_sequential(batch, progress)

                if not processed_batch.empty:
                    self.save_to_db(dataframe=processed_batch, table_name=self.config.statements_file, db_filepath=self.config.initial_filepath)

        except Exception as e:
            self.log_error(e)

        self.close_driver()

        return True
