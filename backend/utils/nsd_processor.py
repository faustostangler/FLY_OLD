from threading import Lock
import pandas as pd
import datetime
import time
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed

from utils.base_processor import BaseProcessor

class NsdProcessor(BaseProcessor):
    '''
    Processar dados de empresas
    '''
    def __init__(self):
        super().__init__()
        self.db_lock = Lock()  # Initialize a threading Lock

    def _generate_nsd_list(self, existing_nsd):
        """
        """
        try:
            last_nsd = existing_nsd['nsd'].max()
            if pd.isna(last_nsd):  # Check if last_nsd is NaN
                last_nsd = 0

            existing_nsd = existing_nsd[existing_nsd['company_name'].notnull() & (existing_nsd['company_name'].str.strip() != '')]

            now = datetime.datetime.now()
            max_date = existing_nsd['sent_date'].max() if not existing_nsd['sent_date'].isna().all() else now
            min_date = existing_nsd['sent_date'].min() if not existing_nsd['sent_date'].isna().all() else datetime.datetime(2010, 1, 1)
            total_nsds = existing_nsd['nsd'].count() if existing_nsd['nsd'].count() > 0 else 1
            
            if max_date != now:
                days_span = (max_date - min_date).days
                days_elapsed = (datetime.datetime.now() - max_date).days + 1 if max_date else 1
                daily_submission_estimate =  total_nsds / days_span if days_span > 0 else 1
                estimated_new_nsds = int(daily_submission_estimate * days_elapsed * self.config.safety_factor) + 1
                nsd_range = list(range(last_nsd + 1, 1 + last_nsd + estimated_new_nsds))

            else:
                nsd_range = list(range(last_nsd + 1, 1 + last_nsd + self.config.batch_size))
        except Exception as e:
            self.log_error(e)
        

        scrape_targets = pd.DataFrame({'nsd': list(nsd_range)})

        return scrape_targets

    def parse_nsd_data_old(self, html, nsd):
        """
        Parse the HTML content to extract NSD data.

        Parameters:
        html (str): The HTML content of the NSD page.
        nsd (int): The NSD value being parsed.

        Returns:
        dict: A dictionary of the parsed NSD data.
        """
        try:
            # Hard-coded XPaths or CSS selectors
            company_name_selector = '#lblNomeCompanhia'
            dri_selector = '#lblNomeDRI'
            nsd_type_version_selector = '#lblDescricaoCategoria'
            auditor_selector = '#lblAuditor'
            responsible_auditor_selector = '#lblResponsavelTecnico'
            protocolo_selector = '#lblProtocolo'
            quarter_selector = '#lblDataDocumento'
            sent_date_selector = '#lblDataEnvio'
            reason_selector = '#lblMotivoCancelamentoReapresentacao'

            # Parse the HTML content
            soup = BeautifulSoup(html, 'html.parser')
            data = {'nsd': nsd}

            # Extracting data using the defined selectors
            data['company_name'] = self.clean_text(soup.select_one(company_name_selector).text)
            data['dri'] = self.clean_text(soup.select_one(dri_selector).text.split('-')[0].strip())
            nsd_type_version = soup.select_one(nsd_type_version_selector).text
            data['nsd_type'] = self.clean_text(nsd_type_version.split('-')[0].strip())
            data['version'] = int(nsd_type_version.split('V')[-1])
            data['auditor'] = self.clean_text(soup.select_one(auditor_selector).text.split('-')[0].strip())
            data['responsible_auditor'] = self.clean_text(soup.select_one(responsible_auditor_selector).text)
            data['protocol'] = soup.select_one(protocolo_selector).text.replace('-', '').strip()

            # Handle quarter parsing
            raw_date = soup.select_one(quarter_selector).text
            try:
                if len(raw_date) == 4:  # Only a year is provided
                    # Assuming the last day of the year
                    data['quarter'] = datetime.datetime.strptime(f"31/12/{raw_date}", "%d/%m/%Y")
                else:
                    data['quarter'] = datetime.datetime.strptime(raw_date, "%d/%m/%Y")
            except ValueError as e:
                # self.log_error(f"Error parsing date for NSD {nsd}: {e}")
                return None

            # Parse the sent date
            try:
                data['sent_date'] = datetime.datetime.strptime(soup.select_one(sent_date_selector).text, "%d/%m/%Y %H:%M:%S")
            except ValueError as e:
                self.log_error(f"Error parsing sent date for NSD {nsd}: {e}")
                data['sent_date'] = None  # Set to None if parsing fails

            data['reason'] = self.clean_text(soup.select_one(reason_selector).text)

            return data if data['sent_date'] else None

        except Exception as e:
            # self.log_error(f"Error parsing NSD {nsd}: {e}")
            return None

    def get_nsd_data_old(self, nsd_data, nsd):
        '''
        '''
        data = ''
        try:
            url = f"https://www.rad.cvm.gov.br/ENET/frmGerenciaPaginaFRE.aspx?NumeroSequencialDocumento={nsd}&CodigoTipoInstituicao=1"
            headers = self.header_random()  # Use the random headers from the system module
            self.test_internet()
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            html = response.text

            if html:
                data = self.parse_nsd_data(html, nsd)

                if data:
                    nsd_data.append(data)
                else:
                    nsd_data.append({'nsd': nsd})

        except Exception as e:
            self.log_error(e)

        return nsd_data

    def process_batch(self, sub_batch, progress):
        """
        Process a batch of NSD data by scraping and extracting relevant information.
        """
        result = pd.DataFrame(columns=self.config.nsd_columns)
        processed_data = []
        start_time = time.time()

        for i, (_, row) in enumerate(sub_batch.iterrows()):
            try:
                nsd = row['nsd']
                # Fetch and process NSD details
                nsd_data = self._fetch_nsd_html(nsd)
                if nsd_data:
                    processed_data.append(nsd_data)

                # Log progress
                extra_info = [
                    nsd,
                    nsd_data.get('sent_date').strftime('%Y-%m-%d %H:%M:%S') if nsd_data.get('sent_date') else '',
                    nsd_data.get('nsd_type', ''),
                    nsd_data.get('company_name', ''),
                    nsd_data.get('quarter').strftime('%Y-%m') if nsd_data.get('quarter') else '',
                ]
                self.print_info(progress['batch_start'] + i, progress['scrape_size'], start_time, extra_info)

            except Exception as e:
                self.log_error(f"Error processing NSD {row['nsd']}: {e}")

        # Combine results into a DataFrame
        if processed_data:
            result = pd.DataFrame(processed_data, columns=self.config.nsd_columns)
        
        return result

    def _fetch_nsd_html(self, nsd):
        """
        Fetch and parse NSD data for a given NSD value.
        """
        result = {'nsd': nsd}  # Minimal data to prevent stopping the process
        try:
            url = f"https://www.rad.cvm.gov.br/ENET/frmGerenciaPaginaFRE.aspx?NumeroSequencialDocumento={nsd}&CodigoTipoInstituicao=1"
            headers = self.header_random()
            self.test_internet()

            response = requests.get(url, headers=headers)
            response.raise_for_status()

            # Parse the response HTML
            html = response.text
            result = self._parse_nsd_data(response.text, nsd)

        except Exception as e:
            self.log_error(f"Error fetching NSD {nsd}: {e}")

        return result

    def _parse_nsd_data(self, html, nsd):
        """
        Parse the HTML content to extract NSD data.
        """
        result = {}
        try:
            soup = BeautifulSoup(html, 'html.parser')
            data = {'nsd': nsd}

            # Define selectors for the required data
            selectors = {
                'company_name': '#lblNomeCompanhia',
                'dri': '#lblNomeDRI',
                'nsd_type_version': '#lblDescricaoCategoria',
                'auditor': '#lblAuditor',
                'responsible_auditor': '#lblResponsavelTecnico',
                'protocol': '#lblProtocolo',
                'quarter': '#lblDataDocumento',
                'sent_date': '#lblDataEnvio',
                'reason': '#lblMotivoCancelamentoReapresentacao',
            }

            for key, selector in selectors.items():
                element = soup.select_one(selector)
                if element:
                    data[key] = self.clean_text(element.text) if key != 'sent_date' else element.text

            # Parse data information nsd_type, version, quarter and sent_date into datetime objects
            parts = data['nsd_type_version'].split()

            # Extract version, year, and nsd_type
            data['version'] = parts[-1]  # Last part
            year = parts[-2]     # Second last part
            data['nsd_type'] = " ".join(parts[:-2])  # Remaining parts joined

            if len(data['quarter']) == 4:  # Only a year is provided
                # Assuming the last day of the year
                data['quarter'] = datetime.datetime.strptime(f"31/12/{data['quarter']}", "%d/%m/%Y")
            else:
                data['quarter'] = datetime.datetime.strptime(data['quarter'], "%d/%m/%Y")
            data['quarter'] = pd.to_datetime(data.get('quarter', None), format="%d/%m/%Y", errors='coerce')
            data['sent_date'] = pd.to_datetime(data.get('sent_date', None), format="%d/%m/%Y %H:%M:%S", errors='coerce')

            if data['sent_date']:
                result = data

        except Exception as e:
            # self.log_error(f"Error parsing NSD {nsd}: {e}")
            pass

        return result 

    def process_instance(self, sub_batch, progress):
        """
        Process a single batch by delegating to process_batch.
        """
        try:
            return self.process_batch(sub_batch, progress)
        except Exception as e:
            self.log_error(f"Error in process_instance: {e}")
            return pd.DataFrame()  # Return an empty DataFrame on failure

    def process_batch_old(self, batch, batch_start, length):
        '''
        '''
        processed_batch = []
        try:
            for i, (_, row) in enumerate(batch.iterrows()):
                nsd = row['nsd']  # Access the NSD value from the DataFrame

                processed_batch = self.get_nsd_data(processed_batch, nsd)

                data = processed_batch[-1]
                try:
                # Prepare extra information for progress reporting
                    extra_info = [nsd, data['sent_date'], data['quarter'].strftime("%Y-%m-%d"), data['nsd_type'], data['company_name']]
                except Exception as e:
                    extra_info = [nsd]
                self.print_info(i+batch_start, length, self.start_time, extra_info, indent_level=1)
        except Exception as e:
            self.log_error(e)

        processed_batch_df = pd.DataFrame(processed_batch, columns=self.config.nsd_columns)
        processed_batch_df = processed_batch_df[self.config.nsd_columns]

        return processed_batch_df

    def main_thread_old(self, batch, batch_start, length):
        """
        Multithreaded processing of NSD sub-batches within a given batch.
        """
        processed_batch = []

        # Determine the size of each sub-batch
        sub_batch_size = max(1, len(batch) // self.config.max_workers)

        # Split the batch into sub-batches
        sub_batches = [batch.iloc[i:i + sub_batch_size] for i in range(0, len(batch), sub_batch_size)]

        def process_sub_batch(sub_batch, sub_batch_start):
            """Helper method to process a single sub-batch in a thread."""
            thread_name = f"Thread-{sub_batch_start // self.config.batch_size + 1}-{sub_batch_start % self.config.batch_size}"
            result = self.process_batch(sub_batch, sub_batch_start, length)
            return result

        # Use ThreadPoolExecutor to process sub-batches concurrently
        with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
            futures = [
                executor.submit(process_sub_batch, sub_batch, batch_start + i * sub_batch_size)
                for i, sub_batch in enumerate(sub_batches)
            ]

            # Collect results from all threads
            for future in as_completed(futures):
                try:
                    processed_batch.append(future.result())
                except Exception as e:
                    self.log_error(f"Error in threaded sub-batch processing: {e}")

        # Combine all results into one DataFrame
        if processed_batch:
            for i, df in enumerate(processed_batch):
                processed_batch[i] = df.fillna('')  # Replace NaN values with an empty string
            result = pd.concat(processed_batch, ignore_index=True)
        else:
            result = pd.DataFrame(columns=self.config.nsd_columns)

        return result

    def main_sequential_old(self, batch, batch_start, length):
        """
        Sequential processing of NSD batches.
        """
        processed_batch = self.process_batch(batch, batch_start, length)

        return processed_batch

    def main(self, thread=True):
        """
        Main method to scrape NSD data, parse it, and save it to the database.
        """
        try:
            # # Initialize the WebDriver
            # self.driver, self.driver_wait = self._initialize_driver()

            # Load existing NSD data
            existing_nsd = self.load_data(table_name=self.config.nsd_table, db_filepath=self.config.metadados_filepath)

            # Filter by the last sent_date
            existing_nsd['sent_date'] = pd.to_datetime(existing_nsd['sent_date'], format="%Y-%m-%dT%H:%M:%S", errors='coerce')
            last_valid_index = existing_nsd.sort_values(by='sent_date', ascending=False).index[0]
            existing_nsd = existing_nsd.loc[:last_valid_index]

            scrape_targets = self._generate_nsd_list(existing_nsd)

            # Exit if no scrape_targets
            if scrape_targets.empty:
                self.db_optimize(self.config.metadados_filepath)
                return True

            thread = False
            # Run processing (threaded or sequential)
            processed_data = self.run(scrape_targets, thread=thread)

            # Save processed data
            if not processed_data.empty:
                self.save_to_db(dataframe=processed_data, table_name=self.config.nsd_table, db_filepath=self.config.metadados_filepath)

        except Exception as e:
            self.log_error(f"Error in main: {e}")

    def main_old(self, limit=2, thread=True):
        """
        The main method to scrape NSD data, parse it, and save it to the database.
        """
        # # Initialize the WebDriver
        # self.driver, self.driver_wait = self._initialize_driver()

        counter = 0

        db_filepath = self.config.metadados_filepath
        try:
            # Step 1: Load existing NSD data from the database
            existing_nsd = self.load_data(table_name=self.config.nsd_table, db_filepath=db_filepath)
            existing_nsd['sent_date'] = pd.to_datetime(existing_nsd['sent_date'], format="%Y-%m-%dT%H:%M:%S", errors='coerce')

            # Step 2: Generate NSD list and setup processing variables
            scrape_targets = self._generate_nsd_list(existing_nsd)
            length = len(scrape_targets)
            batch_size = self.config.batch_size
            total_batches = (length + batch_size - 1) // batch_size
            self.start_time = time.time()

            for c, batch_start in enumerate(range(0, length, batch_size)):
                # Slice the DataFrame for the current batch
                batch = scrape_targets.iloc[batch_start:batch_start + batch_size]

                # Process batches using thread or sequential method
                if thread:
                    processed_batch = self.main_thread(batch, batch_start, length)
                else:
                    processed_batch = self.main_sequential(batch, batch_start, length)
                        
                if not processed_batch.empty:
                    self.save_to_db(dataframe=processed_batch, table_name=self.config.nsd_table, db_filepath=self.config.metadados_filepath)
                else:
                    limit_counter += 1
                    if limit_counter >= limit:
                        return True


        except Exception as e:
            # Loga o erro usando log_error
            self.log_error(e)

        return True
    
