from threading import Lock
import pandas as pd
import datetime
import time
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
import inspect

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
                estimated_new_nsds = int(daily_submission_estimate * days_elapsed * self.config.domain['safety_factor']) + 1
                nsd_range = list(range(last_nsd + 1, 1 + last_nsd + estimated_new_nsds))

            else:
                nsd_range = list(range(last_nsd + 1, 1 + last_nsd + self.config.scraping['batch_size']))
        except Exception as e:
            self.log_error(e)
        

        scrape_targets = pd.DataFrame({'nsd': list(nsd_range)})

        return scrape_targets

    def process_batch(self, sub_batch, progress):
        """
        Process a batch of NSD data by scraping and extracting relevant information.
        """
        result = pd.DataFrame(columns=self.config.domain['columns_nsd'])
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
                    f"Worker {progress['thread_id']} Item {i+1}/{len(sub_batch)}", 
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
            result = pd.DataFrame(processed_data, columns=self.config.domain['columns_nsd'])
        
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

    def main(self, thread=True):
        """
        Main method to scrape NSD data, parse it, and save it to the database.
        """
        try:
            # # Initialize the WebDriver
            # self.driver, self.driver_wait = self._initialize_driver()

            # Load existing NSD data
            existing_nsd = self.load_data(table_name=self.config.databases['raw']['tables']['nsd'], db_filepath=self.config.databases['raw']['filepath'])

            try:
                # Filter by the last sent_date
                existing_nsd['sent_date'] = pd.to_datetime(existing_nsd['sent_date'], format="%Y-%m-%dT%H:%M:%S", errors='coerce')
                last_valid_index = existing_nsd.sort_values(by='sent_date', ascending=False).index[0]
                existing_nsd = existing_nsd.loc[:last_valid_index]
            except Exception as e:
                self.log_error(e)

            scrape_targets = self._generate_nsd_list(existing_nsd)

            # Exit if no scrape_targets
            if scrape_targets.empty:
                self.db_optimize(self.config.databases['raw']['filepath'])
                return True

            # Run processing (threaded or sequential)
            processed_data = self.run(scrape_targets, thread=thread, module_name=self.inspect.getmodule(self.inspect.currentframe()).__name__)

            # Save processed data
            if not processed_data.empty:
                self.save_to_db(dataframe=processed_data, table_name=self.config.databases['raw']['tables']['nsd'], db_filepath=self.config.databases['raw']['filepath'])

        except Exception as e:
            self.log_error(f"Error in main: {e}")

        return True