import json
import re
import time
from threading import Lock

import cloudscraper
import pandas as pd
import requests
from bs4 import BeautifulSoup
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys

from utils.base_processor import BaseProcessor


class CompanyProcessor(BaseProcessor):
    """Processar dados de empresas."""

    def __init__(self):
        super().__init__()
        self.db_lock = Lock()  # Initialize a threading Lock

        # Define at the beginning of your script
        self.total_block_time = 0

        self.homepage_url = "https://sistemaswebb3-listados.b3.com.br/listedCompaniesPage/?language=pt-br"

        # Initialize database and table names
        self.tbl_company_name = self.config.databases["raw"]["table"]["company_info"]
        self.db_filepath = self.config.databases["raw"]["filepath"]

        # # Initialize driver and other resources
        # self.driver, self.driver_wait = self._initialize_driver()

    def process_instance(self, sub_batch, payload, progress):
        """Process a single batch by delegating to process_batch."""
        result = pd.DataFrame()

        try:
            print(
                f"Starting batch {progress['batch_index']}/{progress['total_batches']} {100 * progress['batch_index'] / progress['total_batches']:.02f}%"
            )

            batch_processor = CompanyProcessor()

            # Delegate to process_batch for the actual batch processing
            result, benchmark_results = batch_processor.benchmark_function(
                batch_processor.process_batch, sub_batch, payload, progress, benchmark_mode=False
            )

            # Save result to database
            self.save_to_db(
                dataframe=result, table_name=self.tbl_company_name, db_filepath=self.db_filepath, alert=False
            )

        except Exception as e:
            self.log_error(f"Error in process_instance: {e}")
            self.close_driver()  # Ensure driver is closed even on errors

        return result

    def process_batch(self, sub_batch, payload, progress):
        """
        Process a batch of company tickers by fetching their detailed information.

        Args:
            sub_batch (pd.DataFrame): Subset of tickers to process.
            payload (any): Additional data passed to the batch (not used here).
            progress (dict): Progress tracking information (batch index, total size, etc).

        Returns:
            pd.DataFrame: A concatenated DataFrame with the detailed company information.
                        If no data is found or an error occurs, returns an empty DataFrame.
        """
        result = ''

        try:
            # Initialize an empty list to accumulate the results
            all_data = []

            # Start the timer to measure processing time
            start_time = time.time()

            # create first scraper
            scraper = self._init_scraper()

            # Loop through each row in the sub-batch
            for i, (_, row) in enumerate(sub_batch.iterrows()):
                # Extract ticker symbol from the row
                ticker = row['ticker']

                # Fetch detailed company data for the ticker
                df = self._get_company_data(ticker, scraper)

                # Check if the returned DataFrame is not empty
                if not df.empty:
                    # Append the company data to the list
                    all_data.append(df)

                    # Extract CVM code for logging
                    cvm_code = df['cvm_code'][0]
                    company_name = df['company_name'][0]

                    # Log current progress
                    extra_info = [cvm_code, ticker, company_name]
                    self.print_info(i, len(sub_batch), start_time, extra_info, indent_level=0)

            # After processing all rows, concatenate the collected DataFrames
            if all_data:
                result = pd.concat(all_data, ignore_index=True)

        except Exception as e:
            # In case of any error, initialize an empty DataFrame and log the error
            result = pd.DataFrame()
            self.log_error(e)

        return result

    def _init_scraper(self):
        """Create a cloudscraper instance with randomized headers and prime it on the homepage."""
        try:
            headers = self.header_random()
            
            # backup 
            scraper = requests.Session()
            scraper.headers.update(headers)

            # real one
            scraper = cloudscraper.create_scraper()
            scraper.headers.update(headers)
            scraper.get(self.homepage_url)
        except Exception as e:
            self.log_error(e)

        return scraper


    def _fetch_with_retry(self, scraper, url, wait=1):
        """
        Keep calling scraper.get(url) until we get status_code 200,
        tracking block times and using dynamic_sleep() between tries.
        Returns the successful Response.
        """
        block_start = None
        while True:
            try:
                r = scraper.get(url)
                r.raise_for_status()
                if block_start:
                    self.total_block_time += time.time() - block_start
                return r
            except Exception as e:
                if block_start is None:
                    block_start = time.time()
                time.sleep(self.dynamic_sleep())
                wait += 1
                scraper = self._init_scraper()

    # def _get_initial_company(self, ticker):
    #     '''
    #     '''
    #     try:
    #         token = self.base64_payload({...})
    #         resp = self._fetch_with_retry(self._init_scraper(), self.INITIAL_URL + token)
    #         data = resp.json().get("results", [])
    #         item = next((i for i in data if i["issuingCompany"] == ticker), None)
    #     except Exception as e:
    #         self.log_error(e)
    #     return pd.DataFrame([item]) if item else pd.DataFrame()

    # def _get_company_details(self, cvm_code):
    #     '''
    #     '''
    #     try:
    #         token = self.base64_payload({"codeCVM": cvm_code, "language": "pt-br"})
    #         resp = self._fetch_with_retry(self._init_scraper(), self.DETAIL_URL + token)
    #         df = pd.DataFrame([resp.json()])
    #         if not df.empty:
    #             df["ticker_codes"], df["isin_codes"] = zip(*df["otherCodes"].apply(self._extract_codes))
    #             self._split_industry_classification(df)
    #     except Exception as e:
    #         self.log_error(e)
    #     return df

    # def _get_shareholders(self, token2) -> pd.DataFrame:
    #     '''
    #     '''
    #     try:
    #         resp = self._fetch_with_retry(self._init_scraper(), self.SHAREHOLDER_URL + token2)
    #         jd = resp.json().get("positionShareholders", {}).get("results", [])
    #         df = pd.DataFrame(jd)
    #     except Exception as e:
    #         self.log_error(e)
    #     return df.iloc[:-1, :-1] if not df.empty else pd.DataFrame()

    def _extract_codes(self, otherCodes):
        '''
        '''
        try:
            codes = otherCodes or []
            tickers = [o["code"] for o in codes if isinstance(o, dict)]
            isins   = [o["isin"] for o in codes if isinstance(o, dict)]
        except Exception as e:
            self.log_error(e)
        return json.dumps(tickers), json.dumps(isins)

    def _split_industry_classification(self, df):
        '''
        '''
        try:
            parts = df["industryClassification"].str.split("/", expand=True)
            df["sector"]    = parts.get(0).str.strip()
            df["subsector"] = parts.get(1, df["sector"]).str.strip()
            df["segment"]   = parts.get(2, df["sector"]).str.strip()
        except Exception as e:
            self.log_error(e)
        return df

    def _get_company_data(self, ticker, scraper=None):
        """
        Fetch company info, details and shareholders from B3 for the given ticker,
        retrying automatically on blocks. Returns a single‐row DataFrame with
        mapped and cleaned columns, or the ticker string on failure.
        """
        if scraper is None:
            scraper = self._init_scraper()

        try:
            # Prepare payloads, tokens and endpoints
            payload1 = {"language": "pt-br", "pageNumber": 1, "pageSize": 20, "company": ticker}
            token1   = self.base64_payload(payload1)
            endpoint1 = "https://sistemaswebb3-listados.b3.com.br/listedCompaniesProxy/CompanyCall/GetInitialCompanies/"
            endpoint2 = "https://sistemaswebb3-listados.b3.com.br/listedCompaniesProxy/CompanyCall/GetDetail/"
            endpoint3 = "https://sistemaswebb3-listados.b3.com.br/listedCompaniesProxy/CompanyCall/GetListedFinancial/"

            # Fire off the three requests, with automatic retry on blocks
            r1 = self._fetch_with_retry(scraper, endpoint1 + token1)

            # parse r1 first so we can build token2
            results = r1.json().get("results", [])
            row     = next((i for i in results if i.get("issuingCompany") == ticker), None)
            if not row:
                raise
            company = pd.DataFrame([row])
            cvm_code = row["codeCVM"]

            # now details & financial endpoints share the same token
            payload2 = {"codeCVM": str(cvm_code), "language": "pt-br"}
            token2   = self.base64_payload(payload2)

            # Build the details DataFrame
            r2 = self._fetch_with_retry(scraper, endpoint2 + token2)
            details_json = r2.json()
            company_details = pd.DataFrame([details_json])
            if not company_details.empty:
                company_details["ticker_codes"], company_details["isin_codes"] = zip(
                    *company_details["otherCodes"].apply(self._extract_codes)
                )
                company_details = self._split_industry_classification(company_details)

                # Define the real key
                merge_key = ["issuingCompany"]

                # Merge based only on issuingCompany
                df = pd.merge(
                    company,
                    company_details,
                    on=merge_key,
                    how="outer",
                    suffixes=("", "_details")
                )

                # Find overlapping columns
                overwrite_columns = [col for col in company_details.columns if col in company.columns and col not in merge_key]

                # Overwrite old columns with _details values
                for col in overwrite_columns:
                    detail_col = f"{col}_details"
                    if detail_col in df.columns:
                        df[col] = df[detail_col].combine_first(df[col])
                        df.drop(columns=[detail_col], inplace=True)
            else:
                df = company

            # Build the shareholders DataFrame
            r3 = self._fetch_with_retry(scraper, endpoint3 + token2)
            sh_data = (r3.json().get("positionShareholders") or {}).get("results", [])
            shareholders = pd.DataFrame(sh_data)
            if not shareholders.empty:
                # drop the summary row and extra columns
                shareholders = shareholders.iloc[:-1, :-1]
                shareholders["cvm_code"] = cvm_code
                shareholders['ticker'] = ticker

                # Attach shareholders JSON
                shareholders['describle'] = shareholders['describle'].map(self.clean_text)

                df["shareholders"] = [
                    shareholders.to_json(orient="records", force_ascii=False)
                ]

            # Map to your final column schema and clean text fields
            mapping = self.config.domain["web_company_columns_mapping"]
            df = self.map_dataframe_columns(df, mapping)

            for col in (
                "ticker",
                "company_name",
                "trading_name",
                "sector",
                "subsector",
                "segment",
                "registrar",
                "main_registrar",
            ):
                if col in df.columns:
                    df[col] = df[col].map(self.clean_text)

            return df

        except Exception as e:
            self.log_error(e)
            return ticker

    def _get_company_data_old_with_request(self, ticker):
        '''
        '''
        wait_time = self.config.selenium['wait_time']
        block_start = None

        scraper = cloudscraper.create_scraper()
        headers = self.header_random()
        scraper.headers.update(headers)
        scraper.get(self.homepage_url)

        try:
            # get company cvm_code - first request
            payload_1 = {
                "language": "pt-br",
                "pageNumber": 1,
                "pageSize": 20,
                "company": ticker,
            }
            token_1 = self.base64_payload(payload_1)
            endpoint_1 = f"https://sistemaswebb3-listados.b3.com.br/listedCompaniesProxy/CompanyCall/GetInitialCompanies/"

            block_start_1 = None
            while True:
                try:
                    response_1 = scraper.get(endpoint_1+token_1)
                    if response_1.status_code == 200:
                        if block_start_1:
                            duration = time.time() - block_start_1
                            self.total_block_time += duration

                            # print(f"[{ticker}] Desbloqueado (Request 1) - +{duration:.2f}s | Total: {total_block_time:.2f}s")
                            block_start_1 = None

                        response_data = response_1.json()
                        result_data = response_data.get("results", [])
                        result = next((item for item in result_data if item.get("issuingCompany") == ticker), None)
                        if not result:
                            raise
                        company = pd.DataFrame([result])
                        break

                    else:
                        raise

                except Exception as e:
                    if not block_start_1:
                        block_start_1 = time.time()
                    # print(f"[{ticker}] Bloqueado (Request 1) - Aguardando {wait_time}s | {e}")
                    time.sleep(self.dynamic_sleep())
                    wait_time += 1

                    scraper = cloudscraper.create_scraper()
                    headers = self.header_random()
                    scraper.headers.update(headers)
                    scraper.get(self.homepage_url)

            # get company details - second request
            cvm_code = result['codeCVM']
            payload_2 = {"codeCVM": f"{cvm_code}", "language": "pt-br"}
            token_2 = self.base64_payload(payload_2)
            endpoint_2 = f"https://sistemaswebb3-listados.b3.com.br/listedCompaniesProxy/CompanyCall/GetDetail/"

            block_start_2 = None
            while True:
                try:
                    response_2 = scraper.get(endpoint_2+token_2)
                    if response_2.status_code == 200:
                        if block_start_2:
                            duration = time.time() - block_start_2
                            self.total_block_time += duration
                            
                            # print(f"[{ticker}] Desbloqueado (Request 2) - +{duration:.2f}s | Total: {total_block_time:.2f}s")
                            block_start_2 = None

                        response_2_json = response_2.json()
                        company_details = pd.DataFrame([response_2_json])

                        # isin and tickers
                        def extract_codes(otherCodes):
                            if not isinstance(otherCodes, list):
                                otherCodes = [otherCodes] if pd.notna(otherCodes) else []
                                
                            tickers = [item.get("code") for item in otherCodes if isinstance(item, dict) and "code" in item]
                            isins = [item.get("isin") for item in otherCodes if isinstance(item, dict) and "isin" in item]
                            
                            return json.dumps(tickers), json.dumps(isins)
                        if not company_details.empty:
                            company_details["ticker_codes"], company_details["isin_codes"] = zip(*company_details["otherCodes"].apply(extract_codes))

                            # Garante que a coluna existe
                            if "industryClassification" in company_details.columns:
                                split_sectors = company_details["industryClassification"].str.split("/", expand=True)
                                company_details["sector"] = split_sectors[0].str.strip()
                                company_details["subsector"] = split_sectors[1].str.strip() if split_sectors.shape[1] > 1 else split_sectors[0].str.strip()
                                company_details["segment"] = split_sectors[2].str.strip() if split_sectors.shape[1] > 2 else split_sectors[0].str.strip()


                        break

                    else:
                        raise

                except Exception as e:
                    if not block_start_2:
                        block_start_2 = time.time()
                    # print(f"[{ticker}] Bloqueado (Request 2) - Aguardando {wait_time}s | {e}")
                    time.sleep(self.dynamic_sleep())
                    wait_time += 1

                    scraper = cloudscraper.create_scraper()
                    headers = self.header_random()
                    scraper.headers.update(headers)
                    scraper.get(self.homepage_url)

            # get company shareholders - third request
            endpoint_3 = f"https://sistemaswebb3-listados.b3.com.br/listedCompaniesProxy/CompanyCall/GetListedFinancial/"

            block_start_3 = None
            while True:
                try:
                    response_3 = scraper.get(endpoint_3+token_2)
                    if response_3.status_code == 200:
                        if block_start_3:
                            duration = time.time() - block_start_3
                            self.total_block_time += duration
                            # print(f"[{ticker}] Desbloqueado (Request 3) - +{duration:.2f}s | Total: {total_block_time:.2f}s")
                            block_start_3 = None

                        if response_3.text:
                            shareholder_data = response_3.json()
                            shareholder_result = (shareholder_data.get("positionShareholders") or {}).get("results", [])
                            shareholder = pd.DataFrame(shareholder_result)
                            shareholder = shareholder.iloc[:-1, :-1]
                            shareholder['cvm_code'] = cvm_code
                        break
                    else:
                        raise

                except Exception as e:
                    if not block_start_3:
                        block_start_3 = time.time()
                    # print(f"[{ticker}] Bloqueado (Request 3) - Aguardando {wait_time}s | {e}")
                    time.sleep(self.dynamic_sleep())
                    wait_time += 1

                    scraper = cloudscraper.create_scraper()
                    headers = self.header_random()
                    scraper.headers.update(headers)
                    scraper.get(self.homepage_url)

            # Merge and Return
            if not company_details.empty:
                df = pd.merge(company, company_details, on="issuingCompany", how="outer", suffixes=("", "_details"))
            else:
                df = company
            
            # Convert the shareholder df to JSON (list of dicts)
            if "shareholder" in locals() and isinstance(shareholder, pd.DataFrame) and not shareholder.empty:
                shareholder_json = shareholder.to_json(orient="records", force_ascii=False)
                # Assign it as a single JSON string to the DataFrame cell
                df["shareholders"] = [shareholder_json]

            mapping = self.config.domain['web_company_columns_mapping']

            # Depois é só chamar:
            df = self.map_dataframe_columns(df, mapping)

            cols_to_clean = ["ticker", "company_name", "trading_name", "sector", "subsector", "segment", "registrar", "main_registrar"]
            for col in cols_to_clean:
                if col in df.columns:
                    df[col] = df[col].map(self.clean_text)

            return df

        except Exception as e:
            self.log_error(e)
            # print(f"[{ticker}] Erro final: {e}")
            return ticker

    # def process_batch_old(self, sub_batch, payload, progress):
    #     """Process a batch of company data by scraping details."""
    #     result = []

    #     start_time = time.time()
    #     for i, (_, row) in enumerate(sub_batch.iterrows()):
    #         try:
    #             company_name = row["company_name"]
    #             company_info = row.to_dict()  # Convert the row to a dictionary for processing

    #             # Fetch and process company datails per driver
    #             driver, driver_wait = self.driver, self.driver_wait
    #             # driver, driver_wait = self._initialize_driver()
    #             company_data = self._fetch_and_process_company(company_name, company_info, driver, driver_wait)
    #             # self.close_driver(driver)

    #             actual_item = progress["batch_start"] + i
    #             total_items = progress["scrape_size"]
    #             worker_info = f"Worker {progress['thread_id']} Item {100 * (1 + actual_item) / total_items:.02f}% ({1 + actual_item}/{total_items})"
    #             extra_info = [worker_info, company_info["ticker"], 'skipped' , company_name, 'ACCESS DENIED by RATE LIMIT']

    #             if company_data:
    #                 extra_info = [worker_info, company_info["ticker"], company_data.get("cvm_code", ""), company_name]
    #                 result.append(company_data)

    #             # Log progress
    #             self.print_info(i, len(sub_batch), start_time, extra_info, indent_level=0)

    #         except Exception as e:
    #             self.log_error(f"Error processing row {i}: {e}")

    #     result = pd.DataFrame(result)

    #     return result

    # def _fetch_and_process_company(self, company_name, company_info, driver, driver_wait):
    #     """Fetch and process details for a single company."""
    #     try:
    #         self.test_internet()
    #         # Obter headers e proxy (opcional)
    #         # headers = headers

    #         url = self.config.domain["company_url"]
    #         driver.get(url)


    #         passed_dns_content = self.detect_dns_block(company_name, driver, driver_wait)
    #         if not passed_dns_content:
    #             return False

    #         search_field_xpath = '//*[@id="keyword"]'
    #         search_button_xpath = '//*[@id="divContainerIframeB3"]/form/button'

    #         max_retries = self.config.selenium.get("max_retries", 5)  # Default to 5 retries if not set
    #         retry_count = 0

    #         while retry_count < max_retries:
    #             self._search_company(company_name, search_field_xpath, driver, driver_wait)  # Try searching again

    #             # Wait for the button to appear after searching
    #             if self.wait_forever(self.driver_wait, search_button_xpath):
    #                 break  # Success, exit loop

    #             self.test_internet()  # Check connection before retrying
    #             retry_count += 1

    #         # If the search was never successful, handle the failure
    #         if retry_count == max_retries:
    #             raise TimeoutException(f"Company search failed after {max_retries} retries.")

    #         # Extract details
    #         card_xpath = '//div[contains(@class, "card-body")]'
    #         self.wait_forever(driver_wait, card_xpath)

    #         cards = driver.find_elements(By.CLASS_NAME, "card-body")

    #         for card in cards:
    #             # Extração segura com Selenium
    #             card_ticker = self.clean_text(card.find_element(By.CLASS_NAME, "card-title2").text)

    #             if card_ticker == company_info["ticker"]:
    #                 # Clica no card correspondente
    #                 card_xpath = f'//h5[text()="{card_ticker}"]'
    #                 attempt = 0
    #                 while attempt < max_retries:
    #                     if self.click(card_xpath, driver_wait):
    #                         break
    #                     attempt += 1
    #                     self.test_internet()
    #                     driver.refresh()

    #                 # Aguarda botão "Voltar" da tela de detalhes
    #                 max_retries = self.config.selenium.get("max_retries", 5)
    #                 retry_count = 0

    #                 xpath = "/html/body"
    #                 while not self.wait_forever(driver_wait, xpath, max_retries=1) and retry_count < max_retries:
    #                     self.test_internet()
    #                     driver.refresh()
    #                     retry_count += 1

    #                 # Agora sim, extrai os dados com BeautifulSoup (após o click e carregamento)
    #                 xpath_company_card = '//*[@id="divContainerIframeB3"]/app-companies-overview'
    #                 xpath_company_button = '//*[@id="divContainerIframeB3"]//button[text()="Voltar"]'
    #                 xpath_company_body = "/html/body"

    #                 retry_count = 0

    #                 # Aguarda os 3 elementos críticos: card, botão e body
    #                 while retry_count < max_retries:
    #                     company_card_loaded = self.wait_forever(driver_wait, xpath_company_card, max_retries=1)
    #                     company_button_loaded = self.wait_forever(driver_wait, xpath_company_button, max_retries=1)
    #                     company_body_loaded = self.wait_forever(driver_wait, xpath_company_body, max_retries=1)

    #                     if company_card_loaded and company_button_loaded and company_body_loaded:
    #                         break

    #                     self.click(card_xpath, driver_wait)
    #                     retry_count += 1

    #                 passed_dns_content = self.detect_dns_block(company_name, driver, driver_wait, debug=True)
    #                 if not passed_dns_content:
    #                     return False

    #                 company_soup = BeautifulSoup(driver.page_source, "html.parser")

    #                 company_details = self._extract_company_details(company_soup, driver.current_url)
    #                 company_info.update(company_details)
    #                 break

    #     except Exception as e:
    #         self.log_error(f"Error processing company {company_name}: {e}")

    #     # Save result to database
    #     self.save_to_db(
    #         dataframe=pd.DataFrame([company_info]),
    #         table_name=self.tbl_company_name,
    #         db_filepath=self.db_filepath,
    #         alert=False,
    #     )

    #     return company_info

    # def _search_company(self, company_name, search_field_xpath, driver, driver_wait):
    #     """Perform a search for a company using the search field on the
    #     page."""
    #     try:
    #         search_field = self.wait_forever(driver_wait, search_field_xpath)
    #         search_field.clear()
    #         search_field.send_keys(company_name)
    #         search_field.send_keys(Keys.RETURN)
    #     except Exception as e:
    #         self.log_error(f"Error searching for company {company_name}: {e}")

    #     return True

    # def _extract_company_details(self, company_soup, current_url):
    #     """Extract detailed information about a company from its soup
    #     object."""
    #     company_details = {}
    #     try:
    #         match = re.search(r"/main/(\d+)/", current_url)
    #         cvm_code = match.group(1) if match else ""

    #         ticker_table_id = "accordionBody2"
    #         company_info = company_soup.find("div", class_="card-body")

    #         ticker_codes = []
    #         isin_codes = []

    #         accordion_body = company_soup.find("div", {"id": ticker_table_id})
    #         if accordion_body:
    #             rows = accordion_body.find_all("tr")
    #             for row in rows[1:]:
    #                 cols = row.find_all("td")
    #                 if len(cols) > 1:
    #                     ticker_codes.append(self.clean_text(cols[0].text))
    #                     isin_codes.append(self.clean_text(cols[1].text))

    #         # Serialize lists to strings
    #         ticker_codes_text = json.dumps(ticker_codes)  # JSON serialization
    #         isin_codes_text = json.dumps(isin_codes)

    #         # Extract relevant data fields
    #         cnpj_element = company_info.find(text="CNPJ")
    #         cnpj = re.sub(r"\D", "", cnpj_element.find_next("p", class_="card-linha").text) if cnpj_element else ""

    #         # Additional fields like activity, website, etc.
    #         activity_element = company_info.find(text="Atividade Principal")
    #         activity = activity_element.find_next("p", class_="card-linha").text if activity_element else ""

    #         # Additional processing for sector and listing
    #         sector_element = company_info.find(text="Classificação Setorial")
    #         sector_classification = sector_element.find_next("p", class_="card-linha").text if sector_element else ""

    #         # Extract sector, subsector, segment information
    #         sectors = sector_classification.split("/")
    #         sector = self.clean_text(sectors[0].strip()) if len(sectors) > 0 else ""
    #         subsector = self.clean_text(sectors[1].strip()) if len(sectors) > 1 else ""
    #         segment = self.clean_text(sectors[2].strip()) if len(sectors) > 2 else ""

    #         # Extract website
    #         website_element = company_info.find(text="Site")
    #         website = website_element.find_next("a").text if website_element else ""

    #         # Extract registrar
    #         registrar_element = company_soup.find(text="Escriturador")
    #         registrar = registrar_element.find_next("span").text.strip() if registrar_element else ""

    #         company_details = {
    #             "cvm_code": cvm_code,
    #             "activity": activity,
    #             "sector": sector,
    #             "subsector": subsector,
    #             "segment": segment,
    #             "cnpj": cnpj,
    #             "website": website,
    #             "ticker_codes": ticker_codes_text,
    #             "isin_codes": isin_codes_text,
    #             "registrar": registrar,
    #         }

    #     except Exception as e:
    #         self.log_error(f"Error extracting company details: {e}")

    #     return company_details

    def get_web_companies(self):
        """
        Scrape the list of companies listed on B3 from the official site.

        Steps:
        - Initializes a web scraper with session persistence to handle Cloudflare.
        - Accesses the B3 listed companies homepage to set session cookies.
        - Queries paginated API to retrieve all listed companies.
        - Merges all pages into a single DataFrame containing only the 'ticker' column.

        Returns:
            pd.DataFrame: A DataFrame containing the list of company tickers.
        """
        try:
            # Initialize the scraper
            scraper = self._init_scraper()


            # Container for all companies data
            all_companies = []

            # Start timing the process
            start_time = time.time()

            # First request: page 1
            payload = {"language": "pt-br", "pageNumber": 1, "pageSize": 120}
            token = self.base64_payload(payload)
            endpoint = 'https://sistemaswebb3-listados.b3.com.br/listedCompaniesProxy/CompanyCall/GetInitialCompanies/'

            r1 = self._fetch_with_retry(scraper, endpoint + token)
            r1_json = r1.json()

            total_pages = r1_json['page']['totalPages']

            companies = r1_json['results']
            all_companies.extend(companies)

            # Log progress for page 1
            extra_info = [f"page 1, from {companies[0]['issuingCompany']} to {companies[-1]['issuingCompany']}"]
            self.print_info(0, total_pages, start_time, extra_info)

            # Loop through remaining pages: page 2 onwards
            for i in range(2, total_pages + 1):
                success = False
                while not success:
                    try:
                        payload = {"language": "pt-br", "pageNumber": i, "pageSize": 120}
                        token = self.base64_payload(payload)

                        r2 = self._fetch_with_retry(scraper, endpoint + token)
                        r2_json = r2.json()

                        companies = r2_json["results"]
                        all_companies.extend(companies)

                        # Log progress
                        extra_info = [f"page {i}, from {companies[0]['issuingCompany']} to {companies[-1]['issuingCompany']}"]
                        self.print_info(i - 1, total_pages, start_time, extra_info)

                        success = True  # Only mark as success if no exception occurs

                    except Exception as e:
                        # Reset scraper and retry
                        scraper = self._init_scraper()
                        self.log_error(e)

            # Convert the list of companies into a DataFrame
            df = pd.DataFrame(all_companies)

            # Rename 'issuingCompany' to 'ticker' and keep only the 'ticker' column
            df = df.rename(columns={"issuingCompany": "ticker"})
            df = df[["ticker"]]

        except Exception as e:
            self.log_error(e)
            df = pd.DataFrame()  # Return empty DataFrame if any error occurs

        return df

    # def get_web_companies_old(self):
    #     """"""
    #     try:
    #         field_mapping = {
    #             "ticker": {"tag": "h5", "class": "card-title2"},
    #             "company_name": {"tag": "p", "class": "card-title"},
    #             "trading_name": {"tag": "p", "class": "card-text"},
    #             "listing": {"tag": "p", "class": "card-nome"},
    #         }
    #         # get raw code
    #         try:
    #             select_page_xpath = '//*[@id="selectPage"]'
    #             pagination_xpath = '//*[@id="listing_pagination"]/pagination-template/ul'
    #             nav_bloc_xpath = '//*[@id="nav-bloco"]/div'
    #             next_page_xpath = '//*[@id="listing_pagination"]/pagination-template/ul/li[10]/a'

    #             self.driver, self.driver_wait = self._initialize_driver()

    #             self.test_internet()
    #             self.driver.get(self.config.domain["companies_url"])
    #             self.choose(select_page_xpath, self.driver, self.driver_wait)

    #             text = self.text(pagination_xpath, self.driver_wait)
    #             pages = list(map(int, re.findall(r"\d+", text)))
    #             pages_total = max(pages) - 1

    #             max_retries = self.config.selenium.get("max_retries", 5)
    #             raw_code = []
    #             start_time = time.time()
    #             for i, page in enumerate(range(0, pages_total + 1)):
    #                 time.sleep(self.dynamic_sleep())

    #                 self.wait_forever(self.driver_wait, nav_bloc_xpath)

    #                 # Captura a página atual
    #                 xpath_page = '//*[@id="listing_pagination"]/pagination-template/ul/li[@class="current"]/span[2]'
                    
    #                 page_actual = self.wait_forever(self.driver_wait, xpath_page)
    #                 page_actual = page_actual.text if page_actual else None

    #                 inner_html = self.raw_text(nav_bloc_xpath, self.driver_wait)
    #                 raw_code.append(inner_html)

    #                 if i != pages_total:
    #                     retry_count = 0

    #                     self.click(next_page_xpath, self.driver_wait)
    #                     while retry_count < max_retries:
    #                         elements = self.driver.find_elements(By.XPATH, xpath_page)
    #                         if elements:
    #                             page_new = elements[0].text
    #                             if page_new != page_actual:
    #                                 break  # sucess

    #                         retry_count += 1
    #                         time.sleep(self.dynamic_sleep())
    #                 extra_info = [f"page {page + 1}"]
    #                 self.print_info(i, pages_total + 1, start_time, extra_info)
    #                 # time.sleep(self.dynamic_sleep() / 50)
    #         except Exception as e:
    #             self.config.log_error(e)
    #             raw_code = []

    #         self.close_driver()

    #         company_tickers = {}

    #         for inner_html in raw_code:
    #             soup = BeautifulSoup(inner_html, "html.parser")
    #             cards = soup.find_all("div", class_="card-body")

    #             for card in cards:
    #                 try:
    #                     extracted_info = {
    #                         key: self.clean_text(card.find(details["tag"], class_=details["class"]).text)
    #                         for key, details in field_mapping.items()
    #                     }

    #                     listing = extracted_info["listing"]
    #                     if listing:
    #                         for abbr, full_name in self.config.domain["governance_levels"].items():
    #                             new_listing = self.clean_text(listing.replace(abbr, full_name))
    #                             if new_listing != listing:
    #                                 extracted_info["listing"] = new_listing
    #                                 break

    #                     company_tickers[extracted_info["company_name"]] = {
    #                         "ticker": extracted_info["ticker"],
    #                         "trading_name": extracted_info["trading_name"],
    #                         "listing": extracted_info["listing"],
    #                     }
    #                 except Exception as e:
    #                     self.log_error(e)

    #         # Convert company_tickers to DataFrame
    #         df = (
    #             pd.DataFrame.from_dict(company_tickers, orient="index")
    #             .reset_index()
    #             .rename(columns={"index": "company_name"})
    #         )

    #     except Exception as e:
    #         df = pd.DataFrame(columns=["company_name", "ticker", "trading_name", "listing"])
    #         self.log_error(e)

    #     return df

    def get_targets(self, local_companies, web_companies):
        """
        Garante que web_companies tenha a estrutura de colunas igual à de local_companies,
        e retorna apenas os registros ainda não existentes.
        """
        # Filtra apenas os novos tickers
        try:
            result = web_companies[~web_companies['ticker'].isin(local_companies['ticker'])]
        except Exception as e:
            self.log_error(e)
            result = pd.DataFrame(columns=['ticker'])

        return result

    # def clean_company_columns(self, df, column_map=None, cols_to_clean=None):
    #     '''
    #     '''
    #     try:
    #         # clean and organize df
    #         column_map = column_map or self.config.domain['columns_company_map']
    #         df = self.auto_map_columns(df, column_map)

    #         cols_to_clean = cols_to_clean or ["company_name", "trading_name"]
    #         for col in cols_to_clean:
    #             df[col] = df[col].apply(self.clean_text)


    #         # Define colunas finais desejadas (mesmas do local)
    #         expected_columns = [
    #             "cvm_code", "company_name", "ticker", "ticker_codes", "isin_codes", "trading_name",
    #             "sector", "subsector", "segment", "listing", "activity", "registrar", "cnpj", "website"
    #         ]

    #         # Garante todas as colunas, adicionando vazias se necessário
    #         for col in expected_columns:
    #             if col not in df.columns:
    #                 df[col] = None  # ou "" se preferir string vazia

    #         # Reduz e reordena as colunas para o padrão local
    #         df = df[expected_columns]

    #         df["segment"] = df["segment"].replace("Não Classificado", "Não Classificados")

    #         # Preencher valores ausentes com base em "segment"
    #         df["sector"] = df["sector"].fillna(df["segment"])
    #         df["subsector"] = df["subsector"].fillna(df["segment"])

    #     except Exception as e:
    #         self.log_error(e)
        
    #     return df

    # def get_sss(self):
    #     '''
    #     '''
    #     try:
    #         # Initialize the homepage to get cookies and session
    #         homepage_url = "https://sistemaswebb3-listados.b3.com.br/listedCompaniesPage/?language=pt-br"

    #         # Create a session that bypasses Cloudflare protections
    #         scraper = cloudscraper.create_scraper()
    #         headers = self.header_random()
    #         scraper.headers.update(headers)
    #         scraper.get(self.homepage_url)

    #         # get sector, subsector, segment to merge
    #         payload = {'language': 'pt-br'}
    #         token = self.base64_payload(payload)
    #         endpoint = 'https://sistemaswebb3-listados.b3.com.br/listedCompaniesProxy/CompanyCall/GetIndustryClassification/'
            
    #         response = scraper.get(endpoint+token)
    #         response_request = response.json()  # ou json.loads(res2.text)
    #         # Expandir para DataFrame
    #         rows = []
    #         for setor in response_request:
    #             nome_setor = setor["sector"]
    #             for subsetor in setor.get("subSectors", []):
    #                 nome_subsetor = subsetor["describle"]
    #                 for segmento in subsetor.get("segment", []):
    #                     rows.append({
    #                         "Setor": nome_setor,
    #                         "Subsetor": nome_subsetor,
    #                         "Segmento": segmento
    #                     })
    #         sss = pd.DataFrame(rows)
    #         sss = sss.rename(columns={
    #             "Segmento": "segment",
    #             "Setor": "sector",
    #             "Subsetor": "subsector"
    #         })
    #     except Exception as e:
    #         self.log_error(e)

    #     return sss

    def main(self, thread=True):
        """Main method to process data."""
        try:
            # Load existing and new companies
            local_companies = self.load_data(table_name=self.tbl_company_name, db_filepath=self.db_filepath)
            web_companies = self.get_web_companies()

            # Identify scrape targets
            targets = self.get_targets(local_companies, web_companies)

            # Exit if no targets
            if targets.empty:
                self.db_optimize(self.config.databases["raw"]["filepath"])
                return True

            # Run batch processing
            result = self.run(
                targets, thread=thread, module_name=self.inspect.getmodule(self.inspect.currentframe()).__name__
            )

            # Save processed data
            if not result.empty:
                self.save_to_db(result, table_name=self.tbl_company_name, db_filepath=self.db_filepath)

        except Exception as e:
            self.log_error(f"Error in main: {e}")

        return True
