import json
import re
import time
from threading import Lock

import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from utils.base_processor import BaseProcessor


class CompanyProcessor(BaseProcessor):
    """Processar dados de empresas."""

    def __init__(self):
        super().__init__()
        self.db_lock = Lock()  # Initialize a threading Lock

        # Initialize database and table names
        self.tbl_company_name = self.config.databases["raw"]["table"]["company_info"]
        self.db_filepath = self.config.databases["raw"]["filepath"]

        # Initialize driver and other resources
        self.driver, self.driver_wait = self._initialize_driver()

    def process_instance(self, sub_batch, payload, progress):
        """Process a single batch by delegating to process_batch."""
        result = pd.DataFrame()

        try:
            print(
                f'Starting batch {progress["batch_index"]}/{progress["total_batches"]} {100*progress["batch_index"]/progress["total_batches"]:.02f}%'
            )

            batch_processor = CompanyProcessor()

            # Delegate to process_batch for the actual batch processing
            result, benchmark_results = batch_processor.benchmark_function(
                batch_processor.process_batch,
                sub_batch,
                progress,
                benchmark_mode=FalseFalseTrue,
            )

            # Clean up driver after processing
            batch_processor.close_driver()

            # Save result to database
            self.save_to_db(
                dataframe=result,
                table_name=self.tbl_company_name,
                db_filepath=self.db_filepath,
            )

        except Exception as e:
            self.log_error(f"Error in process_instance: {e}")
            self.close_driver()  # Ensure driver is closed even on errors

        return result

    def process_batch(self, sub_batch, payload, progress):
        """Process a batch of company data by scraping details."""
        result = []

        start_time = time.time()
        for i, (_, row) in enumerate(sub_batch.iterrows()):
            try:
                company_name = row["company_name"]
                company_info = (
                    row.to_dict()
                )  # Convert the row to a dictionary for processing

                # Fetch and process company details
                company_data = self._fetch_and_process_company(
                    company_name, company_info
                )
                result.append(company_data)

                # Log progress
                actual_item = progress["batch_start"] + i
                total_items = progress["scrape_size"] + 1
                worker_info = f"Worker {progress['thread_id']} Item {100*actual_item/total_items:.02f}% ({actual_item}/{total_items})"
                extra_info = [
                    worker_info,
                    company_info["ticker"],
                    company_data.get("cvm_code", ""),
                    company_name,
                ]
                self.print_info(
                    i, len(sub_batch), start_time, extra_info, indent_level=0
                )

            except Exception as e:
                self.log_error(f"Error processing row {i}: {e}")

        result = pd.DataFrame(result)

        return result

    def _fetch_and_process_company(self, company_name, company_info):
        """Fetch and process details for a single company."""
        try:
            self.test_internet()
            self.driver.get(self.config.domain["company_url"])

            search_field_xpath = '//*[@id="keyword"]'
            search_button_xpath = '//*[@id="divContainerIframeB3"]/form/button'

            max_retries = self.config.selenium.get(
                "max_retries", 5
            )  # Default to 5 retries if not set
            retry_count = 0

            while retry_count < max_retries:
                self._search_company(
                    company_name, search_field_xpath
                )  # Try searching again

                # Wait for the button to appear after searching
                if self.wait_forever(self.driver_wait, search_button_xpath):
                    break  # Success, exit loop

                self.test_internet()  # Check connection before retrying
                retry_count += 1

            # If the search was never successful, handle the failure
            if retry_count == max_retries:
                raise TimeoutException(
                    f"Company search failed after {max_retries} retries."
                )

            # Extract details
            soup = BeautifulSoup(self.driver.page_source, "html.parser")
            cards = soup.find_all("div", class_="card-body")

            for card in cards:
                card_ticker = self.clean_text(
                    card.find("h5", class_="card-title2").text
                )
                if card_ticker == company_info["ticker"]:
                    card_xpath = f'//h5[text()="{card_ticker}"]'
                    self.click(card_xpath, self.driver_wait)

                    max_retries = self.config.selenium.get(
                        "max_retries", 5
                    )  # Set a default max retry value
                    retry_count = 0
                    xpath = '//*[@id="divContainerIframeB3"]/app-companies-overview/div/div/button'
                    while (
                        not self.wait_forever(self.driver_wait, xpath)
                        and retry_count < max_retries
                    ):
                        self.test_internet()
                        self.driver.refresh()  # Reloads the current page instead of navigating to a new one
                        retry_count += 1

                    # Extract additional company details
                    company_soup = BeautifulSoup(self.driver.page_source, "html.parser")
                    company_details = self._extract_company_details(company_soup)
                    company_info.update(company_details)
                    break

        except Exception as e:
            self.log_error(f"Error processing company {company_name}: {e}")

        return company_info

    def _search_company(self, company_name, search_field_xpath):
        """Perform a search for a company using the search field on the
        page."""
        try:
            search_field = self.wait_forever(self.driver_wait, search_field_xpath)
            search_field.clear()
            search_field.send_keys(company_name)
            search_field.send_keys(Keys.RETURN)
        except Exception as e:
            self.log_error(f"Error searching for company {company_name}: {e}")

        return True

    def _extract_company_details(self, company_soup):
        """Extract detailed information about a company from its soup
        object."""
        company_details = {}
        try:
            match = re.search(r"/main/(\d+)/", self.driver.current_url)
            cvm_code = match.group(1) if match else ""

            ticker_table_id = "accordionBody2"
            company_info = company_soup.find("div", class_="card-body")

            ticker_codes = []
            isin_codes = []

            accordion_body = company_soup.find("div", {"id": ticker_table_id})
            if accordion_body:
                rows = accordion_body.find_all("tr")
                for row in rows[1:]:
                    cols = row.find_all("td")
                    if len(cols) > 1:
                        ticker_codes.append(self.clean_text(cols[0].text))
                        isin_codes.append(self.clean_text(cols[1].text))

            # Serialize lists to strings
            ticker_codes_text = json.dumps(ticker_codes)  # JSON serialization
            isin_codes_text = json.dumps(isin_codes)

            # Extract relevant data fields
            cnpj_element = company_info.find(text="CNPJ")
            cnpj = (
                re.sub(r"\D", "", cnpj_element.find_next("p", class_="card-linha").text)
                if cnpj_element
                else ""
            )

            # Additional fields like activity, website, etc.
            activity_element = company_info.find(text="Atividade Principal")
            activity = (
                activity_element.find_next("p", class_="card-linha").text
                if activity_element
                else ""
            )

            # Additional processing for sector and listing
            sector_element = company_info.find(text="Classificação Setorial")
            sector_classification = (
                sector_element.find_next("p", class_="card-linha").text
                if sector_element
                else ""
            )

            # Extract sector, subsector, segment information
            sectors = sector_classification.split("/")
            sector = self.clean_text(sectors[0].strip()) if len(sectors) > 0 else ""
            subsector = self.clean_text(sectors[1].strip()) if len(sectors) > 1 else ""
            segment = self.clean_text(sectors[2].strip()) if len(sectors) > 2 else ""

            # Extract website
            website_element = company_info.find(text="Site")
            website = website_element.find_next("a").text if website_element else ""

            # Extract registrar
            registrar_element = company_soup.find(text="Escriturador")
            registrar = (
                registrar_element.find_next("span").text.strip()
                if registrar_element
                else ""
            )

            company_details = {
                "cvm_code": cvm_code,
                "activity": activity,
                "sector": sector,
                "subsector": subsector,
                "segment": segment,
                "cnpj": cnpj,
                "website": website,
                "ticker_codes": ticker_codes_text,
                "isin_codes": isin_codes_text,
                "registrar": registrar,
            }

        except Exception as e:
            self.log_error(f"Error extracting company details: {e}")

        return company_details

    def get_web_companies(self):
        """"""
        try:
            field_mapping = {
                "ticker": {"tag": "h5", "class": "card-title2"},
                "company_name": {"tag": "p", "class": "card-title"},
                "trading_name": {"tag": "p", "class": "card-text"},
                "listing": {"tag": "p", "class": "card-nome"},
            }
            # get raw code
            try:
                select_page_xpath = '//*[@id="selectPage"]'
                pagination_xpath = (
                    '//*[@id="listing_pagination"]/pagination-template/ul'
                )
                nav_bloc_xpath = '//*[@id="nav-bloco"]/div'
                next_page_xpath = (
                    '//*[@id="listing_pagination"]/pagination-template/ul/li[10]/a'
                )

                self.driver, self.driver_wait = self._initialize_driver()

                self.test_internet()
                self.driver.get(self.config.domain["companies_url"])
                self.choose(select_page_xpath, self.driver, self.driver_wait)

                text = self.text(pagination_xpath, self.driver_wait)
                pages = list(map(int, re.findall(r"\d+", text)))
                total_pages = max(pages) - 1

                raw_code = []
                start_time = time.time()
                for i, page in enumerate(range(0, total_pages + 1)):
                    self.wait_forever(self.driver_wait, nav_bloc_xpath)
                    inner_html = self.raw_text(nav_bloc_xpath, self.driver_wait)
                    raw_code.append(inner_html)

                    if i != total_pages:
                        self.click(next_page_xpath, self.driver_wait)

                    extra_info = [f"page {page + 1}"]
                    self.print_info(i, total_pages + 1, start_time, extra_info)
                    time.sleep(self.config.selenium["wait_time"] / 50)

            except Exception as e:
                self.config.log_error(e)
                raw_code = []

            self.close_driver()

            company_tickers = {}

            for inner_html in raw_code:
                soup = BeautifulSoup(inner_html, "html.parser")
                cards = soup.find_all("div", class_="card-body")

                for card in cards:
                    try:
                        extracted_info = {
                            key: self.clean_text(
                                card.find(details["tag"], class_=details["class"]).text
                            )
                            for key, details in field_mapping.items()
                        }

                        listing = extracted_info["listing"]
                        if listing:
                            for abbr, full_name in self.config.domain[
                                "governance_levels"
                            ].items():
                                new_listing = self.clean_text(
                                    listing.replace(abbr, full_name)
                                )
                                if new_listing != listing:
                                    extracted_info["listing"] = new_listing
                                    break

                        company_tickers[extracted_info["company_name"]] = {
                            "ticker": extracted_info["ticker"],
                            "trading_name": extracted_info["trading_name"],
                            "listing": extracted_info["listing"],
                        }
                    except Exception as e:
                        self.self.log_error(e)

            # Convert company_tickers to DataFrame
            df = (
                pd.DataFrame.from_dict(company_tickers, orient="index")
                .reset_index()
                .rename(columns={"index": "company_name"})
            )

        except Exception as e:
            df = pd.DataFrame(
                columns=["company_name", "ticker", "trading_name", "listing"]
            )
            self.self.log_error(e)

        return df

    def get_targets(self, local_companies, web_companies):
        """"""
        result = []
        try:
            result = web_companies[
                ~web_companies["company_name"].isin(local_companies["company_name"])
            ]
        except Exception as e:
            # self.log_error(e)
            result = web_companies

        return result

    def main(self, thread=True):
        """Main method to process data."""
        try:
            # Load existing and new companies
            local_companies = self.load_data(
                table_name=self.tbl_company_name, db_filepath=self.db_filepath
            )
            web_companies = self.get_web_companies()

            # Identify scrape targets
            targets = self.get_targets(local_companies, web_companies)

            # Exit if no targets
            if targets.empty:
                self.db_optimize(self.config.databases["raw"]["filepath"])
                return True

            # Run batch processing
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
                    result,
                    table_name=self.tbl_company_name,
                    db_filepath=self.db_filepath,
                )

        except Exception as e:
            self.log_error(f"Error in main: {e}")

        return True
