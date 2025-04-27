import datetime
import time
from io import StringIO
from threading import Lock

import pandas as pd
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from urllib.parse import urlencode
from utils.base_processor import BaseProcessor
from bs4 import BeautifulSoup


class StatementsProcessor(BaseProcessor):
    """Financial Statements."""

    def __init__(self):
        super().__init__()
        self.db_lock = Lock()  # Initialize a threading Lock

        # Define at the beginning of your script
        self.total_block_time = 0
        
        # track total bytes transferred (in bytes)
        self.shared_total_bytes = None  # dicionário { "total": int, "threads": {thread_id: subtotal} }
        self.total_bytes_transferred = 0  # fallback local counter (for single-thread usage)
        self.shared_lock = None
        self.thread_id = None  # cada processor deve saber seu thread_id

        self.homepage_url = "https://www.google.com.br"

        # Initialize database and table names
        self.tbl_company = self.config.databases["raw"]["table"]["company_info"]
        self.tbl_nsd = self.config.databases["raw"]["table"]["nsd"]
        self.tbl_statements_raw = self.config.databases["raw"]["table"]["statements_raw"]
        self.db_filepath = self.config.databases["raw"]["filepath"]

        # # Initialize driver and other resources
        # self.driver, self.driver_wait = self._initialize_driver()

    def process_instance(self, sub_batch, payload, progress):
        """Process a single batch by delegating to process_batch."""
        result = pd.DataFrame()  # Return an empty DataFrame on failure

        try:
            print(
                f"Starting batch {progress['batch_index']}/{progress['total_batches']} "
                f"{100 * progress['batch_index'] / progress['total_batches']:.02f}%"
            )
            batch_processor = StatementsProcessor()

            # Inject shared control
            batch_processor.shared_total_bytes = self.shared_total_bytes
            batch_processor.shared_lock = self.shared_lock
            batch_processor.thread_id = progress["thread_id"]  # <- inject thread_id

            # Delegate to process_batch for the actual batch processing
            result, benchmark_results = batch_processor.benchmark_function(
                batch_processor.process_batch, sub_batch, payload, progress, benchmark_mode=False
            )

            # Show subtotal download size
            if self.shared_total_bytes and self.shared_lock and progress.get("thread_id") is not None:
                with self.shared_lock:
                    subtotal = self.shared_total_bytes["threads"].get(progress["thread_id"], 0)
                    print(f"Batch Completed Worker {progress['thread_id']}: {self._format_bytes(subtotal)} transferred")

            # Save result to database
            self.save_to_db(dataframe=result, table_name=self.table_name, db_filepath=self.db_filepath, alert=False)

            # Clean up driver after processing
            batch_processor.close_driver()

        except Exception as e:
            self.log_error(f"Error in process_instance: {e}")
            self.close_driver()  # Ensure driver is closed even on errors

        return result

    def process_batch(self, sub_batch, payload, progress):
        """Process a batch of financial data by iterating over rows and
        scraping statements."""
        try:
            result = []

            start_time = time.time()

            # Initialize batch size tracking
            if self.shared_total_bytes and self.shared_lock and self.thread_id is not None:
                batch_start_bytes = self.shared_total_bytes["threads"].get(self.thread_id, 0)
            else:
                batch_start_bytes = self.total_bytes_transferred

            # create first scraper
            scraper = self._init_scraper(url=self.homepage_url)

            for i, (_, row) in enumerate(sub_batch.iterrows()):
                try:
                    # Extract and process data for each row
                    row_data = self._process_company_quarter_data(row, scraper)
                    result.extend(row_data)

                    # Track bytes transferred after this ticker
                    if self.shared_total_bytes and self.shared_lock and self.thread_id is not None:
                        bytes_after = self.shared_total_bytes["threads"].get(self.thread_id, 0)
                    else:
                        bytes_after = self.total_bytes_transferred
                    bytes_this_item = bytes_after - batch_start_bytes
                    formatted_size = self._format_bytes(bytes_this_item)
                    batch_start_bytes = bytes_after  # Update for next item

                    if i % self.config.selenium['log_loop'] == 0 or i == len(sub_batch) - 1:  # Always log last item too
                        # Log progress

                        actual_item = progress["batch_start"] + i
                        total_items = progress["scrape_size"] + 1
                        worker_info = f"Worker {progress['thread_id']} Item {100 * actual_item / total_items:.02f}% ({actual_item}/{total_items})"
                        nsd = row["nsd"]
                        version = f"v{row['version']}"
                        company = row["company_name"]
                        quarter = datetime.datetime.strptime(row["quarter"], "%Y-%m-%dT%H:%M:%S").strftime("%Y-%m")
                        sent_date = datetime.datetime.strptime(row["sent_date"], "%Y-%m-%dT%H:%M:%S").strftime(
                            "%Y-%m-%d %H:%M:%S"
                        )
                        extra_info = [worker_info, nsd, company, quarter, version, sent_date]
                        self.print_info(i, len(sub_batch), start_time, extra_info)

                        # Save result to database
                        if result:
                            temp_df = pd.concat(result, ignore_index=True)
                        else:
                            temp_df = pd.DataFrame(columns=self.config.domain["statements_columns"])

                        self.save_to_db(dataframe=temp_df, table_name=self.table_name, db_filepath=self.db_filepath, alert=False)

                except Exception as e:
                    self.log_error(f"Error processing row {i}: {e}")
                    row_data = "empty"

            # After processing the batch
            progress["batch_start"] += len(sub_batch)

            # Combine results into a single DataFrame
            if result:
                result = pd.concat(result, ignore_index=True)
            else:
                result = pd.DataFrame(columns=self.config.domain["statements_columns"])

        except Exception as e:
            self.log_error(e)
        
        return result

    def _process_company_quarter_data(self, row, scraper=None):
        '''
        '''
        company_quarter_data = []  # List to store data for the company in the current quarter
        try:
            if scraper is None:
                scraper = self._init_scraper(url=self.homepage_url)
            row.to_csv('row.csv', index=True)

            # get row values
            nsd = row['nsd']
            company_name = row['company_name']
            quarter_raw = row['quarter']
            quarter_dt = pd.to_datetime(quarter_raw)
            quarter = quarter_dt.strftime("%Y-%m-%d")
            nome_tipo_documento = "DFP" if quarter_dt.quarter == 4 else "ITR"
            cod_tipo_documento = 4 if nome_tipo_documento == "DFP" else 2
            cvm_code = row['cvm_code']
            version = row['version'] 

            # get hash
            endpoint = f"https://www.rad.cvm.gov.br/ENET/frmGerenciaPaginaFRE.aspx?NumeroSequencialDocumento={nsd}&CodigoTipoInstituicao=1"

            # Fire off the three requests, with automatic retry on blocks
            r1 = self._fetch_with_retry(scraper, endpoint)

            # Parse the response HTML
            html = r1.text
            soup = BeautifulSoup(html, "html.parser")
            hash_value = soup.select_one("#hdnHash")["value"]

            # base_url
            base = "https://www.rad.cvm.gov.br/ENET/"
            base_url_financial = f"{base}frmDemonstracaoFinanceira{nome_tipo_documento}.aspx"
            base_url_financial = f"{base}frmDemonstracaoFinanceiraITR.aspx"
            base_url_capital = f"{base}/rmDadosComposicaoCapital{nome_tipo_documento}.aspx"

            # Definição dos parâmetros fixos para cada demonstração
            document_configs = {
                "dc_bpa": {"Informacao": 2, "Demonstracao": 2, "Periodo": 0, "Grupo": "DFs Consolidadas", "Quadro": "Balanço Patrimonial Ativo"},
                "dc_bpp": {"Informacao": 2, "Demonstracao": 3, "Periodo": 0, "Grupo": "DFs Consolidadas", "Quadro": "Balanço Patrimonial Passivo"},
                "dc_dre": {"Informacao": 2, "Demonstracao": 4, "Periodo": 0, "Grupo": "DFs Consolidadas", "Quadro": "Demonstração do Resultado"},
                "dc_dra": {"Informacao": 2, "Demonstracao": 5, "Periodo": 0, "Grupo": "DFs Consolidadas", "Quadro": "Demonstração do Resultado Abrangente"},
                "dc_dfc": {"Informacao": 2, "Demonstracao": 99, "Periodo": 0, "Grupo": "DFs Consolidadas", "Quadro": "Demonstração do Fluxo de Caixa"},
                "dc_dmpl": {"Informacao": 200, "Demonstracao": 8, "Periodo": "1|2|3", "Grupo": "DFs Consolidadas", "Quadro": "Demonstração das Mutações do Patrimônio Líquido"},
                "dc_dva": {"Informacao": 2, "Demonstracao": 9, "Periodo": 0, "Grupo": "DFs Consolidadas", "Quadro": "Demonstração de Valor Adicionado"},

                "di_bpa": {"Informacao": 1, "Demonstracao": 2, "Periodo": 0, "Grupo": "DFs Individuais", "Quadro": "Balanço Patrimonial Ativo"},
                "di_bpp": {"Informacao": 1, "Demonstracao": 3, "Periodo": 0, "Grupo": "DFs Individuais", "Quadro": "Balanço Patrimonial Passivo"},
                "di_dre": {"Informacao": 1, "Demonstracao": 4, "Periodo": 0, "Grupo": "DFs Individuais", "Quadro": "Demonstração do Resultado"},
                "di_dra": {"Informacao": 1, "Demonstracao": 5, "Periodo": 0, "Grupo": "DFs Individuais", "Quadro": "Demonstração do Resultado Abrangente"},
                "di_dfc": {"Informacao": 1, "Demonstracao": 99, "Periodo": 0, "Grupo": "DFs Individuais", "Quadro": "Demonstração do Fluxo de Caixa"},
                "di_dmpl": {"Informacao": 100, "Demonstracao": 8, "Periodo": "1|2|3", "Grupo": "DFs Individuais", "Quadro": "Demonstração das Mutações do Patrimônio Líquido"},
                "di_dva": {"Informacao": 1, "Demonstracao": 9, "Periodo": 0, "Grupo": "DFs Individuais", "Quadro": "Demonstração de Valor Adicionado"},

                "composicao_capital": {"Grupo": "Dados da Empresa", "Quadro": "Composição do Capital"}
            }

            endpoints = {}
            for key, cfg in document_configs.items():
                url_key = base_url_capital if key == "composicao_capital" else base_url_financial

                # monta params injetando o que é dinâmico
                params = {
                    **cfg,
                    "NomeTipoDocumento": nome_tipo_documento,
                    "DataReferencia": quarter,
                    "CodTipoDocumento": cod_tipo_documento,
                    "NumeroSequencialDocumento": nsd,
                    "CodigoTipoInstituicao": 1,
                    "Hash": hash_value,
                }

                # constrói a URL escapando corretamente
                endpoint = f"{url_key}?{urlencode(params, doseq=True)}"
                endpoints[key] = endpoint

                # r2 = self._fetch_with_retry(scraper, endpoint)
                # html2 = r2.text
                # soup2 = BeautifulSoup(html2, "html.parser")
                # pass

            print(endpoints)

        except Exception as e:
            self.log_error(e)

        return endpoints

    def _process_company_quarter_data_old(self, row, scraper=None):
        """Internal method to process financial and statements data for a
        specific company and quarter.

        Args:
            row (pd.Series): A row of data containing NSD, company name, quarter, sector, and other metadata.

        Returns:
            list: A list of DataFrames with the processed financial and statements data for the company.
        """
        if scraper is None:
            scraper = self._init_scraper(url=self.homepage_url)

        try:
            company_quarter_data = []  # List to store data for the company in the current quarter

            # Extract data from the row
            nsd = row["nsd"]
            company_name = row["company_name"]
            quarter = pd.to_datetime(row["quarter"], dayfirst=False, errors="coerce").strftime("%Y-%m-%d")
            sector = row["sector"]
            subsector = row["subsector"]
            segment = row["segment"]
            version = row["version"]

            # Construct the URL for the NSD entry
            url = f"https://www.rad.cvm.gov.br/ENET/frmGerenciaPaginaFRE.aspx?NumeroSequencialDocumento={nsd}&CodigoTipoInstituicao=1"
            self.test_internet()
            self.driver.get(url)

            # Define all statements to be scraped
            statements = (
                self.config.domain["statements_financial_data"] + self.config.domain["statements_capital_config"]
            )

            for cmbGrupo, cmbQuadro in statements:
                # Determine which scraping method to use
                if [cmbGrupo, cmbQuadro] in self.config.domain["statements_financial_data"]:
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
                        frame=cmbQuadro,
                    )
                    # Append the processed DataFrame to the list
                    company_quarter_data.append(df[self.config.domain["statements_columns"]])

            return company_quarter_data

        except Exception as e:
            # Log any errors encountered during processing
            self.log_error(f"Error processing company quarter data: {e}")
            return []  # Return an empty list to prevent the process from stopping

    def get_targets(self, company_info, existing_nsd, financial_statements):
        """"""
        last_order = "ZZZZZZZZZZ"
        scrape_order = ["sector", "subsector", "segment", "company_name", "quarter", "version"]

        try:
            # nsd_df keep only valid types
            nsd_df = existing_nsd[existing_nsd["nsd_type"].isin(self.config.domain["statements_types"])]

            # merge nsd and company info
            nsd_company_df = pd.merge(nsd_df, company_info, on="company_name", how="inner")

            nsd_company_outer = pd.merge(nsd_df, company_info, on="company_name", how="outer")
            nsd_company_unmatched_df = nsd_company_outer[~nsd_company_outer['company_name'].isin(nsd_company_df['company_name'])]
            
            try:
                # remove already processed nsd (existing in financial_statements df)
                targets = nsd_company_df[~nsd_company_df["nsd"].isin(financial_statements["nsd"].unique())]
            except Exception as e:
                targets = nsd_company_df
                self.log_error(e)

            # Custom sorting to place empty fields last
            targets.loc[targets["sector"] == "", "sector"] = last_order
            targets.loc[targets["subsector"] == "", "subsector"] = last_order
            targets.loc[targets["segment"] == "", "segment"] = last_order

            # Order the list by sector, subsector, segment, company_name, quarter, and version
            targets = targets.sort_values(by=scrape_order, ascending=True)

            # Restore empty fields
            targets.loc[targets["sector"] == last_order, "sector"] = ""
            targets.loc[targets["subsector"] == last_order, "subsector"] = ""
            targets.loc[targets["segment"] == last_order, "segment"] = ""

            return targets

        except Exception as e:
            self.log_error(e)

        return targets

    def _scrape_financial_data(self, cmbGrupo, cmbQuadro):
        """Scrapes statements data from the specified page.

        Parameters:
        - group_value: The group combo box value.
        - quadro_value: The frame combo box value.

        Returns:
        - DataFrame: A DataFrame containing the scraped data.
        """
        try:
            drop_items = "3.99"
            xpath_grupo = '//*[@id="cmbGrupo"]'
            xpath_quadro = '//*[@id="cmbQuadro"]'
            xpath_frame = '//*[@id="iFrameFormulariosFilho"]'

            # Select the correct options for cmbGrupo and cmbQuadro
            self.test_internet()

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
            df2 = pd.read_html(StringIO(html_content), header=0, thousands=".")[0].fillna(0)

            split_col = 2
            total_col = 3
            df1 = df1.iloc[:, 0:total_col]
            df2 = df2.iloc[:, 0:total_col]
            df1.columns = self.config.domain["financial_statements_columns"]
            df2.columns = self.config.domain["financial_statements_columns"]
            df = pd.concat([df1.iloc[:, :split_col], df2.iloc[:, split_col:total_col]], axis=1)

            col = df.iloc[:, split_col].astype(str)
            col = col.str.replace(".", "", regex=False)
            col = col.str.replace(",", ".", regex=False)
            col = pd.to_numeric(col, errors="coerce")
            col = col * thousand
            df.iloc[:, split_col] = col

            try:
                df = df[~df[self.config.domain["financial_statements_columns"][0]].str.startswith(drop_items)]
            except Exception:
                pass

            # selenium exit frame
            self.driver.switch_to.parent_frame()

            return df

        except Exception:
            # self.log_error(e)
            return None

    def _scrape_statements_data(self, cmbGrupo, cmbQuadro):
        """Process the scraped statements data into a DataFrame.

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
            thousand_word = "Mil"

            # XPaths for the different data points
            acoes_on_xpath = '//*[@id="QtdAordCapiItgz_1"]'
            acoes_pn_xpath = '//*[@id="QtdAprfCapiItgz_1"]'
            acoes_on_tesouraria_xpath = '//*[@id="QtdAordTeso_1"]'
            acoes_pn_tesouraria_xpath = '//*[@id="QtdAprfTeso_1"]'

            # Select the correct options for cmbGrupo and cmbQuadro
            self.test_internet()
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
                self.config.domain["financial_statements_columns"][0]: [],  # 'account'
                self.config.domain["financial_statements_columns"][1]: [],  # 'description'
                self.config.domain["financial_statements_columns"][2]: [],  # 'value'
            }

            # Extract values using the XPaths
            acoes_on = (
                self.driver.find_element(By.XPATH, acoes_on_xpath).text.strip().replace(".", "").replace(",", ".")
            )
            acoes_pn = (
                self.driver.find_element(By.XPATH, acoes_pn_xpath).text.strip().replace(".", "").replace(",", ".")
            )
            acoes_on_tesouraria = (
                self.driver.find_element(By.XPATH, acoes_on_tesouraria_xpath)
                .text.strip()
                .replace(".", "")
                .replace(",", ".")
            )
            acoes_pn_tesouraria = (
                self.driver.find_element(By.XPATH, acoes_pn_tesouraria_xpath)
                .text.strip()
                .replace(".", "")
                .replace(",", ".")
            )

            # Populate the data dictionary using settings values
            data[self.config.domain["financial_statements_columns"][0]] = [
                self.config.domain["accounts"]["acoes_on"],
                self.config.domain["accounts"]["acoes_pn"],
                self.config.domain["accounts"]["acoes_on_tesouraria"],
                self.config.domain["accounts"]["acoes_pn_tesouraria"],
            ]
            data[self.config.domain["financial_statements_columns"][1]] = [
                self.config.domain["descriptions"]["acoes_on"],
                self.config.domain["descriptions"]["acoes_pn"],
                self.config.domain["descriptions"]["acoes_on_tesouraria"],
                self.config.domain["descriptions"]["acoes_pn_tesouraria"],
            ]
            data[self.config.domain["financial_statements_columns"][2]] = [
                float(acoes_on) * thousand,
                float(acoes_pn) * thousand,
                float(acoes_on_tesouraria) * thousand,
                float(acoes_pn_tesouraria) * thousand,
            ]

            df = pd.DataFrame(data)

            # Selenium exit frame
            self.driver.switch_to.parent_frame()

            return df

        except Exception:
            # self.log_error(f"Error processing statements data: {e}")
            return None

    def main(self, thread=True):
        """Main method to process data."""
        try:
            # Load necessary data
            company_info = self.load_data(table_name=self.tbl_company, db_filepath=self.db_filepath)
            existing_nsd = self.load_data(table_name=self.tbl_nsd, db_filepath=self.db_filepath)
            financial_statements = self.load_data(table_name=self.tbl_statements_raw, db_filepath=self.db_filepath)

            # Identify scrape targets
            targets = self.get_targets(company_info, existing_nsd, financial_statements)
            targets.to_csv('targets.csv', index=False)
            # Exit if no targets
            if targets.empty:
                self.db_optimize(self.config.databases["raw"]["filepath"])
                return True

            # Process targets using threading or sequential logic
            result = self.run(
                targets, thread=thread, module_name=self.inspect.getmodule(self.inspect.currentframe()).__name__
            )

            # Save processed data
            if not result.empty:
                self.save_to_db(dataframe=result, table_name=self.tbl_statements_raw, db_filepath=self.db_filepath)

        except Exception as e:
            self.log_error(f"Error in main: {e}")

        return True
