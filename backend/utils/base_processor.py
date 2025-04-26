import base64
import concurrent.futures
import inspect
import json
import logging
import os
import platform
import random
import re
import sqlite3
import string
import subprocess
import threading
import time
import traceback
import warnings
import zipfile
from abc import abstractmethod
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from threading import Lock

import pandas as pd
import psutil
import pyautogui
import requests
import unidecode
import urllib3
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException, StaleElementReferenceException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select, WebDriverWait
from tqdm import tqdm

from utils.config import Config

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
with warnings.catch_warnings():
    warnings.filterwarnings("ignore", category=FutureWarning, module="pandas")


class BaseProcessor:
    def __init__(self):
        self.inspect = inspect
        self.config = Config()  # Assume Config is already defined
        self.db_lock = Lock()  # Initialize a threading Lock

    # APP FLOW LOGIC
    def run(self, data, payload=None, thread=True, module_name=""):
        """Split data into batches and process them sequentially or with
        threads."""
        results = []
        try:
            if "utils.intel_processor" in module_name:
                batches = self._split_batches_by_company(data, self.config.scraping["max_workers"])
            else:
                batches = self._split_batches(data, self.config.scraping["max_workers"])
                items_per_batch = len(batches[0])

            if thread:
                print(
                    f"From {module_name.split('.')[-1]}: processing {data.shape[0]} items in {len(batches)} batches of up to {items_per_batch} items each"
                )
                results = self._process_with_threads(batches, payload=payload)
            else:
                print(
                    f"From {module_name.split('.')[-1]}: processing {data.shape[0]} items in {self.config.scraping['max_workers']} batches of up to {items_per_batch} items each"
                )
                results = self._process_sequentially(batches, payload=payload)

        except Exception as e:
            self.log_error(e)

        try:
            processed_batch = pd.concat(results, ignore_index=True) if results else pd.DataFrame()
        except Exception:
            # self.log_error(e)
            try:
                # try to flatten
                flat_results = [df for sublist in results for df in sublist]
                processed_batch = pd.concat(flat_results, ignore_index=True)
            except Exception:
                processed_batch = results

        return processed_batch

    def _split_batches(self, data, batch_size):
        """Split data into batches."""
        batches = []
        try:
            # # split with limit as batch_size
            # batches = [data[i:i + batch_size] for i in range(0, len(data), batch_size)]

            # split with limit as batch count
            batch_size = min(batch_size, len(data))  # Ensure batch_size doesn't exceed data length
            chunk_size = (len(data) + batch_size - 1) // batch_size  # Equivalent to ceil(len(data) / batch_size)
            batches = [data[i * chunk_size : (i + 1) * chunk_size] for i in range(batch_size)]

        except Exception as e:
            self.log_error(e)

        return batches

    def _split_batches_by_company(self, data, num_workers):
        """docstring."""
        batches = []
        try:
            # Get unique company names
            unique_companies = data["company_name"].unique()
            num_companies = len(unique_companies)

            # Handle case where there are fewer companies than workers
            if num_companies < num_workers:
                num_workers = num_companies  # Set number of workers to the number of companies

            # Calculate batch size and remainder
            batch_size = int(num_companies // num_workers)  # Integer division for batch size
            remainder = num_companies % num_workers  # Calculate the remainder

            batches = []
            start = 0

            # Distribute companies across workers
            for i in range(num_workers):
                end = start + batch_size + (1 if i < remainder else 0)  # Add one extra if there's a remainder
                batch_companies = unique_companies[start:end]
                # Filter the original DataFrame for each batch
                batch_data = data[data["company_name"].isin(batch_companies)]
                batches.append(batch_data)
                start = end

        except Exception as e:
            self.log_error(e)

        return batches

    def _process_with_threads(self, batches, payload):
        """Process batches with threading."""
        results = []
        try:
            total_batches = len(batches)
            start_time = time.time()
            total_scrape_size = sum(len(b) for b in batches)
            cumulative = 0  # will keep track of the global start index for each batch
            items_per_batch = len(batches[0])

            with ThreadPoolExecutor(max_workers=self.config.scraping["max_workers"]) as executor:
                futures = []
                for batch_index, batch in enumerate(batches):
                    time.sleep(self.dynamic_sleep() * 5)
                    progress = {
                        "items_per_batch": items_per_batch,
                        "batch_index": batch_index,
                        "total_batches": total_batches,
                        "batch_start": cumulative,  # actual starting index in the overall data,
                        "scrape_size": total_scrape_size,
                        "start_time": start_time,
                        "thread_id": batch_index
                        % self.config.scraping["max_workers"],  # Map batch index to thread pool ID
                    }
                    cumulative += len(batch)  # add the length of this batch for the next iteration

                    # Submit task with progress
                    futures.append(executor.submit(self.process_instance, batch, payload, progress))

                for future in as_completed(futures):
                    try:
                        result = future.result()
                        results.append(result)
                    except Exception as e:
                        self.log_error(f"Error in thread: {e}")
        except Exception as e:
            self.log_error(e)

        return results

    def _process_sequentially(self, batches, payload):
        """"""
        results = []
        start_time = time.time()
        total_batches = len(batches)
        total_scrape_size = sum(len(b) for b in batches)
        items_per_batch = len(batches[0])

        for batch_index, batch in enumerate(batches):
            # Prepare progress dictionary
            progress = {
                "items_per_batch": items_per_batch,
                "batch_index": batch_index,
                "total_batches": total_batches,
                "batch_start": batch_index * items_per_batch,  # self.config.scraping["batch_size"],
                "scrape_size": total_scrape_size,
                "start_time": start_time,
                "thread_id": batch_index % self.config.scraping["max_workers"],  # Map batch index to thread pool ID
            }

            try:
                result = self.process_instance(batch, payload, progress)
                results.append(result)

            except Exception as e:
                self.log_error(f"Error in batch: {e}")

        return results

    @abstractmethod
    def process_instance(self, batch, payload, progress):
        """To be implemented by child classes."""
        pass

    # WEB & REQUESTS
    def header_random(self):
        """Generate random HTTP headers for requests."""
        try:
            user_agent = random.choice(self.config.requests["user_agents"])
            referer = random.choice(self.config.requests["referers"])
            language = random.choice(self.config.requests["languages"])

            headers = {"User-Agent": user_agent, "Referer": referer, "Accept-Language": language}

            # headers = {
            #     "User-Agent": user_agent,
            #     "Referer": referer,
            #     "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            #     "Accept-Encoding": "gzip, deflate, br",
            #     "Accept-Language": language,
            #     "Connection": "keep-alive",
            #     "Upgrade-Insecure-Requests": "1",
            #     "DNT": "1",
            #     "Sec-Fetch-Mode": "navigate",
            #     "Sec-Fetch-Site": "none",
            #     "Sec-Fetch-User": "?1",
            #     "Sec-Fetch-Dest": "document",
            # }

        except Exception as e:
            self.log_error(e)

        return headers

    def test_internet(self, wait_time=None, url="https://www.google.com/favicon.ico"):
        """Test internet connection by sending an HTTP GET request to a
        specified URL. Retries if no connection is detected.

        Parameters:
            url (str): The URL to request (default: Google's favicon URL).
        """
        wait_time = wait_time = self.dynamic_sleep() * 10

        while True:
            try:
                # set random headers
                headers = self.header_random()
                session = requests.Session()
                session.headers.update(headers)

                # Make a lightweight GET request
                response = session.get(url, timeout=wait_time)
                if response.status_code == 200:
                    return True  # Connection is successful
            except Exception:
                # Log the error or suppress if preferred
                # print(f"No Internet connection: {e}. Retrying in {wait_time} seconds...")
                pass
            time.sleep(wait_time)  # Wait before retrying

    def detect_dns_block(self, title, driver=False, driver_wait=False, content=False, debug=False, block=False):
        """
        Detecta se uma resposta HTML foi bloqueada pela Cloudflare.
        Funciona com Selenium (driver.page_source) e requests (response.text).
        
        Args:
            title (str): Título para o arquivo de debug.
            driver (WebDriver): Instância do Selenium WebDriver.
            debug (bool): Indica se deve salvar o HTML para debug.

        Returns:
            str or bool: Conteúdo da página ou False se bloqueado.
        """
        try:
            self._simulate_human_interaction(driver)
            if not content:
                content = driver.page_source.lower().strip()

                # Verifica bloqueio tentando encontrar elemento exclusivo do erro 1015
                cloudflare_xpath = "//div[@id='cf-error-details']"
                is_blocked = self.wait_forever(driver_wait, cloudflare_xpath, max_retries=1) is not False
            else:
                block_indicators = [
                    "error 1015",
                    "rate limited",
                    "access denied",
                    "cloudflare",
                    "ray id", 
                ]

                is_blocked = any(term in content for term in block_indicators) or (
                    "<app-root></app-root>" in content and len(content) < 10000
                )

            is_blocked = block if block else is_blocked

            filename = f"{'dns_block_' if is_blocked else ''}{title}.html"

            if debug:
                temp_path = os.path.join(self.config.paths["temp_folder"], filename)
                try:
                    with open(temp_path, "w", encoding="utf-8") as f:
                        f.write(content)
                except Exception as e:
                    self.log_error(f"Failed to save blocked HTML content: {e}")

            if is_blocked:
                time.sleep(self.config.selenium['wait_time'] * self.dynamic_sleep())
                content = False
        except Exception as e:
            self.log_error(e)

        return content

    # SELENIUM DRIVER METHODS
    def _get_chrome_version(self):
        """Retrieve the version of Chrome installed on the system.

        Returns:
            str: The Chrome version, or None if not found.
        """
        chrome_error_msg = "Failed to retrieve Chrome version: {e}"

        try:
            for reg_query in self.config.selenium["registry_paths"]:
                try:
                    output = subprocess.check_output(reg_query, shell=True)
                    version = re.search(r"\d+\.\d+\.\d+\.\d+", output.decode("utf-8")).group(0)
                    return version
                except subprocess.CalledProcessError:
                    continue

            try:
                chrome_path = (
                    self.config.selenium["chrome_path_64"]
                    if os.path.exists(self.config.selenium["chrome_path_64"])
                    else self.config.selenium["chrome_path_32"]
                )
                output = subprocess.check_output([chrome_path, "--version"], shell=True)
                version = re.search(r"\d+\.\d+\.\d+\.\d+", output.decode("utf-8")).group(0)
                return version

            except Exception as e:
                self.system.log_error(chrome_error_msg.format(e=e))
                return None
        except Exception as e:
            self.log_error(e)

    def _get_chromedriver_url(self, version):
        """Generate the download URL for ChromeDriver based on the Chrome
        version.

        Args:
            version (str): The Chrome version.

        Returns:
            str: The URL for downloading the corresponding ChromeDriver.
        """
        chromedriver_url_template = (
            f"https://storage.googleapis.com/chrome-for-testing-public/{version}/win64/chromedriver-win64.zip"
        )
        url_error_msg = f"Error obtaining ChromeDriver for version {version}"

        try:
            self.test_internet()
            response = requests.get(chromedriver_url_template)
            if response.status_code == 200:
                return chromedriver_url_template
            else:
                print(url_error_msg)
                return None

        except Exception as e:
            self.log_error(str(e))
            return None

    def _download_and_extract_chromedriver(self, url):
        """Download and extract ChromeDriver from the given URL.

        Args:
            url (str): The URL for downloading ChromeDriver.
            dest_folder (Path): The destination folder for extraction.

        Returns:
            str: The path to the extracted ChromeDriver executable.
        """
        zip_filename = "chromedriver.zip"
        dest_folder = self.config.paths["bin_folder"]
        chromedriver_folder = "chromedriver-win64"
        chromedriver_executable = "chromedriver.exe"
        download_error_msg = "Failed to download or extract ChromeDriver: {e}"

        try:
            self.test_internet()
            response = requests.get(url)
            zip_path = os.path.join(dest_folder, zip_filename)

            with open(zip_path, "wb") as file:
                file.write(response.content)

            with zipfile.ZipFile(zip_path, "r") as zip_ref:
                zip_ref.extractall(dest_folder)

            os.remove(zip_path)
            chromedriver_path = os.path.join(dest_folder, chromedriver_folder, chromedriver_executable)

            return str(chromedriver_path)

        except Exception as e:
            self.log_error(download_error_msg.format(e=e))
            return None

    def _get_chromedriver_path(self):
        """Download and extract the ChromeDriver based on the Chrome version
        installed on the system.

        Returns:
            str: The path to the ChromeDriver executable.
        """
        chrome_version_error_msg = "Unable to determine Chrome version."
        chromedriver_url_error_msg = "Unable to determine the correct ChromeDriver URL."
        path_error_msg = "Failed to obtain ChromeDriver path dynamically."

        try:
            chrome_version = self._get_chrome_version()
            if not chrome_version:
                raise Exception(chrome_version_error_msg)

            chromedriver_url = self._get_chromedriver_url(chrome_version)
            if not chromedriver_url:
                raise Exception(chromedriver_url_error_msg)

            chromedriver_path = self._download_and_extract_chromedriver(chromedriver_url)
            if not chromedriver_path:
                raise Exception(path_error_msg)

            return chromedriver_path

        except Exception as e:
            self.log_error(str(e))
            return None

    def _load_driver(self, chromedriver_path=None):
        """Initialize and return the Selenium WebDriver and WebDriverWait
        instances.

        Args:
            chromedriver_path (str): The path to the ChromeDriver executable.

        Returns:
            tuple: A tuple containing the WebDriver and WebDriverWait instances.
        """
        load_driver_error_msg = "Failed to load driver: {e}"

        chromedriver_path = chromedriver_path or self.config.selenium["chromedriver_path"]

        try:
            # Get random headers using the custom function
            headers = self.header_random()
            width = random.randint(800, 1600)
            height = random.randint(600, 1000)

            chrome_service = Service(executable_path=chromedriver_path)

            chrome_options = Options()

            chrome_options.add_argument(f"--window-size={width},{height}")
            chrome_options.add_argument("--ignore-certificate-errors")
            chrome_options.add_argument("--log-level=3")
            chrome_options.add_argument("--ignore-ssl-errors")
            chrome_options.add_argument("--disable-infobars")

            # anti cloudflare
            chrome_options.add_argument(f"user-agent={headers['User-Agent']}")
            chrome_options.add_argument(f"--lang={headers['Accept-Language']}")
            chrome_options.add_argument(f"--referer={headers['Referer']}")

            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            chrome_options.add_experimental_option("useAutomationExtension", False)
            chrome_options.add_argument("--disable-blink-features=AutomationControlled")

            if self.config.selenium.get("proxy_socks5"):
                chrome_options.add_argument(f"--proxy-server=socks5://{self.config.selenium['proxy_socks5']}")

            # chrome_options.add_argument('--headless')

            driver = webdriver.Chrome(service=chrome_service, options=chrome_options)
            exceptions_ignore = (NoSuchElementException, StaleElementReferenceException)
            driver_wait = WebDriverWait(driver, self.config.selenium["wait_time"], ignored_exceptions=exceptions_ignore)

            return driver, driver_wait

        except Exception as e:
            self.log_error(load_driver_error_msg.format(e=e))
            return None, None

    def _initialize_driver(self):
        """Obtain the Selenium WebDriver and WebDriverWait instances.

        This function either uses a predefined path to ChromeDriver or fetches and loads it dynamically.

        Returns:
            tuple: A tuple containing the WebDriver and WebDriverWait instances.
        """
        # https://googlechromelabs.github.io/chrome-for-testing/#stable
        initialize_driver_error_msg = "Failed to load driver from hardcoded path."
        dynamic_driver_error_msg = "Failed to obtain ChromeDriver path dynamically."

        try:
            driver, driver_wait = self._load_driver()
            if driver is not None:
                return driver, driver_wait
            else:
                raise Exception(initialize_driver_error_msg)

        except Exception:
            try:
                chromedriver_path = self._get_chromedriver_path()
                if not chromedriver_path:
                    raise Exception(dynamic_driver_error_msg)

                driver, driver_wait = self._load_driver(chromedriver_path)
                return driver, driver_wait

            except Exception as dynamic_error:
                self.log_error(str(dynamic_error))
                return None, None

    def _simulate_human_interaction(self, driver):
        """
        Simula interações humanas leves que não exigem foco da janela.
        - Faz scrolls aleatórios.
        - Adiciona pequenas pausas para simular tempo de leitura.
        """
        try:
            # Scroll aleatório para simular navegação
            scroll_height = driver.execute_script("return document.body.scrollHeight")
            max_scroll = max(50, scroll_height // 2)  # Garante valor mínimo razoável

            for _ in range(random.randint(2, 5)):
                scroll = random.randint(10, max_scroll)
                driver.execute_script(f"window.scrollBy(0, {scroll});")
                time.sleep(self.dynamic_sleep())

            # Volta para o topo
            driver.execute_script("window.scrollTo(0, 0);")

            return True

        except Exception as e:
            self.log_error(f"Erro simulando interação humana: {e}")
            return False

    def close_driver(self, driver=None, driver_wait=None):
        """Safely quits the Selenium WebDriver instance."""
        driver = driver or self.driver
        driver_wait = driver_wait or self.driver_wait

        try:
            if driver:
                driver.quit()
        except Exception:
            pass

    # TEXT & SELENIUM OBJECT METHODS
    def clean_text(self, text):
        """Cleans and normalizes the input text by removing punctuation,
        converting to uppercase, removing extra whitespace, and eliminating
        specific words.

        Parameters:
        - text (str): The input text to be cleaned.

        Returns:
        str: The cleaned and normalized text.
        """
        try:
            # Remove punctuation, accents, and normalize case
            translation_table = str.maketrans("", "", string.punctuation)
            if text:
                text = unidecode.unidecode(text)
                text = text.translate(translation_table)
                text = text.upper()
                text = text.strip()
                text = re.sub(r"\s+", " ", text)

                # Regular expression pattern to remove specific words from text
                words_to_remove = "|".join(map(re.escape, self.config.domain["words_to_remove"]))
                pattern = r"\b(?:" + words_to_remove + r")\b"
                text = re.sub(pattern, "", text)

                # Remove extra spaces after word removal
                text = re.sub(r"\s+", " ", text)
                text = text.strip()

        except Exception as e:
            self.log_error(e)

        return text

    def text(self, xpath, driver=None, driver_wait=None):
        """Encontra e recupera o texto de um elemento da web usando o xpath e o
        objeto de espera fornecido.

        Parameters:
        - xpath (str): O xpath do elemento para recuperar o texto.
        - driver_wait (WebDriverWait): O objeto de espera para encontrar o elemento.

        Returns:
        str: O texto do elemento ou uma string vazia se ocorrer uma exceção.
        """
        driver = driver or self.driver
        driver_wait = driver_wait or self.driver_wait

        try:
            element = self.wait_forever(driver_wait, xpath)
            return element.text
        except Exception as e:
            self.log_error(e)
            return ""

    def click(self, xpath, driver_wait=None):
        """Encontra e clica em um elemento da web usando o xpath e o objeto de
        espera fornecido.

        Parameters:
        - xpath (str): O xpath do elemento para clicar.
        - driver_wait (WebDriverWait): O objeto de espera para encontrar o elemento.

        Returns:
        bool: True se o elemento foi encontrado e clicado, False caso contrário.
        """
        driver_wait = driver_wait or self.driver_wait

        try:
            element = self.wait_forever(driver_wait, xpath)
            if not element:
                return False  # retorna False se o elemento não for encontrado

            # time.sleep(self.dynamic_sleep())
            element.click()
            # time.sleep(self.dynamic_sleep())

            return True
        except Exception as e:
            self.log_error(e)
            return False

    def choose(self, xpath, driver=None, driver_wait=None):
        """Encontra e seleciona um elemento da web usando o xpath e o objeto de
        espera fornecido.

        Parameters:
        - xpath (str): O xpath do elemento para selecionar.
        - driver (webdriver.Chrome): O objeto driver Chrome a ser usado.
        - driver_wait (WebDriverWait): O objeto de espera para encontrar o elemento.

        Returns:
        int: O valor da opção selecionada ou uma string vazia se ocorrer uma exceção.
        """
        driver = driver or self.driver
        driver_wait = driver_wait or self.driver_wait

        try:
            element = self.wait_forever(driver_wait, xpath)
            element.click()
            select = Select(driver.find_element(By.XPATH, xpath))
            options = [int(option.text) for option in select.options]
            highest_option = str(max(options))
            select.select_by_value(highest_option)
            return int(highest_option)
        except Exception as e:
            self.log_error(e)
            return ""

    def choose_by_value(self, xpath, value, driver=None, driver_wait=None):
        """Encontra e seleciona um elemento da web usando o xpath e o objeto de
        espera fornecido.

        Parameters:
        - xpath (str): O xpath do elemento para selecionar.
        - driver (webdriver.Chrome): O objeto driver Chrome a ser usado.
        - driver_wait (WebDriverWait): O objeto de espera para encontrar o elemento.

        Returns:
        int: O valor da opção selecionada ou uma string vazia se ocorrer uma exceção.
        """
        driver = driver or self.driver
        driver_wait = driver_wait or self.driver_wait

        try:
            element = self.wait_forever(driver_wait, xpath)
            element.click()
            select = Select(driver.find_element(By.XPATH, xpath))
            # options = [option.text for option in select.options]
            select.select_by_value(value)
            self.click("/html/body")
            return value
        except Exception as e:
            self.log_error(e)
            return ""

    def get_options(self, xpath, driver=None, driver_wait=None):
        """Encontra e retorna os elementos da web usando o xpath e o objeto de
        espera fornecido.

        Parameters:
        - xpath (str): O xpath do elemento para selecionar.
        - driver (webdriver.Chrome): O objeto driver Chrome a ser usado.
        - driver_wait (WebDriverWait): O objeto de espera para encontrar o elemento.

        Returns:
        int: O valor da opção selecionada ou uma string vazia se ocorrer uma exceção.
        """
        driver = driver or self.driver
        driver_wait = driver_wait or self.driver_wait

        try:
            element = self.wait_forever(driver_wait, xpath, max_retries=1)
            element.click()
            select = Select(driver.find_element(By.XPATH, xpath))
            options = [int(option.text) for option in select.options]
            return options
        except Exception:
            # self.log_error(e)
            return ""

    def select(self, xpath, text, driver=None, driver_wait=None):
        """"""
        driver = driver or self.driver
        driver_wait = driver_wait or self.driver_wait
        select = ""

        try:
            element = self.wait_forever(driver_wait, xpath, max_retries=1)
            select = Select(driver.find_element(By.XPATH, xpath))
            select.select_by_visible_text(text)
        except Exception:
            pass
            # self.log_error(e)
        return select

    def raw_text(self, xpath, driver=None, driver_wait=None):
        """Encontra e recupera o HTML bruto de um elemento da web usando o
        xpath e o objeto de espera fornecido.

        Parameters:
        - xpath (str): O xpath do elemento para recuperar o HTML bruto.
        - driver_wait (WebDriverWait): O objeto de espera para encontrar o elemento.

        Returns:
        str: O HTML bruto do elemento ou uma string vazia se ocorrer uma exceção.
        """
        driver = driver or self.driver
        driver_wait = driver_wait or self.driver_wait

        try:
            element = self.wait_forever(driver_wait, xpath)
            return element.get_attribute("innerHTML")
        except Exception as e:
            self.log_error(e)
            return ""

    def wait_forever(self, driver_wait, xpath, max_retries=None):
        """Espera indefinidamente até que o elemento da web localizado pelo
        xpath seja encontrado.

        Parameters:
        - driver_wait (WebDriverWait): O objeto de espera para usar.
        - xpath (str): O xpath do elemento para esperar.

        Returns:
        WebElement: O elemento da web encontrado.
        """
        attempt = 0
        max_retries = max_retries or self.config.selenium.get("max_retries", 5)
        while True:
            try:
                element = driver_wait.until(EC.presence_of_element_located((By.XPATH, xpath)))
                return element
            except Exception:
                attempt += 1
                if max_retries and attempt >= max_retries:
                    return False
                self.driver.refresh()
                time.sleep(self.config.selenium["wait_time"])
                # return False

    def subtract_lists(self, list1, list2):
        """
        Subtract elements of list2 from list1.
        Example: ['a', 'b'] - ['b', 'c'] = ['a']

        Parameters
        ----------
        list1 : list
            The list from which elements will be removed.
        list2 : list
            The list of elements to remove from list1.

        Returns
        -------
        list
            A list containing elements from list1 that are not in list2.
        """
        return [item for item in list1 if item not in list2]

    def escape_keywords(self, keywords):
        """Escape special characters in a list of keywords for regex
        operations.

        Parameters
        ----------
        keywords : list
            A list of keywords that may contain special characters.

        Returns
        -------
        list
            A list of escaped keywords ready for regex operations.
        """
        return [re.escape(keyword) for keyword in keywords]

    def map_dataframe_columns(self, df, mapping):
        """
        Map columns of a dataframe to a target schema, adjusting for missing columns.

        Args:
            df (pd.DataFrame): The input dataframe with raw scraped data.
            mapping (dict): A dictionary where keys are source columns and values are target columns.

        Returns:
            pd.DataFrame: DataFrame with columns renamed and completed according to the target schema.
        """
        try:
            # Defensive copy
            df = df.copy()

            # Check if mapping needs to be inverted
            first_col = df.columns[0]
            if first_col not in mapping.keys() and first_col in mapping.values():
                mapping = {v: k for k, v in mapping.items()}

            # Rename the dataframe
            df = df.rename(columns=mapping)

            # Add missing columns as None
            for target_col in mapping.values():
                if target_col not in df.columns:
                    df[target_col] = None

            # Reorder dataframe according to final column order
            df = df[list(mapping.values())]

        except Exception as e:
            self.log_error(e)
            return pd.DataFrame()

        return df

    # APP METHODS
    def prefill_input(self, text, delay=None):
        """Simulate typing the default text into the input field.

        Parameters:
        - text (str): The text to pre-fill in the input.
        - delay (int): The delay before typing starts.
        """
        try:
            if delay == None:
                delay = self.config.selenium["wait_time"]

            time.sleep(self.dynamic_sleep())
            pyautogui.typewrite(text)

        except Exception as e:
            self.log_error(e)

    def timed_input(self, prompt, timeout=None, default="YES"):
        """
        Exibe um prompt e aguarda input do usuário por um tempo limitado.

        Args:
            prompt (str): A mensagem que será exibida ao usuário.
            timeout (int): Tempo máximo em segundos para aguardar a resposta.
            default (str): Valor padrão caso o tempo se esgote.

        Returns:
            str: Resposta do usuário ou valor padrão.
        """
        try:
            timeout = timeout or self.config.selenium["wait_time"]

            result = []

            def ask_input():
                try:
                    result.append(input(prompt))
                except Exception:
                    result.append(default)

            thread = threading.Thread(target=ask_input)
            thread.daemon = True
            thread.start()
            thread.join(timeout)

        except Exception as e:
            self.log_error(e)

        return result[0] if result else default

    def dynamic_sleep(self):
        """
        Dynamically adjusts the sleep time based on the system's CPU usage.

        The function monitors the CPU usage and adjusts the sleep duration as follows:
        - If CPU usage is greater than 80%, the function returns a longer sleep time (suggested 0.5 seconds).
        - If CPU usage is between 50% and 80%, the function returns a moderate sleep time (suggested 0.3 seconds).
        - If CPU usage is below 50%, the function returns a short sleep time (suggested 0.1 seconds).

        Returns:
            float: The sleep duration in seconds, based on current CPU usage.
        """

        wait = self.config.selenium["wait_time"]

        # Get the current CPU usage
        cpu_usage = psutil.cpu_percent(interval=0.1)  # Get CPU usage over 0.1 second

        # Adjust sleep time based on CPU usage
        if cpu_usage > 80:
            return wait * random.uniform(0.3, 1.5)  # Increase delay if CPU usage is high
        elif cpu_usage > 50:
            return wait * random.uniform(0.2, 1.0)  # delay if CPU usage is medium load
        else:
            return wait * random.uniform(0.1, 0.5)  # delay if CPU usage is medium low

    def detect_and_correct_outliers(self, df):
        """
        definitions
        """
        try:
            # Define primary key columns
            statements_sheet_columns = self.config.domain["statements_sheet_columns"]

            # Initiate variables
            group_cols = ["company_name", "account"]
            value_col = "value"
            date_col = "quarter"
            neighbor_count = 5  # Quantidade de vizinhos a considerar para média

            df_sorted = df.sort_values(by=group_cols + [date_col]).reset_index(drop=True)
            df_sorted["original_value"] = df_sorted[value_col]  # Preserva o valor original

            def process_group(group):
                group = group.copy()  # Evita modificar os dados originais

                # Remove duplicates by keeping the row with the highest value (ignoring 0)
                group = group.loc[group.groupby(statements_sheet_columns)["value"].idxmax()].reset_index(drop=True)

                for idx in range(len(group)):
                    try:
                        # Get Value
                        value = group.iloc[idx][value_col]

                        # Seleciona vizinhos
                        prev_values = group.iloc[max(0, idx - neighbor_count) : idx][value_col].tolist()
                        next_values = group.iloc[idx + 1 : idx + 1 + neighbor_count][value_col].tolist()

                        if not prev_values or not next_values:
                            continue  # Se não há vizinhos suficientes, pula a verificação

                        # Loop regressivo de neighbor_count até 1
                        for n in range(neighbor_count, 0, -1):
                            prev_values_n = prev_values[-n:]  # Considera últimos n valores anteriores
                            next_values_n = next_values[:n]  # Considera primeiros n valores posteriores

                            if prev_values_n and next_values_n:
                                mean_prev = sum(prev_values_n) / len(prev_values_n)
                                mean_next = sum(next_values_n) / len(next_values_n)

                                # Se a média for 1000x maior ou menor e for diferente de 0, aplica a correção
                                if (mean_prev == value * 1000 or mean_prev == value / 1000) and mean_prev != 0:
                                    group.iloc[idx, group.columns.get_loc(value_col)] = mean_prev
                                    break  # Sai do loop ao encontrar um valor válido

                                elif (mean_next == value * 1000 or mean_next == value / 1000) and mean_next != 0:
                                    group.iloc[idx, group.columns.get_loc(value_col)] = mean_next
                                    break  # Sai do loop ao encontrar um valor válido

                    except Exception as e:
                        self.log_error(e)

                return group

            corrected_df = df_sorted.groupby(group_cols, group_keys=False).apply(process_group).reset_index(drop=True)

        except Exception as e:
            self.log_error(e)

        return corrected_df

    def base64_payload(self, payload: dict) -> str:
        """
        Gera um token Base64 com os parâmetros corretos para o endpoint da B3.

        Args:
            payload (dict): Dicionário com parâmetros como language, pageNumber, etc.

        Returns:
            str: String Base64 pronta para ser usada na URL.
        """
        try:
            json_str = json.dumps(payload, separators=(',', ':'))
            base64_encoded = base64.b64encode(json_str.encode()).decode()

        except Exception as e:
            self.log_error(e)

        return base64_encoded

    def base64_decode(self, token_base64: str) -> dict:
        """
        Decodifica um payload Base64 da B3 e retorna como dicionário JSON.

        Args:
            token_base64 (str): Token codificado (ex: 'eyJsYW5ndWFnZSI6InB0LWJyIn0=')

        Returns:
            dict: Dicionário com os dados decodificados
        """
        if token_base64.startswith("https"):
            token_base64 = token_base64.rstrip("/").split("/")[-1]

        try:
            decoded_bytes = base64.b64decode(token_base64)
            decoded_str = decoded_bytes.decode()
            json_str = json.loads(decoded_str)
            return json_str

        except Exception as e:
            self.log_error(e)
            return {}

    # BENCHMARK, LOG & DEBUG METHODS
    def benchmark_function(self, function, *args, benchmark_mode=False, workers_list=None, **kwargs):
        """Generic benchmarking method to evaluate resource usage with
        different worker counts.

        Parameters:
        - function: The function to benchmark (e.g., process_batch).
        - *args: Positional arguments for the function.
        - benchmark_mode (bool): If True, runs the benchmark; otherwise, just calls the function normally.
        - workers_list (list, optional): List of worker counts to test. Defaults to [1, half CPU, full CPU, double CPU].
        - **kwargs: Keyword arguments for the function.

        Returns:
        - original_result: The actual result from the function being benchmarked.
        - benchmark_results: A list of tuples containing (workers, time_taken, memory_used, cpu_usage).
        """
        try:
            if not benchmark_mode:
                # Just run the function normally without benchmarking
                result = function(*args, **kwargs), []

                return result

            if workers_list is None:
                workers_list = [1, max(2, os.cpu_count() // 2), os.cpu_count(), os.cpu_count() * 2]

            print(f"\nRunning benchmark for {inspect.getmodule(function).__name__}.{function.__name__}")

            benchmark_results = []
            original_result = None  # Store the result of the first execution

            for i, workers in enumerate(workers_list):
                print(f"{self.config.domain['indent']}starting benchmark {i + 1} of {len(workers_list)}")
                start_time = time.time()
                process = psutil.Process()
                initial_memory = process.memory_info().rss / (1024 * 1024)  # MB

                with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
                    future = executor.submit(function, *args, **kwargs)
                    result = future.result()

                end_time = time.time()
                elapsed_time = end_time - start_time
                final_memory = process.memory_info().rss / (1024 * 1024)  # MB
                memory_used = final_memory - initial_memory
                cpu_usage = process.cpu_percent(interval=0.5)

                # Store the benchmark data
                benchmark_results.append((workers, elapsed_time, memory_used, cpu_usage))

                print(f"\n🔹 Workers: {workers}")
                print(f"⏳ Time Taken: {elapsed_time:.2f} sec")
                print(f"📈 Memory Used: {memory_used:.2f} MB")
                print(f"⚡ CPU Usage: {cpu_usage:.2f}%\n")

                # Store the original function's result (only from the first execution)
                if original_result is None:
                    original_result = result
        except Exception as e:
            self.log_error(e)
            original_result, benchmark_results = [], []

        return (original_result, benchmark_results)  # Return both the function result and benchmark data

    def log_error(self, error):
        """Logs an error to a file with detailed context, including caller
        info, module, function, line number, timestamp, and system
        information."""
        try:
            # Get the current frame and the caller frame
            current_frame = inspect.currentframe()
            caller_frame = current_frame.f_back

            # Gather detailed context information
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            function_name = caller_frame.f_code.co_name
            line_number = caller_frame.f_lineno
            module_name = caller_frame.f_globals["__name__"]
            system_info = platform.platform()
            caller_name = caller_frame.f_globals["__name__"]

            # Get traceback as string
            full_traceback = traceback.format_exc()

            # Configure logging settings
            logging.basicConfig(
                filename="app_errors.log", level=logging.ERROR, format="%(asctime)s - %(levelname)s - %(message)s"
            )

            # Detailed log message without stack trace
            log_message = (
                f"Timestamp: {timestamp}\n"
                f"Error in module '{module_name}', function '{function_name}', line {line_number}\n"
                f"Caller: {caller_name}\n"
                f"Error: {error}\n"
                f"System Info: {system_info}\n"
                f"Traceback:\n{full_traceback}"
            )

            # Log the error message to the file
            logging.error(log_message)

            # Print a simplified error message to the console
            print(f"Error in {function_name} (line {line_number}): {error}")

        except Exception as e:
            print(e)

        return error

    def print_info(self, index=0, size=1, start_time=time.time(), extra_info=[], indent_level=0):
        """Prints the provided information along with the progress, elapsed
        time, estimated remaining time, and total estimated time."""
        try:
            completed_items = index + 1
            remaining_items = size - completed_items
            percentage_completed = completed_items / size

            elapsed_time = time.time() - start_time
            avg_time_per_item = elapsed_time / completed_items
            remaining_time = remaining_items * avg_time_per_item
            total_estimated_time = elapsed_time + remaining_time

            # Format elapsed time
            elapsed_hours, elapsed_remainder = divmod(int(elapsed_time), 3600)
            elapsed_minutes, elapsed_seconds = divmod(elapsed_remainder, 60)
            elapsed_time_formatted = f"{int(elapsed_hours)}h {int(elapsed_minutes):02}m {int(elapsed_seconds):02}s"

            # Format remaining time
            remaining_hours, remaining_remainder = divmod(int(remaining_time), 3600)
            remaining_minutes, remaining_seconds = divmod(remaining_remainder, 60)
            remaining_time_formatted = (
                f"{int(remaining_hours)}h {int(remaining_minutes):02}m {int(remaining_seconds):02}s"
            )

            # Format total estimated time
            total_hours, total_remainder = divmod(int(total_estimated_time), 3600)
            total_minutes, total_seconds = divmod(total_remainder, 60)
            total_time_formatted = f"{int(total_hours)}h {int(total_minutes):02}m {int(total_seconds):02}s"

            # Prepare progress string
            progress = (
                f"{percentage_completed:.2%} ({completed_items}+{remaining_items}), "
                f"{avg_time_per_item:.4f}s per item, "
                f"{total_time_formatted} = {elapsed_time_formatted} + {remaining_time_formatted}"
            )

            # Add indentation
            indent = self.config.domain["indent"] * (indent_level + 1)
            extra_info_str = " ".join(map(str, extra_info))
            print(f"{indent}{progress} {extra_info_str}")

        except Exception as e:
            self.log_error(e)
            pass

    def winbeep(frequency=5000, duration=50):
        """Generates a system beep sound with the specified frequency and
        duration.

        Parameters:
        - frequency (int): The frequency of the beep sound in Hertz (default is 5000 Hz).
        - duration (int): The duration of the beep sound in milliseconds (default is 50 ms).

        Returns:
        bool: True if the beep was successful, False otherwise.
        """
        # winsound.Beep(frequency, duration)
        return True

    # DATABASE METHODS
    def _initialize_database(self, db_filepath, database_name, table_name=None):
        """Ensure the database and table exist, creating them if necessary."""
        try:
            if not os.path.exists(db_filepath):
                # print(f"Database '{database_name}' does not exist. Creating...")
                with self._get_db_connection(db_filepath, read_only=False) as conn:
                    # Create the database file if it doesn't exist
                    conn.execute("PRAGMA temp_store=MEMORY;")
                    conn.execute("PRAGMA locking_mode=NORMAL;")
                    conn.execute("PRAGMA journal_mode=WAL;")
                    conn.execute("PRAGMA synchronous=NORMAL;")

                if table_name:
                    self._initialize_table(db_filepath, database_name, table_name)
        except Exception as e:
            self.log_error(e)

        return True

    def _initialize_table(self, db_filepath, database_name, table_name):
        """Ensure the specified table exists, creating it if necessary."""
        try:
            schema_definitions = self.config.schemas[database_name]
            for schema_table_name, schema_sql in schema_definitions.items():
                # Handle dynamic table names for sectors
                if schema_table_name in table_name or schema_table_name == table_name:
                    with self._get_db_connection(db_filepath, read_only=False) as conn:
                        cursor = conn.cursor()
                        [cursor.execute(statement.strip()) for statement in schema_sql.split(";") if statement.strip()]
                        conn.commit()
                        # print(f"Table '{table_name}' initialized in '{database_name}'.")
                    return
            # print(f"Warning: No schema defined for table '{table_name}' in database '{database_name}'.")
        except Exception as e:
            self.log_error(e)

        return True

    def _configure_db(self, db_filepath):
        """Set persistent PRAGMA settings for the database."""
        try:
            with sqlite3.connect(db_filepath) as conn:
                conn.execute("PRAGMA temp_store=MEMORY;")
                conn.execute("PRAGMA locking_mode=NORMAL;")
                conn.execute("PRAGMA journal_mode=WAL;")
                conn.execute("PRAGMA synchronous=NORMAL;")
            # print("Database configured successfully.")
        except Exception as e:
            self.log_error(e)

        return conn

    def _get_db_connection(self, db_filepath, read_only=True):
        """Return a new database connection with session-specific PRAGMA settings.

        Parameters:
        - db_filepath (str): Path to the database file.
        - read_only (bool): Whether to open the database in read-only mode (default is False).

        Returns:
        - conn (sqlite3.Connection): A SQLite connection object if successful, else None.
        """
        try:
            # Determine the connection mode based on the read_only flag
            if read_only:
                # Open in read-only mode using URI format
                uri = f"file:{db_filepath}?mode=ro"
            else:
                # Normal read-write mode
                uri = f"file:{db_filepath}"

            # Attempt to establish a connection with the database
            conn = sqlite3.connect(uri, uri=True, check_same_thread=False)

            # Return the connection if successful
            return conn

        except sqlite3.Error as e:
            # Log the error if the connection fails
            self.log_error(f"SQLite connection error: {e}")
            return None

    def prepare_db_conn(self, db):
        """description."""
        try:
            conn = ""
        except Exception as e:
            self.log_error(e)

        return conn

    def auto_map_columns(self, df, column_map):
        """
        Converte os nomes das colunas de um DataFrame de local → web ou web → local,
        baseado nas colunas detectadas no DataFrame.

        Parâmetros:
        - df: pd.DataFrame
        - column_map: dict — mapeamento local → web

        Retorno:
        - DataFrame com colunas renomeadas (ou original, se irrelevante)
        """
        df_cols = set(column_map.keys())
        df_reverse = set(column_map.values())
        current_cols = set(df.columns)

        if df_cols & current_cols:
            # Se contém colunas
            rename_map = column_map
        elif df_reverse & current_cols:
            # Se o mapa está invertido
            rename_map = {v: k for k, v in column_map.items()}
        else:
            # Nenhuma coluna relevante encontrada
            return df

        return df.rename(columns=rename_map)

    def _add_columns_if_not_exist(self, db_filepath, table_name, column_names):
        """Adds multiple columns to the table if they do not already exist.

        Args:
            db_path (str): Path to the SQLite database.
            table_name (str): Name of the table to modify.
            column_names (list): List of column names to add.
        """
        conn = sqlite3.connect(db_filepath)
        cursor = conn.cursor()

        # Retrieve existing column names
        cursor.execute(f"PRAGMA table_info({table_name})")
        existing_columns = {row[1] for row in cursor.fetchall()}  # Use a set for faster lookups

        # Prepare ALTER TABLE statements only for missing columns
        alter_statements = [
            f"ALTER TABLE {table_name} ADD COLUMN '{column}' REAL"
            for column in column_names
            if column not in existing_columns
        ]

        # Execute all ALTER TABLE commands in a single transaction
        if alter_statements:
            for statement in alter_statements:
                cursor.execute(statement)
            conn.commit()

        conn.close()

    def _count_records(self, db_filepath, table_name, query, params):
        """Counts total records in the specified table or query."""
        try:
            with sqlite3.connect(db_filepath) as conn:
                cursor = conn.cursor()
                try:
                    if query:
                        sql_query = query if not params else f"SELECT COUNT(*) FROM ({query})"
                        (cursor.execute(sql_query, params) if params else cursor.execute(sql_query))
                    elif table_name:
                        if params:
                            sql_query = f"SELECT COUNT(*) FROM {table_name} WHERE ticker_code = ?"
                            cursor.execute(sql_query, params)
                        else:
                            sql_query = f"SELECT COUNT(*) FROM {table_name}"
                            cursor.execute(sql_query)
                    total_rows = cursor.fetchone()[0]

                except Exception:
                    total_rows = 0

                return total_rows

        except Exception as e:
            self.log_error(e)

    def _load_data_batches(self, db_filepath, table_name, query, params, total_rows, normalize_columns, alert):
        """Loads data in batches using multi-threading."""
        try:
            batch_size = self.config.scraping["chunk_size"]
            batch_number = (total_rows // batch_size) + 1
            batch_threads = min(self.config.scraping["max_workers"], batch_number)
            offsets = range(0, total_rows, batch_size)

            start_time = time.time()
            with ThreadPoolExecutor(max_workers=batch_threads) as executor:
                tasks = [
                    executor.submit(
                        self._read_batch,
                        db_filepath,
                        table_name,
                        query,
                        params,
                        offset,
                        batch_size,
                        batch_num,
                        total_rows,
                        start_time,
                        alert,
                    )
                    for batch_num, offset in enumerate(offsets)
                ]
                dataframes = [task.result() for task in tasks]

            final_df = pd.concat(dataframes, ignore_index=True) if dataframes else pd.DataFrame()

            if normalize_columns:
                final_df = self._normalize_columns(final_df, normalize_columns)

        except Exception as e:
            final_df = pd.DataFrame()
            self.log_error(e)

        return final_df

    def _read_batch(
        self, db_filepath, table_name, query, params, offset, batch_size, batch_num, total_rows, start_time, alert
    ):
        """Reads a single batch of data."""
        try:
            if alert:
                extra_info = [f"Batch {batch_num + 1}/{(total_rows // batch_size) + 1}"]
                self.print_info(batch_num, (total_rows // batch_size) + 1, start_time, extra_info)

            attempts = 0
            max_retries = self.config.selenium["max_retries"]
            while attempts < max_retries:
                try:
                    with sqlite3.connect(f"file:{db_filepath}?mode=ro", uri=True) as conn:
                        try:
                            sql_query = self._construct_query(table_name, query, params, batch_size, offset)
                            df = pd.read_sql_query(sql_query, conn, params=params)
                        except Exception as e:
                            self.log_error(e)
                        return df
                except Exception as e:
                    if "database is locked" in str(e):
                        attempts += 1
                        time.sleep(self.dynamic_sleep())
                    else:
                        raise
            raise Exception(f"Failed to read batch after {max_retries} attempts.")

        except Exception as e:
            self.log_error(e)

    def load_data(self, table_name=None, query=None, params=None, db_filepath=None, max_retries=None, alert=True):
        """Load data from the SQLite database into a pandas DataFrame using
        multithreading for faster reads.

        Dynamically creates databases and tables if they do not exist.
        """
        try:
            params = params or ()
            max_retries = max_retries or self.config.selenium["max_retries"]
            db_filepath = db_filepath or self.config.databases["raw"]["filepath"]
            database_name = os.path.basename(db_filepath)
            dataframes = []

            columns, dtypes, primary_keys = self._get_table_structure(table_name, database_name)

            # 1. Count total rows
            try:
                # Retry logic for database connection
                attempts = 0
                while attempts < max_retries:
                    # Database Code
                    with self.db_lock:
                        # Ensure the database and table exist
                        self._initialize_database(db_filepath, database_name, table_name)
                        self._configure_db(db_filepath)

                        with self._get_db_connection(db_filepath, read_only=True) as conn:
                            cursor = conn.cursor()
                            try:
                                if table_name:
                                    # columns, dtypes, primary_keys = 
                                    sql_query = f"SELECT COUNT({primary_keys[0]}) FROM {table_name}"
                                    params = ()
                                elif query:
                                    query_count_index = query.upper().find("FROM")
                                    sql_query = f"SELECT COUNT(*) {query[query_count_index:]}"

                                cursor.execute(sql_query, params)
                                total_rows = cursor.fetchone()[0]

                            except Exception:
                                self._initialize_table(db_filepath, database_name, table_name)
                                total_rows = 0
                    break
            except Exception as e:
                self.log_error(e)

            # 2. Load database
            try:
                # Total number of batches
                batch_size = self.config.scraping["chunk_size"]
                batch_number = (total_rows // batch_size) + (1 if total_rows % batch_size > 0 else 0)

                batch_threads = min(self.config.scraping["max_workers"], batch_number)
                offsets = range(0, total_rows, batch_size)

                if batch_number == 0 or batch_threads == 0:
                    schema_str = self.config.schemas[database_name][table_name]
                    df = pd.DataFrame(columns=dtypes.keys()).astype(dtypes)
                    return df  # Nothing to process

                start_time = time.time()

                # Internal method for batch sql reading
                def read_batch(
                    offset, batch_number, table_name, query, params, start_time=start_time, alert=True
                ):
                    if alert:
                        extra_info = [f"Parte {batch_number + 1}/{batch_number}"]
                        self.print_info(batch_number, start_time, extra_info)

                    # Retry logic for database connection
                    attempt = 0
                    while attempt < max_retries:
                        try:
                            # Database Code
                            with self._get_db_connection(db_filepath, read_only=True) as conn:
                                if table_name:
                                    query_batch = f"SELECT * FROM {table_name} LIMIT {batch_size} OFFSET {offset}"
                                elif query:
                                    query_batch = f"{query} LIMIT {batch_size} OFFSET {offset}"
                                query_df = pd.read_sql_query(query_batch, conn, params=params)

                                return query_df

                        except Exception as e:
                            if "database is locked" in str(e):
                                attempt += 1
                                time.sleep(self.dynamic_sleep())
                            else:
                                raise
                    raise Exception(f"Failed to read batch after {max_retries} attempts.")

                # **TQDM Progress Bar Inside Multithreading**
                with ThreadPoolExecutor(max_workers=batch_threads) as executor:
                    tasks = []
                    with tqdm(total=total_rows, unit=" rows", desc=f"{table_name}", leave=False) as pbar:
                        for batch_number, offset in enumerate(offsets):
                            task = executor.submit(
                                read_batch, offset, batch_number, table_name, query, params, alert=False
                            )
                            tasks.append(task)
                            time.sleep(1)

                        # Collect results while updating the progress bar
                        for task in tasks:
                            dataframes.append(task.result())
                            pbar.update(batch_size)  # Update progress bar

                final_df = pd.concat(dataframes, ignore_index=True) if dataframes else pd.DataFrame()

                # if normalize_columns:
                #     for col in normalize_columns:
                #         if col in final_df.columns:
                #             if 'date' in col.lower() or 'time' in col.lower():
                #                 final_df[col] = pd.to_datetime(final_df[col], errors='coerce')
                #             else:
                #                 final_df[col] = pd.to_numeric(final_df[col], errors='coerce').fillna(0)

                return final_df

            except Exception as e:
                if "database is locked" in str(e):
                    attempts += 1
                    time.sleep(self.dynamic_sleep())
                else:
                    raise

        except Exception as e:
            self.log_error(e)
            return pd.DataFrame()  # Return an empty DataFrame in case of failure

    def save_to_db(
        self,
        dataframe,
        table_name=None,
        db_filepath=None,
        alert=True,
        max_retries=None,
        update=True,
        sql_update=None,  # New parameter for optional SQL update
        sql_update_params=None,  # Parameters for the update query
    ):
        """
        Save or update a DataFrame in a SQLite database table.

        Args:
            dataframe (DataFrame): The DataFrame containing data to insert or update.
            table_name (str): The name of the database table.
            db_filepath (str): The SQLite database file path.
            alert (bool): Whether to print update messages.
            max_retries (int): Maximum retry attempts if the database is locked.
            update (bool): Whether to update existing rows on conflict.
            sql_update (str, optional): SQL command to execute instead of inserting data.
            sql_update_params (tuple, optional): Parameters for the `sql_update` statement.
        """
        try:
            db_filepath = db_filepath or self.config.db_filepath
            database_name = os.path.basename(db_filepath)
            max_retries = max_retries or self.config.selenium["max_retries"]

            primary_keys = self._get_primary_key(table_name, database_name)
            columns, dtypes, primary_keys = self._get_table_structure(table_name, database_name)

            # Prepare and Insert Data
            sql = self._get_sql_statement(dataframe, primary_keys, table_name, update=update)

            if sql_update:
                pass
            else:
                dataframe = self._prepare_dataframe(dataframe)
                data_tuples = [tuple(row) for row in dataframe.itertuples(index=False)]

            # Acquire the lock to ensure thread safety
            with self.db_lock, self._get_db_connection(db_filepath, read_only=False) as conn:
                cursor = conn.cursor()

                # Ensure Table Columns Match DataFrame
                cursor.execute(f"PRAGMA table_info({table_name})")
                existing_columns = {row[1] for row in cursor.fetchall()}

                missing_columns = [col for col in dataframe.columns if col not in existing_columns]

                for _, col in enumerate(missing_columns):
                    safe_col = f'"{col}"'  # Ensure proper escaping of column names
                    alter_query = f"ALTER TABLE {table_name} ADD COLUMN {safe_col} REAL;"
                    cursor.execute(alter_query)

                conn.commit()  # Apply table modifications

                attempts = 0

                while attempts < max_retries:
                    try:
                        if sql_update:
                            # If sql_update is provided, execute it instead of inserting data
                            cursor.execute(sql_update, sql_update_params or ())
                            conn.commit()

                            # debug
                            time.sleep(self.dynamic_sleep())
                            row_count = cursor.rowcount

                            verify_sql = f"""
                                SELECT COUNT(*) FROM {table_name} WHERE processed IS NOT NULL AND company_name = ?;
                            """
                            cursor.execute(verify_sql, sql_update_params or ())
                            row_updated = cursor.fetchone()[0]
                            if row_count != row_updated:
                                print(f"DEBUG executed: {row_count}, updated: {row_updated}")
                                print(
                                    f"  SELECT COUNT(*) FROM tbl_statements_raw WHERE processed IS NOT NULL AND company_name = '{sql_update_params[0]}';"
                                )

                        else:
                            # Otherwise, proceed with batch insertion
                            cursor.executemany(sql, data_tuples)
                            conn.commit()

                        if alert:
                            print(f"Updated {table_name} in {database_name}")

                        break  # Exit loop on success

                    except Exception as e:
                        if "database is locked" in str(e):
                            attempts += 1
                            time.sleep(self.dynamic_sleep())
                        else:
                            raise  # Exit on other errors

        except Exception as e:
            print(dataframe.dtypes)
            print(sql)
            dataframe.to_csv("dataframe.csv", index=False)
            self.log_error(f"Error saving to database: {e}")

    def get_columns(self, schema_str):
        """
        Extract column names and corresponding pandas dtypes from a SQL CREATE TABLE string.

        Returns:
            dict: {column_name: pandas_dtype}
        """
        try:
            match = re.search(r"\((.*?)\)\s*(;|\n)", schema_str, re.DOTALL)
            if not match:
                return {}

            column_block = match.group(1)
            column_lines = column_block.splitlines()

            col_type_dict = {}

            for line in column_lines:
                line = line.strip().strip(",")
                if not line or line.upper().startswith("PRIMARY KEY") or line.upper().startswith("FOREIGN KEY"):
                    continue

                parts = line.split()
                if len(parts) < 2:
                    continue  # Skip invalid lines

                col_name = parts[0]
                sql_type = parts[1].upper()

                # SQL to pandas dtype mapping
                if "INT" in sql_type:
                    pandas_type = "Int64"
                elif "REAL" in sql_type or "FLOAT" in sql_type or "DECIMAL" in sql_type:
                    pandas_type = "float"
                elif "TEXT" in sql_type or "CHAR" in sql_type:
                    pandas_type = "string"
                else:
                    pandas_type = "object"  # Default fallback

                col_type_dict[col_name] = pandas_type

            return col_type_dict

        except Exception as e:
            self.log_error(e)
            return {}

    def _get_primary_key(self, table_name, db_filepath):
        """"""
        primary_key = ""
        database_name = os.path.basename(db_filepath)

        try:
            schema = self.config.schemas[database_name][table_name]
            lines = schema.strip().splitlines()

            # Step 2: Initialize Variables for Parsing
            primary_keys = []

            # Step 3: Parse the Lines for PRIMARY KEY
            for line in lines:
                if "PRIMARY KEY" in line.upper():
                    primary_key_index = line.upper().find("PRIMARY KEY")
                    key_part_before = line[:primary_key_index].strip()  # Part before PRIMARY KEY
                    key_part_after = line[primary_key_index + len("PRIMARY KEY") :].strip()  # Part after PRIMARY KEY

                    # Handle keys before PRIMARY KEY
                    if key_part_before:
                        key = key_part_before.split()[0]  # Extract the first part as the key
                        primary_keys.append(key)

                    # Handle keys inside parentheses after PRIMARY KEY
                    if key_part_after.startswith("(") and key_part_after.endswith(")"):
                        keys_in_parentheses = key_part_after[1:-1].split(",")  # Remove parentheses and split keys
                        primary_keys.extend(key.strip() for key in keys_in_parentheses)

            # primary_key = primary_keys[0] if len(primary_keys) == 1 else ",".join(primary_keys)

        except Exception as e:
            self.log_error(e)

        return primary_keys

    def _get_table_structure(self, table_name, db_filepath):
        """
        Extracts all column names, pandas dtypes, and primary key columns from a CREATE TABLE statement.

        Args:
            table_name (str): Name of the table.
            db_filepath (str): Path to the database file.

        Returns:
            tuple: (columns: List[str], dtypes: Dict[str, str], primary_keys: List[str])
        """
        columns = []
        dtypes = {}
        primary_keys = []
        database_name = os.path.basename(db_filepath)

        try:
            schema = self.config.schemas[database_name][table_name]
            lines = schema.strip().splitlines()

            for line in lines:
                line = line.strip()

                if not line:
                    continue

                # Detect PRIMARY KEY
                if "PRIMARY KEY" in line.upper():
                    primary_key_index = line.upper().find("PRIMARY KEY")
                    key_part_before = line[:primary_key_index].strip()
                    key_part_after = line[primary_key_index + len("PRIMARY KEY"):].strip()

                    if key_part_before:
                        key = key_part_before.split()[0]
                        primary_keys.append(key)

                    if key_part_after.startswith("(") and key_part_after.endswith(")"):
                        keys_in_parentheses = key_part_after[1:-1].split(",")
                        primary_keys.extend(key.strip() for key in keys_in_parentheses)

                # Detect column definitions
                elif any(type_hint in line.upper() for type_hint in ["TEXT", "INTEGER", "REAL", "FLOAT", "DECIMAL", "CHAR", "DATE", "BLOB", "BOOLEAN"]):
                    parts = line.replace(",", "").split()
                    if len(parts) >= 2:
                        column_name = parts[0].strip()
                        sql_type = parts[1].strip().upper()

                        columns.append(column_name)
                        dtypes[column_name] = self._map_sql_to_pd_dtype(sql_type)

        except Exception as e:
            self.log_error(e)

        return columns, dtypes, primary_keys

    def _map_sql_to_pd_dtype(self, sql_type):
        """
        Map a SQL data type to the closest equivalent pandas dtype.

        Args:
            sql_type (str): SQL column type.

        Returns:
            str: pandas dtype string.
        """
        try:
            sql_type = sql_type.upper().strip()

            # SQL to pandas dtype mapping
            if "INT" in sql_type:
                pandas_type = "Int64"  # nullable integer
            elif "REAL" in sql_type or "FLOAT" in sql_type or "DOUBLE" in sql_type or "DECIMAL" in sql_type:
                pandas_type = "float64"
            elif "TEXT" in sql_type or "CHAR" in sql_type or "CLOB" in sql_type:
                pandas_type = "string"
            elif "BLOB" in sql_type:
                pandas_type = "object"  # binary/blob is generic object
            elif "BOOLEAN" in sql_type:
                pandas_type = "boolean"
            elif "DATE" in sql_type or "TIME" in sql_type:
                pandas_type = "datetime64[ns]"
            else:
                pandas_type = "object"  # fallback for unknown types

        except Exception as e:
            self.log_error(e)
            pandas_type = "object"

        return pandas_type

    def _get_sql_statement(self, dataframe, primary_keys, table_name, update=True):
        """Generate a SQL statement dynamically, ensuring column names are
        correctly formatted to prevent syntax errors in SQLite.

        Args:
            dataframe (DataFrame): DataFrame containing the data to insert or update.
            primary_keys (list): List of primary key columns to check for conflicts.
            table_name (str): Name of the table where the data will be saved.
            update (bool): Whether to update existing rows on conflict (default is True).

        Returns:
            str: The dynamically generated SQL statement.
        """
        sql = ""
        try:
            # Wrap columns in double quotes if needed (special characters, spaces, or digits at the start)
            def quote_column(col):
                result = f'"{col}"' if not col.isidentifier() or col[0].isdigit() else col
                return result

            # Quote column names and primary keys
            quoted_columns = [quote_column(col) for col in dataframe.columns]
            quoted_primary_keys = [quote_column(col) for col in primary_keys] if primary_keys else []

            # Build the basic fields and values parts of the query
            f_fields = "(" + ", ".join(quoted_columns) + ")"
            f_values = "(" + ", ".join(["?"] * len(dataframe.columns)) + ")"

            # Start building SQL query
            sql = f"INSERT INTO {table_name} {f_fields} VALUES {f_values}"

            # Only add ON CONFLICT if primary keys exist and `update` is True
            if quoted_primary_keys and update:
                f_primary_keys = f"({', '.join(quoted_primary_keys)})"
                sql += f" ON CONFLICT {f_primary_keys}"

                # Create the UPDATE set clause (update all columns except primary keys)
                f_update_set = ", ".join([
                    f"{col} = excluded.{col}" for col in quoted_columns if col not in quoted_primary_keys
                ])

                if f_update_set:
                    sql += f" DO UPDATE SET {f_update_set}"
                else:
                    sql += " DO NOTHING"  # Do nothing if no columns to update

            else:
                # If update is False, use INSERT OR IGNORE to ignore duplicates
                sql = f"INSERT OR IGNORE INTO {table_name} {f_fields} VALUES {f_values}"

        except Exception as e:
            self.log_error(f"Error in SQL statement generation: {e}")

        return sql

    def _prepare_dataframe(self, dataframe):
        """"""
        try:
            text_columns = ["version"]
            date_columns = ["quarter", "sent_date", "date", "ex_date"]
            numeric_columns = ["price_or_factor"]

            # Replace NaN with None for SQLite compatibility
            dataframe = dataframe.where(pd.notnull(dataframe), None)

            # Replace NaN and None with an empty string for text columns
            try:
                for col in text_columns:
                    if col in dataframe.columns:
                        dataframe[col] = dataframe[col].replace([None, ""], "").astype(str)
            except Exception:
                pass

            # Convert datetime columns to string in ISO format or None
            try:
                for col in date_columns:
                    if col in dataframe.columns:
                        # Convert column to datetime safely
                        try:
                            # Attempt to convert the datetime with a stricter format
                            dataframe[col] = pd.to_datetime(dataframe[col], format="%Y-%m-%d", errors="raise")
                        except Exception as e_outer:
                            try:
                                # Handle ISO 8601 format like '2010-12-31T00:00:00'
                                dataframe[col] = pd.to_datetime(dataframe[col], format="ISO8601", errors="raise")
                            except Exception as e_inner:
                                # Fallback to automatic inference of format
                                dataframe[col] = pd.to_datetime(dataframe[col], errors="coerce")
                                print(e_outer, e_inner)

                        # Apply the conversion to ISO format
                        dataframe[col] = dataframe[col].apply(
                            lambda x: (x.isoformat() if isinstance(x, pd.Timestamp) and pd.notna(x) else None)
                        )
            except Exception:
                pass

            # Ensure numeric columns have valid values or are set to None
            try:
                for col in numeric_columns:
                    if col in dataframe.columns:
                        dataframe[col] = dataframe[col].apply(lambda x: float(x) if pd.notna(x) else None)
            except Exception:
                pass

        except Exception as e:
            self.log_error(e)

        return dataframe

    def db_optimize(self, db_filepath=None):
        """Optimize the SQLite database by running VACUUM, ANALYZE, and
        REINDEX.

        Parameters:
            db_filepath (str): The file path to the SQLite database.
        """
        db_filepath = db_filepath or self.config.db_filepath

        try:
            # Acquire the lock to ensure thread safety
            with self.db_lock:
                with self._get_db_connection(db_filepath, read_only=False) as conn:
                    cursor = conn.cursor()

                    # Runs a WAL checkpoint to reduce WAL file size
                    conn.execute("PRAGMA wal_checkpoint(FULL);")

                    # Run ANALYZE to update statistics for query optimization
                    cursor.execute("ANALYZE")

                    # Run REINDEX to rebuild indexes for better performance
                    cursor.execute("REINDEX")

                    # Run VACUUM to reduce file size and defragment the database
                    cursor.execute("VACUUM")

                    conn.commit()

        except sqlite3.Error as e:
            self.log_error(f"An error occurred during database optimization: {e}")

    # OTHER NOT CLASSIFIED YET
    def explode_company(self, company_info):
        def process_ticker_isin(row):
            ticker_codes = json.loads(row["ticker_codes"]) if row["ticker_codes"] else []
            isin_codes = json.loads(row["isin_codes"]) if row["isin_codes"] else []
            ticker_isin = [
                pair for pair in sorted(list(zip(ticker_codes, isin_codes)), key=lambda x: x[0]) if "ACN" in pair[1]
            ]
            return ticker_isin

        company_info["ticker_isin"] = company_info.apply(process_ticker_isin, axis=1)
        mask = company_info["ticker_isin"].apply(lambda x: len(x) > 0)
        company_info = company_info[mask]

        company_info_exploded = company_info.explode("ticker_isin")
        company_info_exploded[["ticker_code", "isin_code"]] = pd.DataFrame(
            company_info_exploded["ticker_isin"].tolist(), index=company_info_exploded.index
        )
        company_info_exploded = company_info_exploded.drop(columns=["ticker_isin"])

        return company_info_exploded


class TemplateProcessor(BaseProcessor):
    """docstrings."""

    def __init__(self):
        """docstrings."""
        super().__init__()
        self.db_lock = Lock()  # Initialize a threading Lock

        # # Initialize the WebDriver
        # self.driver, self.driver_wait = self._initialize_driver()

    def process_instance(self, sub_batch, payload, progress):
        """Process a single batch by delegating from abstract base_processor
        method to this class process_batch (true process info method) via this
        process_instance method (create instance method).

        sub_batch progress

        return result from process_batch
        """
        try:
            extra_info = [f"Worker {progress['thread_id']}", " ".join(sub_batch)]
            self.print_info(progress["batch_index"], progress["total_batches"], progress["start_time"], extra_info)
            # Delegate to process_batch for the actual batch processing
            result = self.process_batch(sub_batch, progress)

        except Exception:
            pass

        return result

    def process_batch(self, sub_batch, payload, progress):
        """"""
        result = ""

        try:
            pass
        except Exception as e:
            self.log_error(e)

        return result

    def main(self, thread=True):
        """docstring."""
        try:
            # Load existing information
            table_name = ""
            db_filepath = ""
            data_1 = self.load_data(table_name=table_name, db_filepath="")
            data_2 = self.load_data()

            # Fetch Scrape targets
            targets = ""

            # if no targets, optimize db and return True
            if targets:
                if targets.size == 0:  # Check if the array is empty
                    self.db_optimize(self.config.databases["raw"]["filepath"])
                    return True

            # Process targets using threading or sequential logic
            processed_batch = self.run(
                targets, thread=thread, module_name=self.inspect.getmodule(self.inspect.currentframe()).__name__
            )

            # save/update db
            if not processed_batch.empty:
                self.save_to_db(
                    dataframe=processed_batch,
                    table_name=self.config.historical_tickers_urls_table,
                    db_filepath=self.config.databases["raw"]["filepath"],
                )

        except Exception as e:
            self.log_error(e)

        return True
