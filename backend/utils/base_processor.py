import os
import pandas as pd
import logging
import inspect
import platform
from datetime import datetime
import time
import string
import unidecode
import re
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, StaleElementReferenceException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.keys import Keys
import random
import sqlite3
import subprocess
from bs4 import BeautifulSoup
from selenium import webdriver
from threading import Lock
import zipfile
import requests
import threading
import pyautogui
import inspect
import warnings
import urllib3
import json

from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor, as_completed

from utils.config import Config

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
with warnings.catch_warnings():
    warnings.filterwarnings("ignore", category=FutureWarning, module='pandas')
class BaseProcessor:
    def __init__(self):
        self.inspect = inspect
        self.config = Config()  # Assume Config is already defined
        self.db_lock = Lock()  # Initialize a threading Lock

    # APP FLOW LOGIC
    def run(self, data, thread=True, module_name=''):
        """
        Split data into batches and process them sequentially or with threads.
        """
        results = []
        try:
            if 'utils.intel_processor' in module_name:
                batches = self._split_batches_by_company(data, self.config.max_workers)
            else: 
                batches = self._split_batches(data, self.config.max_workers)

            if thread:
                print(f'From {module_name.split(".")[-1]}: processing {data.shape[0]} items in {self.config.batch_size} batches of up to {1+int(data.shape[0]/self.config.max_workers)} items each throught {self.config.max_workers} simultaneous workers')
                results = self._process_with_threads(batches)
            else:
                print(f'From {module_name.split(".")[-1]}: processing {data.shape[0]} items in {self.config.max_workers} batches of up to {1+int(data.shape[0]/self.config.max_workers)} items each')
                results = self._process_sequentially(batches)

        except Exception as e:
            self.log_error(e)

        try:
            processed_batch = pd.concat(results, ignore_index=True) if results else pd.DataFrame()
        except Exception as e:
            # self.log_error(e)
            try:
                # try to flatten
                flat_results = [df for sublist in results for df in sublist]
                processed_batch = pd.concat(flat_results, ignore_index=True)
            except Exception as e:
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
            batches = [data[i * chunk_size:(i + 1) * chunk_size] for i in range(batch_size)]

        except Exception as e:
            self.log_error(e)

        return batches
    
    def _split_batches_by_company(self, data, num_workers):
        '''
        docstring
        '''
        batches = []
        try:
            # Get unique company names
            unique_companies = data['company_name'].unique()
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
                batch_data = data[data['company_name'].isin(batch_companies)]
                batches.append(batch_data)
                start = end

        except Exception as e:
            self.log_error(e)

        return batches

    def _process_with_threads(self, batches):
        """
        Process batches with threading.
        """
        results = []
        try:
            total_batches = len(batches)
            start_time = time.time()
            total_scrape_size = sum(len(b) for b in batches)

            with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
                futures = []
                for batch_index, batch in enumerate(batches):
                    progress = {
                        'batch_index': batch_index,
                        'total_batches': total_batches,
                        'batch_start': batch_index * self.config.batch_size,
                        'scrape_size': total_scrape_size, 
                        'start_time': start_time,
                        'thread_id': batch_index % self.config.max_workers,  # Map batch index to thread pool ID
                    }

                    # Submit task with progress
                    futures.append(executor.submit(self.process_instance, batch, progress))

                for future in as_completed(futures):
                    try:
                        result = future.result()
                        results.append(result)
                    except Exception as e:
                        self.log_error(f"Error in thread: {e}")
        except Exception as e:
            self.log_error(e)

        return results

    def _process_sequentially(self, batches):
        '''
        '''
        results = []
        start_time = time.time()
        total_batches = len(batches)
        total_scrape_size = sum(len(b) for b in batches)

        for batch_index, batch in enumerate(batches):
            # Prepare progress dictionary
            progress = {
                'batch_index': batch_index,
                'total_batches': total_batches,
                'batch_start': batch_index * self.config.batch_size,
                'scrape_size': total_scrape_size, 
                'start_time': start_time,
                'thread_id': batch_index % self.config.max_workers,  # Map batch index to thread pool ID
            }

            try:
                result = self.process_instance(batch, progress)
                results.append(result)
            except Exception as e:
                self.log_error(f"Error in batch: {e}")
        
        return results

    @abstractmethod
    def process_instance(self, batch, progress):
        """To be implemented by child classes."""
        pass

    # SELENIUM DRIVER METHODS
    def _get_chrome_version(self):
        """
        Retrieve the version of Chrome installed on the system.

        Returns:
            str: The Chrome version, or None if not found.
        """
        chrome_error_msg = 'Failed to retrieve Chrome version: {e}'

        for reg_query in self.config.registry_paths:
            try:
                output = subprocess.check_output(reg_query, shell=True)
                version = re.search(r'\d+\.\d+\.\d+\.\d+', output.decode('utf-8')).group(0)
                return version
            except subprocess.CalledProcessError:
                continue

        try:
            chrome_path = self.config.chrome_path_64 if os.path.exists(self.config.chrome_path_64) else self.config.chrome_path_32
            output = subprocess.check_output([chrome_path, '--version'], shell=True)
            version = re.search(r'\d+\.\d+\.\d+\.\d+', output.decode('utf-8')).group(0)
            return version

        except Exception as e:
            self.system.log_error(chrome_error_msg.format(e=e))
            return None

    def _get_chromedriver_url(self, version):
        """
        Generate the download URL for ChromeDriver based on the Chrome version.

        Args:
            version (str): The Chrome version.

        Returns:
            str: The URL for downloading the corresponding ChromeDriver.
        """
        chromedriver_url_template = f'https://storage.googleapis.com/chrome-for-testing-public/{version}/win64/chromedriver-win64.zip'
        url_error_msg = f'Error obtaining ChromeDriver for version {version}'

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
        """
        Download and extract ChromeDriver from the given URL.

        Args:
            url (str): The URL for downloading ChromeDriver.
            dest_folder (Path): The destination folder for extraction.

        Returns:
            str: The path to the extracted ChromeDriver executable.
        """
        zip_filename = 'chromedriver.zip'
        dest_folder = self.config.bin_folder
        chromedriver_folder = 'chromedriver-win64'
        chromedriver_executable = 'chromedriver.exe'
        download_error_msg = 'Failed to download or extract ChromeDriver: {e}'

        try:
            self.test_internet()
            response = requests.get(url)
            zip_path = os.path.join(dest_folder, zip_filename)

            with open(zip_path, 'wb') as file:
                file.write(response.content)

            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(dest_folder)

            os.remove(zip_path)
            chromedriver_path = os.path.join(dest_folder, chromedriver_folder, chromedriver_executable)

            return str(chromedriver_path)

        except Exception as e:
            self.log_error(download_error_msg.format(e=e))
            return None

    def _get_chromedriver_path(self):
        """
        Download and extract the ChromeDriver based on the Chrome version installed on the system.

        Returns:
            str: The path to the ChromeDriver executable.
        """
        chrome_version_error_msg = 'Unable to determine Chrome version.'
        chromedriver_url_error_msg = 'Unable to determine the correct ChromeDriver URL.'
        path_error_msg = 'Failed to obtain ChromeDriver path dynamically.'

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

    def _load_driver(self, chromedriver_path):
        """
        Initialize and return the Selenium WebDriver and WebDriverWait instances.

        Args:
            chromedriver_path (str): The path to the ChromeDriver executable.

        Returns:
            tuple: A tuple containing the WebDriver and WebDriverWait instances.
        """
        load_driver_error_msg = 'Failed to load driver: {e}'

        try:
            # Get random headers using the custom function
            headers = self.header_random()

            chrome_service = Service(chromedriver_path)
            chrome_options = Options()
            chrome_options.add_argument(f"user-agent={headers['User-Agent']}")
            chrome_options.add_argument('--window-size=960,540')
            chrome_options.add_argument('--ignore-certificate-errors')
            chrome_options.add_argument('--log-level=3')
            chrome_options.add_argument('--ignore-ssl-errors')
            chrome_options.add_argument('--disable-infobars')
            # chrome_options.add_argument('--headless')

            driver = webdriver.Chrome(service=chrome_service, options=chrome_options)
            exceptions_ignore = (NoSuchElementException, StaleElementReferenceException)
            driver_wait = WebDriverWait(driver, self.config.wait_time, ignored_exceptions=exceptions_ignore)

            return driver, driver_wait

        except Exception as e:
            self.log_error(load_driver_error_msg.format(e=e))
            return None, None

    def _initialize_driver(self):
        """
        Obtain the Selenium WebDriver and WebDriverWait instances.

        This function either uses a predefined path to ChromeDriver or fetches and loads it dynamically.

        Returns:
            tuple: A tuple containing the WebDriver and WebDriverWait instances.
        """
        # https://googlechromelabs.github.io/chrome-for-testing/#stable
        computer_name = os.environ['COMPUTERNAME']
        chromedriver_path = os.path.join(self.config.backend_folder, r'bin\chromedriver-win64\chromedriver.exe')
        initialize_driver_error_msg = 'Failed to load driver from hardcoded path.'
        dynamic_driver_error_msg = 'Failed to obtain ChromeDriver path dynamically.'

        try:
            driver, driver_wait = self._load_driver(chromedriver_path)
            if driver is not None:
                return driver, driver_wait
            else:
                raise Exception(initialize_driver_error_msg)

        except Exception as initial_error:
            try:
                chromedriver_path = self._get_chromedriver_path()
                if not chromedriver_path:
                    raise Exception(dynamic_driver_error_msg)

                driver, driver_wait = self._load_driver(chromedriver_path)
                return driver, driver_wait

            except Exception as dynamic_error:
                self.log_error(str(dynamic_error))
                return None, None
    
    def close_driver(self):
        """
        Safely quits the Selenium WebDriver instance.
        """
        try:
            if self.driver:
                self.driver.quit()
        except Exception as e:
            pass

    # TEXT & SELENIUM OBJECT METHODS
    def clean_text(self, text):
        """
        Cleans and normalizes the input text by removing punctuation, converting to uppercase,
        removing extra whitespace, and eliminating specific words.

        Parameters:
        - text (str): The input text to be cleaned.

        Returns:
        str: The cleaned and normalized text.
        """
        try:
            # Remove punctuation, accents, and normalize case
            translation_table = str.maketrans('', '', string.punctuation)
            text = unidecode.unidecode(text).translate(translation_table).upper().strip()
            text = re.sub(r'\s+', ' ', text)

            # Regular expression pattern to remove specific words from text
            words_to_remove = '|'.join(map(re.escape, self.config.words_to_remove))
            pattern = r'\b(?:' + words_to_remove + r')\b'
            text = re.sub(pattern, '', text)

            # Remove extra spaces after word removal
            text = re.sub(r'\s+', ' ', text).strip()

        except Exception as e:
            self.log_error(e)
        
        return text

    def text(self, xpath, driver=None, driver_wait=None):
        """
        Encontra e recupera o texto de um elemento da web usando o xpath e o objeto de espera fornecido.

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
            return ''

    def click(self, xpath, driver=None, driver_wait=None):
        """
        Encontra e clica em um elemento da web usando o xpath e o objeto de espera fornecido.

        Parameters:
        - xpath (str): O xpath do elemento para clicar.
        - driver_wait (WebDriverWait): O objeto de espera para encontrar o elemento.

        Returns:
        bool: True se o elemento foi encontrado e clicado, False caso contrário.
        """
        driver = driver or self.driver
        driver_wait = driver_wait or self.driver_wait

        try:
            element = self.wait_forever(driver_wait, xpath)

            time.sleep(self.config.wait_time/10)
            element.click()
            time.sleep(self.config.wait_time/10)

            return True
        except Exception as e:
            self.log_error(e)
            return False

    def choose(self, xpath, driver=None, driver_wait=None):
        """
        Encontra e seleciona um elemento da web usando o xpath e o objeto de espera fornecido.

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
            return ''

    def choose_by_value(self, xpath, value, driver=None, driver_wait=None):
        """
        Encontra e seleciona um elemento da web usando o xpath e o objeto de espera fornecido.

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
            self.click('/html/body')
            return value
        except Exception as e:
            self.log_error(e)
            return ''

    def get_options(self, xpath, driver=None, driver_wait=None):
        """
        Encontra e retorna os elementos da web usando o xpath e o objeto de espera fornecido.

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
        except Exception as e:
            # self.log_error(e)
            return ''

    def select(self, xpath, text, driver=None, driver_wait=None):
        '''
        '''
        driver = driver or self.driver
        driver_wait = driver_wait or self.driver_wait
        select = ''

        try:

            element = self.wait_forever(driver_wait, xpath, max_attempts=3)
            select = Select(driver.find_element(By.XPATH, xpath))
            select.select_by_visible_text(text)
        except Exception as e:
            self.log_error(e)

        return select

    def raw_text(self, xpath, driver=None, driver_wait=None):
        """
        Encontra e recupera o HTML bruto de um elemento da web usando o xpath e o objeto de espera fornecido.

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
            return ''

    def wait_forever(self, driver_wait, xpath, max_retries=None):
        """
        Espera indefinidamente até que o elemento da web localizado pelo xpath seja encontrado.

        Parameters:
        - driver_wait (WebDriverWait): O objeto de espera para usar.
        - xpath (str): O xpath do elemento para esperar.

        Returns:
        WebElement: O elemento da web encontrado.
        """
        attempt = 0
        max_retries = max_retries or self.config.max_retries 
        while True:
            try:
                element = driver_wait.until(EC.presence_of_element_located((By.XPATH, xpath)))
                return element
            except Exception as e:
                attempt += 1
                if max_retries and attempt >= max_retries:
                    raise TimeoutException(f"Element with xpath '{xpath}' not found after {max_retries} attempts.") from e
                time.sleep(self.config.wait_time)

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
        """
        Escape special characters in a list of keywords for regex operations.

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

    # APP METHODS
    def prefill_input(self, text, delay=None):
        """
        Simulate typing the default text into the input field.
        
        Parameters:
        - text (str): The text to pre-fill in the input.
        - delay (int): The delay before typing starts.
        """
        try:
            if delay == None:
                delay = self.config.wait_time

            time.sleep(delay)
            pyautogui.typewrite(text)
            
        except Exception as e:
            self.log_error(e)

    def timed_input(self, prompt, timeout=None, default='YES'):
        """
        Display a prompt and return the user's input, with a timeout and pre-filled value.
        
        Parameters:
        - prompt (str): The input prompt to display.
        - timeout (int): The number of seconds to wait for input.
        - default (str): The default value to return if timeout is reached.
        
        Returns:
        - str: The user's input or the default value if timeout is reached.
        """
        try:
            if timeout == None:
                timeout = self.config.wait_time

            prefill_thread = threading.Thread(target=self.prefill_input, args=(default,))
            prefill_thread.start()

            print(f'{prompt} (default: {default}) [You have {timeout} seconds to answer]: ', end='', flush=True)
            
            # Start a thread to run the input() call, which will block until the user provides input
            input_thread = threading.Thread(target=lambda: input())
            input_thread.start()
            
            # Wait for the input or timeout
            input_thread.join(timeout=timeout)
            
            if input_thread.is_alive():
                # print(f'\nNo input provided within {timeout} seconds. Using default: {default}')
                return default
            else:
                return input()
        except Exception as e:
            self.log_error(e)

    # LOG & DEBUG METHODS
    def log_error(self, error):
        """
        Logs an error to a file with detailed context, including caller info, 
        module, function, line number, timestamp, and system information.
        """
        try:
            # Get the current frame and the caller frame
            current_frame = inspect.currentframe()
            caller_frame = current_frame.f_back

            # Gather detailed context information
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            function_name = caller_frame.f_code.co_name
            line_number = caller_frame.f_lineno
            module_name = caller_frame.f_globals["__name__"]
            system_info = platform.platform()
            caller_name = caller_frame.f_globals["__name__"]

            # Configure logging settings
            logging.basicConfig(
                filename='app_errors.log',
                level=logging.ERROR,
                format='%(asctime)s - %(levelname)s - %(message)s'
            )

            # Detailed log message without stack trace
            log_message = (
                f"Timestamp: {timestamp}\n"
                f"Error in module '{module_name}', function '{function_name}', line {line_number}\n"
                f"Caller: {caller_name}\n"
                f"Error: {error}\n"
                f"System Info: {system_info}\n"
            )

            # Log the error message to the file
            logging.error(log_message)

            # Print a simplified error message to the console
            print(f"Error in {function_name} (line {line_number}): {error}")

        except Exception as e:
            print(e)

        return error

    def print_info(self, index=0, size=1, start_time=time.time(), extra_info=[], indent_level=0):
        """
        Prints the provided information along with the progress, elapsed time, 
        estimated remaining time, and total estimated time.
        """
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
            remaining_time_formatted = f"{int(remaining_hours)}h {int(remaining_minutes):02}m {int(remaining_seconds):02}s"

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
            indent = " " * 2 * (indent_level + 1)
            extra_info_str = " ".join(map(str, extra_info))
            print(f"{indent}{progress} {extra_info_str}")

        except Exception as e:
            self.log_error(e)
            pass
    
    def winbeep(frequency=5000, duration=50):
        """
        Generates a system beep sound with the specified frequency and duration.

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
        """
        Ensure the database and table exist, creating them if necessary.
        """
        try:
            if not os.path.exists(db_filepath):
                # print(f"Database '{database_name}' does not exist. Creating...")
                with sqlite3.connect(db_filepath) as conn:
                    pass  # Create the database file if it doesn't exist

                if table_name:
                    self._initialize_table(db_filepath, database_name, table_name)
        except Exception as e:
            self.log_error(e)

    def _initialize_table(self, db_filepath, database_name, table_name):
        """
        Ensure the specified table exists, creating it if necessary.
        """
        try:
            schema_definitions = self.config.schema_definitions.get(database_name, {})
            for schema_table_name, schema_sql in schema_definitions.items():
                # Handle dynamic table names for sectors
                if schema_table_name in table_name or schema_table_name == table_name:
                    with sqlite3.connect(db_filepath) as conn:
                        cursor = conn.cursor()
                        cursor.execute(schema_sql.format(table_name=table_name))
                        conn.commit()
                        # print(f"Table '{table_name}' initialized in '{database_name}'.")
                    return
            # print(f"Warning: No schema defined for table '{table_name}' in database '{database_name}'.")
        except Exception as e:
            self.log_error(e)

    def prepare_db_conn(self, db):
        conn = ''
        return conn

    def load_data(self, table_name=None, query=None, params=None, normalize_columns=None, db_filepath=None):
        """
        Load data from the SQLite database into a pandas DataFrame using multithreading for faster reads.
        Dynamically creates databases and tables if they do not exist.
        """
        db_filepath = db_filepath or self.config.metadados_filepath
        database_name = os.path.basename(db_filepath)
        primary_keys = self._get_primary_key(table_name, database_name)
        dataframes = []

        with self.db_lock:
            try:
                # Ensure the database and table exist
                self._initialize_database(db_filepath, database_name, table_name)

                # Connect to the database and count total rows
                with sqlite3.connect(db_filepath) as conn:
                    cursor = conn.cursor()
                    if table_name:
                        try:
                            if query:
                                # Adjust query to count rows based on filters
                                count_query = f"SELECT COUNT(*) FROM ({query})"
                                cursor.execute(count_query, params)
                            else:
                                # Default to counting all rows in the table
                                cursor.execute(f"SELECT COUNT({primary_keys[0]}) FROM {table_name}")
                            total_rows = cursor.fetchone()[0]

                        except Exception as e:
                            self._initialize_table(db_filepath, database_name, table_name)
                            total_rows = 0

                    batch_size = self.config.chunk_size
                    number_of_batches = (total_rows // batch_size) + 1
                    num_threads = min(self.config.max_workers, number_of_batches)  # Use fewer threads if less data
                    offsets = range(0, total_rows, batch_size)

                    start_time = time.time()  # Start time for tracking progress

                    # Define the worker function for reading batches
                    def read_batch(offset, batch_number):
                        extra_info = [f"Parte {batch_number + 1}/{number_of_batches}", f"{database_name}", f"{table_name}"]
                        self.print_info(batch_number, number_of_batches, start_time, extra_info)
                        with sqlite3.connect(f"file:{db_filepath}?mode=ro", uri=True) as conn:
                            if query:
                                paginated_query = f"{query} LIMIT {batch_size} OFFSET {offset}"
                                return pd.read_sql_query(paginated_query, conn, params=params)
                            elif table_name:
                                paginated_query = f"SELECT * FROM {table_name} LIMIT {batch_size} OFFSET {offset}"
                                return pd.read_sql_query(paginated_query, conn)
                            return pd.DataFrame()

                    # Read data using multithreading
                    with ThreadPoolExecutor(max_workers=num_threads) as executor:
                        tasks = []
                        for batch_number, offset in enumerate(offsets):
                            task = executor.submit(read_batch, offset, batch_number)
                            tasks.append(task)
                            time.sleep(1)

                        dataframes = [task.result() for task in tasks]

                    # Concatenate all the dataframes
                    if dataframes:
                        final_df = pd.concat(dataframes, ignore_index=True)
                    else:
                        final_df = pd.DataFrame()

                    # Normalize columns if specified
                    if normalize_columns:
                        for col in normalize_columns:
                            if col in final_df.columns:
                                if 'date' in col.lower() or 'time' in col.lower():
                                    final_df[col] = pd.to_datetime(final_df[col], errors='coerce')
                                else:
                                    final_df[col] = pd.to_numeric(final_df[col], errors='coerce').fillna(0)

                    return final_df

            except Exception as e:
                self.log_error(e)

    def save_to_db(self, dataframe, table_name=None, db_filepath=None, alert=True):
        """
        Save or update a DataFrame in a SQLite database table.

        Args:
            table_name (str): Name of the table where the data will be saved.
            dataframe (DataFrame): DataFrame containing the data to save.
            db_filepath (str): Path to the database file. Defaults to self.config.db_filepath.
            primary_key (str): The column used to identify unique rows in the table.
        """
        try:
            db_filepath = db_filepath or self.config.db_filepath
            database_name = os.path.basename(db_filepath)

            primary_keys = self._get_primary_key(table_name, database_name)

            # Acquire the lock to ensure thread safety
            with self.db_lock:
                with sqlite3.connect(db_filepath) as conn:
                    conn.execute("PRAGMA journal_mode=WAL;")
                    cursor = conn.cursor()

                    sql = self._get_sql_statement(dataframe, primary_keys, table_name)

                    dataframe = self._prepare_dataframe(dataframe)

                    # Convert the DataFrame to a list of tuples for executemany
                    data_tuples = [tuple(row) for row in dataframe.itertuples(index=False)]

                    # Execute the batch operation
                    cursor.executemany(sql, data_tuples)

                    conn.commit()
            if alert:
                print(f'Saved {database_name}')

        except Exception as e:
            print(dataframe.dtypes)
            print(sql)
            dataframe.to_csv('dataframe.csv', index=False)
            self.log_error(f"Error saving to database: {e}")

    def _get_primary_key(self, table_name, database_name):
        """
        """
        primary_key = ''

        try:
            schema = self.config.schema_definitions.get(database_name, {}).get(table_name, "")
            lines = schema.strip().splitlines()

            # Step 2: Initialize Variables for Parsing
            primary_keys = []

            # Step 3: Parse the Lines for PRIMARY KEY
            for line in lines:
                if "PRIMARY KEY" in line.upper():
                    primary_key_index = line.upper().find("PRIMARY KEY")
                    key_part_before = line[:primary_key_index].strip()  # Part before PRIMARY KEY
                    key_part_after = line[primary_key_index + len("PRIMARY KEY"):].strip()  # Part after PRIMARY KEY

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

    def _get_sql_statement(self, dataframe, primary_keys, table_name):
        """
        """
        sql = ''
        try:
            f_fields = '(' + ', '.join([f for f in dataframe.columns]) + ')'
            f_values = '(' + ', '.join(['?'] * len(dataframe.columns)) + ')'
            f_primary_keys = f"({', '.join(primary_keys)})"
            f_update_set = ', '.join([f"{f} = excluded.{f}" for f in dataframe.columns if f not in primary_keys])

            # Construção do SQL
            sql = 'INSERT INTO ' + table_name + ' '
            sql += f_fields
            sql += ' VALUES ' + f_values
            sql += ' ON CONFLICT ' + f_primary_keys
            if f_update_set:
                sql += ' DO UPDATE SET ' + f_update_set
            else:
                sql += ' DO NOTHING'
        except Exception as e:
            pass

        return sql

    def _prepare_dataframe(self, dataframe):
        """
        """
        try:
            text_columns = ['version']
            date_columns = ['quarter', 'sent_date', 'date', 'ex_date']
            numeric_columns = ['price_or_factor']

            # Replace NaN with None for SQLite compatibility
            dataframe = dataframe.where(pd.notnull(dataframe), None)

            # Replace NaN and None with an empty string for text columns
            try:
                for col in text_columns:
                    if col in dataframe.columns:
                        dataframe[col] = dataframe[col].replace([None, ''], '').astype(str)
            except Exception as e:
                pass

            # Convert datetime columns to string in ISO format or None
            try:
                for col in date_columns:
                    if col in dataframe.columns:
                        # Convert column to datetime safely
                        try:
                            # Attempt to convert the datetime with a stricter format
                            dataframe[col] = pd.to_datetime(dataframe[col], format='%Y-%m-%d', errors='raise')
                        except Exception as e_outer:
                            try:
                                # Handle ISO 8601 format like '2010-12-31T00:00:00'
                                dataframe[col] = pd.to_datetime(dataframe[col], format='ISO8601', errors='raise')
                            except Exception as e_inner:
                                # Fallback to automatic inference of format
                                dataframe[col] = pd.to_datetime(dataframe[col], errors='coerce')
                                self.print(e_outer, e_inner)

                        # Apply the conversion to ISO format
                        dataframe[col] = dataframe[col].apply(
                            lambda x: x.isoformat() if isinstance(x, pd.Timestamp) and pd.notna(x) else None
                        )
            except Exception as e:
                pass

            # Ensure numeric columns have valid values or are set to None
            try:
                for col in numeric_columns:
                    if col in dataframe.columns:
                        dataframe[col] = dataframe[col].apply(lambda x: float(x) if pd.notna(x) else None)
            except Exception as e:
                pass

        except Exception as e:
            self.log_error(e)

        return dataframe
    
    def db_optimize(self, db_filepath=None):
        """
        Optimize the SQLite database by running VACUUM, ANALYZE, and REINDEX.

        Parameters:
            db_filepath (str): The file path to the SQLite database.
        """
        db_filepath = db_filepath or self.config.db_filepath

        try:
            # Acquire the lock to ensure thread safety
            with self.db_lock:
                with sqlite3.connect(db_filepath) as conn:
                    cursor = conn.cursor()

                    # Run VACUUM to reduce file size and defragment the database
                    cursor.execute("VACUUM")

                    # Run ANALYZE to update statistics for query optimization
                    cursor.execute("ANALYZE")

                    # Run REINDEX to rebuild indexes for better performance
                    cursor.execute("REINDEX")

                    conn.commit()

        except sqlite3.Error as e:
            self.log_error(f"An error occurred during database optimization: {e}")

    # WEB & REQUESTS
    def header_random(self):
        """Generate random HTTP headers for requests."""
        user_agent = random.choice(self.config.USER_AGENTS)
        referer = random.choice(self.config.REFERERS)
        language = random.choice(self.config.LANGUAGES)

        headers = {
            'User-Agent': user_agent,
            'Referer': referer,
            'Accept-Language': language
        }

        return headers

    def test_internet(self, wait_time=None, url="https://www.google.com/favicon.ico"):
        """
        Test internet connection by sending an HTTP GET request to a specified URL. 
        Retries if no connection is detected.
        
        Parameters:
            url (str): The URL to request (default: Google's favicon URL).
        """
        if not wait_time:
            wait_time = self.config.wait_time  # Time to wait before retrying on failure
        

        while True:
            try:
                # Make a lightweight GET request
                response = requests.get(url, timeout=wait_time)  # Timeout in seconds
                if response.status_code == 200:
                    return True  # Connection is successful
            except requests.RequestException as e:
                # Log the error or suppress if preferred
                # print(f"No Internet connection: {e}. Retrying in {wait_time} seconds...")
                pass
            time.sleep(wait_time)  # Wait before retrying

    # OTHER NOT CLASSIFIED YET
    def explode_company(self, company_info):

        def process_ticker_isin(row):
            ticker_codes = json.loads(row['ticker_codes']) if row['ticker_codes'] else []
            isin_codes = json.loads(row['isin_codes']) if row['isin_codes'] else []
            ticker_isin = [pair for pair in sorted(list(zip(ticker_codes, isin_codes)), key=lambda x: x[0]) if 'ACN' in pair[1]]
            return ticker_isin

        company_info['ticker_isin'] = company_info.apply(process_ticker_isin, axis=1)
        mask = company_info['ticker_isin'].apply(lambda x: len(x) > 0)
        company_info = company_info[mask]

        company_info = company_info.explode('ticker_isin')
        company_info[['ticker_code', 'isin_code']] = pd.DataFrame(company_info['ticker_isin'].tolist(), index=company_info.index)
        company_info = company_info.drop(columns=['ticker_isin'])

        return company_info
class TemplateProcessor(BaseProcessor):
    '''
    docstrings
    '''
    def __init__(self):
        '''
        docstrings
        '''
        super().__init__()
        self.db_lock = Lock()  # Initialize a threading Lock

        # # Initialize the WebDriver
        # self.driver, self.driver_wait = self._initialize_driver()

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
            extra_info = [f"Worker {progress['thread_id']}", ' '.join(sub_batch)]
            self.print_info(progress['batch_index'], progress['total_batches'], progress['start_time'], extra_info)
            # Delegate to process_batch for the actual batch processing
            result = self.process_batch(sub_batch, progress)

        except Exception as e:
            pass

        return result

    def process_batch(self, sub_batch, progress):
        '''
        '''
        result = ''

        try:
            pass
        except Exception as e:
            self.log_error(e)

        return result

    def main(self, thread=True):
        '''
        docstring
        '''
        try:
            # Load existing information
            table_name = ''
            db_filepath = ''
            data_1 = self.load_data(table_name=table_name, db_filepath='')
            data_2 = self.load_data()

            # Fetch Scrape targets
            scrape_targets = ''

            # if no scrape_targets, optimize db and return True
            if scrape_targets:
                if scrape_targets.size == 0:  # Check if the array is empty
                    self.db_optimize(self.config.metadados_filepath)
                    return True

            # Process targets using threading or sequential logic
            processed_batch = self.run(scrape_targets, thread=thread, module_name=self.inspect.getmodule(self.inspect.currentframe()).__name__)

            # save/update db
            if not processed_batch.empty:
                self.save_to_db(dataframe=processed_batch, table_name=self.config.historical_tickers_urls_table, db_filepath=self.config.metadados_filepath)

        except Exception as e:
            self.log_error(e)

        return True
