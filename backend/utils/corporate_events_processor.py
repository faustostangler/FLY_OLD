from threading import Lock
import datetime
import time
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import requests
import urllib3
from io import StringIO
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from io import StringIO
import re
import warnings
import ast
import sqlite3
import yfinance as yf
import sys
import io

from utils.base_processor import BaseProcessor

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
with warnings.catch_warnings():
    warnings.filterwarnings("ignore", category=FutureWarning, module='pandas')

class CorporateEventsProcessor(BaseProcessor):
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
            ticker_list = sub_batch['ticker_code']
            extra_info = [f"Worker {progress['thread_id']}", ' '.join(ticker_list)]
            self.print_info(progress['batch_index'], progress['total_batches'], progress['start_time'], extra_info)

            # Delegate to process_batch for the actual batch processing
            result = self.process_batch(sub_batch, progress)

        except Exception as e:
            pass

        return result

    def process_batch(self, sub_batch, progress):
        '''
        '''
        result = pd.DataFrame()
        try:
            data = []
            start_time = time.time()
            for i, row in sub_batch.reset_index().iterrows():
                start_date = row['date']
                company_name = row['company_name']
                ticker = row['ticker']
                ticker_code = row['ticker_code']

                try:
                    # Redirect stderr to silence error messages
                    old_stderr = sys.stderr
                    sys.stderr = io.StringIO()

                    ticker_obj = yf.Ticker(ticker_code + '.SA')
                    historical_data = ticker_obj.history(start=start_date, actions=True, auto_adjust=True).reset_index()

                    # historical_data = yf.download(ticker_code + '.SA', start=start_date, progress=False, actions=True, auto_adjust=True).reset_index()
                    if not historical_data.empty:
                        historical_data.columns = historical_data.columns.str.lower().str.replace(" ", "_")
                        historical_data['date'] = pd.to_datetime(historical_data['date']).dt.strftime('%Y-%m-%d')
                        historical_data['company_name'] = company_name
                        historical_data['ticker'] = ticker
                        historical_data['ticker_code'] = ticker_code
                        historical_data = historical_data[self.config.historical_stock_data_all_columns]
                    else:
                        new_row = {
                            'company_name': company_name,
                            'ticker': ticker,
                            'ticker_code': ticker_code,
                        }
                        historical_data = pd.DataFrame([new_row])

                finally:
                    # Reset stderr to its original state
                    sys.stderr = old_stderr

                data.append(historical_data)

                extra_info = [f'Worker {progress["batch_index"]}', ticker_code, company_name]
                self.print_info(i, len(sub_batch), start_time, extra_info, indent_level=2)

            result = pd.concat(data)

        except Exception as e:
            self.log_error(e)

        self.save_to_db(dataframe=result, table_name=self.config.historical_stock_data_table, db_filepath=self.config.metadados_filepath)

        return result

    def get_scrape_targets(self, company_info, historical_data, statements_company):
        '''
        docstrings
        '''
        historical_data_primary_key_columns = ['company_name', 'ticker', 'ticker_code']
        merging_columns = historical_data_primary_key_columns + ['date']

        try:
            # prepare company_info
            company_info = self.explode_company(company_info)

            # prepare historical_data
            try:
                historical_data['date'] = pd.to_datetime(historical_data['date'])
                # Sort the DataFrame by date in descending order (most recent first)
                historical_data = historical_data.sort_values(by='date', ascending=False)
                # Drop duplicates based on ['company_name', 'ticker', 'ticker_code'], keeping the most recent
                historical_data = historical_data.drop_duplicates(subset=historical_data_primary_key_columns, keep='first')
            except Exception as e:
                company_info['date'] = self.config.stock_data_start_date
                scrape_targets = company_info[merging_columns]
                return scrape_targets

            # get unprocessed companies
            # Merge to find unprocessed companies (companies in `company_info` not in `historical_data`)
            unprocessed_companies = pd.merge(
                company_info,
                historical_data[historical_data_primary_key_columns],
                on=historical_data_primary_key_columns,
                how='left',
                indicator=True
            ).query('_merge == "left_only"').drop(columns=['_merge'])
            unprocessed_companies['date'] = pd.to_datetime('1960-01-01').strftime('%Y-%m-%d')
            unprocessed_companies = unprocessed_companies[merging_columns]

            # Filter rows based on the date threshold
            non_existing_historical_data = historical_data[historical_data['date'].isna()][merging_columns]

            delta = datetime.timedelta(days=self.config.update_days + 1)
            date_diff = datetime.datetime.now() - delta
            processed_companies = historical_data[historical_data['date'] < (date_diff)]
            processed_companies.loc[:, 'date'] = processed_companies['date'].dt.strftime('%Y-%m-%d')
            processed_companies = processed_companies[merging_columns]

            # Combine the datasets
            if not unprocessed_companies.empty:
                scrape_targets = pd.concat([unprocessed_companies, processed_companies], ignore_index=True)
                scrape_targets['date'] = pd.to_datetime(scrape_targets['date'], errors='coerce')
                scrape_targets['date'] = scrape_targets['date'].dt.strftime('%Y-%m-%d')
                scrape_targets = scrape_targets.dropna(subset=['date']).reset_index(drop=True)
            else:
                scrape_targets = processed_companies

        except Exception as e:
            self.log_error(e)
            scrape_targets = pd.DataFrame(columns=merging_columns)

        return scrape_targets

    def main(self, thread=True):
        '''
        docstring
        '''
        try:
            # Load existing information
            company_info = self.load_data(table_name=self.config.company_table, db_filepath=self.config.metadados_filepath)
            historical_data = self.load_data(table_name=self.config.historical_stock_data_table, db_filepath=self.config.metadados_filepath)
            # statements_company = self.load_data(table_name=self.config.initial_table, db_filepath=self.config.initial_filepath)
            statements_company = pd.DataFrame(columns=self.config.statements_columns)

            scrape_targets = self.get_scrape_targets(company_info, historical_data, statements_company)

            # if no scrape_targets, optimize db and return True
            if scrape_targets.size == 0:  # Check if the array is empty
                self.db_optimize(self.config.metadados_filepath)
                return True

            # Process targets using threading or sequential logic
            processed_batch = self.run(scrape_targets, thread=thread, module_name=self.inspect.getmodule(self.inspect.currentframe()).__name__)

            # save/update db
            if not processed_batch.empty:
                self.save_to_db(dataframe=processed_batch, table_name=self.config.historical_stock_data_table, db_filepath=self.config.metadados_filepath)

        except Exception as e:
            self.log_error(e)

        return True

class EventsStatementsProcessor(BaseProcessor):
    '''
    definitions
    '''
    def __init__(self):
        '''
        definitions
        '''
        super().__init__()
        self.db_lock = Lock()  # Initialize a threading Lock

    def get_scrape_targets(self, company_info):
        """
        Obtém os alvos de raspagem de dados para a empresa.

        Esta função expande as informações da empresa e retorna os alvos específicos para a raspagem.

        Parâmetros:
        - company_info (pd.DataFrame): DataFrame contendo informações das empresas.

        Retorna:
        - pd.DataFrame: Um DataFrame contendo os alvos de raspagem.
        """
        try:
            # Expande a lista de empresas e seus respectivos alvos para raspagem
            scrape_targets = self.explode_company(company_info).reset_index(drop=True)

        except Exception as e:
            # Registra qualquer erro ocorrido
            self.log_error(e)
            scrape_targets = pd.DataFrame()

        return scrape_targets

    def get_company_statements(self, company_name):
        """
        Obtém as demonstrações financeiras de uma empresa específica.

        Esta função carrega e processa os dados financeiros padronizados da empresa,
        filtrando apenas os dados mais recentes por trimestre e organizando-os em formato de tabela.

        Parâmetros:
        - company_name (str): Nome da empresa cujas demonstrações financeiras serão extraídas.

        Retorna:
        - statements (pd.DataFrame): DataFrame contendo os dados financeiros pivotados e organizados.
        - company_statements (pd.DataFrame): DataFrame contendo as demonstrações financeiras brutas.
        """
        try:
            # Consulta SQL para buscar demonstrações financeiras de uma empresa específica
            sql_statements_company = '''SELECT * 
                                        FROM statements_standart 
                                        WHERE company_name = ?'''

            statements_company = self.load_data(
                table_name=self.config.standart_table,
                query=sql_statements_company,
                params=(company_name,),
                db_filepath=self.config.standart_filepath,
                alert=False
            )

            # Converte a coluna 'quarter' para datetime para garantir ordenação e filtragem corretas
            statements_company['quarter'] = pd.to_datetime(statements_company['quarter'])

            # Obtém a versão mais recente para cada trimestre
            latest_versions = statements_company.groupby('quarter')['version'].max().reset_index()

            # Filtra os dados para manter apenas as versões mais recentes
            statements_company = statements_company.merge(latest_versions, on=['quarter', 'version'])

        except Exception as e:
            # Retorna um DataFrame vazio em caso de erro e registra o erro
            statements_company = pd.DataFrame()
            # self.log_error(e)

        return statements_company

    def get_statements_quarterly(self, statements):
        '''
        definitions
        '''
        index_columns = self.config.statements_index_columns
        pivot_columns = self.config.statements_pivot_columns
        
        try:
            # Cria uma tabela pivô para organizar os dados financeiros
            statements_quarterly = statements.pivot_table(
                index=index_columns,  # Define as colunas principais como índice
                columns=pivot_columns,  # Define as colunas a serem pivotadas
                values='value',  # Usa os valores financeiros como dados
                aggfunc='first'  # Usa 'first' para lidar com duplicatas
            ).reset_index()

            # Ajusta os nomes das colunas para um formato mais acessível
            statements_quarterly.columns = [
                self.config.joint.join([str(part) for part in col if part]) 
                if isinstance(col, tuple) else col for col in statements_quarterly.columns]

        except Exception as e:
            self.log_error(e)

        return statements_quarterly


    def get_company_stock_data(self, ticker_code=None):
        """
        Obtém os dados históricos de ações de uma empresa específica.

        Esta função carrega os dados de preços históricos das ações de uma empresa com base no código do ticker.

        Parâmetros:
        - ticker_code (str): Código do ticker da empresa.

        Retorna:
        - company_stock_data (pd.DataFrame): DataFrame contendo os dados históricos de ações.
        """
        ticker_code = ticker_code or 'PETR4'
        try:
            # Consulta SQL para obter os dados históricos de ações de um ticker específico de uma empresa específica
            sql_stock_data = '''SELECT *
                                FROM stock_data
                                WHERE ticker_code = ?'''

            company_stock_data = self.load_data(
                table_name=self.config.historical_stock_data_table,
                query=sql_stock_data,
                params=(ticker_code,),
                db_filepath=self.config.metadados_filepath,
                alert=False
            )

            # Remove linhas onde a data esteja ausente
            company_stock_data = company_stock_data.dropna(subset=['date'])

            # Converte a coluna 'date' para formato datetime para garantir consistência
            company_stock_data['date'] = pd.to_datetime(company_stock_data['date'])

            if not company_stock_data.empty:
                splits = company_stock_data[['company_name', 'ticker', 'ticker_code', 'date', 'stock_splits']].query("stock_splits != 0")
                splits['date'] = pd.to_datetime(splits['date'])
            else:
                company_stock_data, splits = self.get_company_stock_data()
                splits = pd.DataFrame(columns=self.config.split_columns)

        except Exception as e:
            # Retorna um DataFrame vazio em caso de erro e registra o erro
            company_stock_data = pd.DataFrame()
            self.log_error(e)

        return company_stock_data, splits

    def parse_statements_daily(self, statements_quarterly, company_stock_data):
        """
        Alinha as demonstrações financeiras trimestrais com os dados diários das ações.

        Esta função expande os dados das demonstrações financeiras trimestrais para corresponder
        às datas dos preços diários das ações, garantindo que os dados financeiros estejam 
        disponíveis para cada dia de negociação.

        Parâmetros:
        - statements (pd.DataFrame): DataFrame contendo demonstrações financeiras trimestrais.
        - company_stock_data (pd.DataFrame): DataFrame contendo dados diários de preços das ações.

        Retorna:
        - pd.DataFrame: Um DataFrame onde cada linha diária está alinhada com a demonstração trimestral mais próxima.
        """
        try:
            # Extrai datas diárias únicas do stock_data para garantir o alinhamento
            daily_dates = company_stock_data[['date']].drop_duplicates()

            # Mescla os dados diários das ações com as demonstrações trimestrais, alinhando as datas dos trimestres
            statements_daily = pd.merge(daily_dates, statements_quarterly, left_on='date', right_on='quarter', how='left')

            # Converte colunas de datas para formato datetime para garantir ordenação e processamento adequados
            statements_daily["date"] = pd.to_datetime(statements_daily["date"], errors='coerce')
            statements_daily["quarter"] = pd.to_datetime(statements_daily["quarter"], errors='coerce')

            # Ordena por data para garantir que o preenchimento posterior aconteça corretamente
            statements_daily = statements_daily.sort_values(by="date")

        except Exception as e:
            # Em caso de erro, retorna um DataFrame vazio e registra o erro
            statements_daily = pd.DataFrame()
            self.log_error(e)

        return statements_daily

    def parse_statements_filled(self, statements_quaterly, statements_daily, splits, columns_to_update):
        """
        Processa e preenche os dados financeiros diários alinhados com os dados das ações.

        Esta função realiza:
        - **Preenchimento de valores ausentes (Backfilling):** Garante que os dados financeiros diários
          carreguem a última informação trimestral disponível.
        - **Ajustes para Desdobramentos de Ações (Stock Splits):** Ajusta os dados financeiros para 
          eventos de desdobramento de ações dentro de cada trimestre.
        - **Controle Trimestral:** Garante que os dados financeiros de cada trimestre sejam preservados
          e corretamente alinhados com os dias de negociação.

        Parâmetros:
        - statements (pd.DataFrame): DataFrame original contendo as demonstrações financeiras trimestrais.
        - statements_daily (pd.DataFrame): DataFrame com os dados financeiros alinhados aos preços diários das ações.
        - splits (pd.DataFrame): DataFrame contendo eventos de desdobramento de ações e suas respectivas datas-ex.
        - columns_to_update (list, opcional): Colunas específicas a serem atualizadas; por padrão, todas as colunas financeiras.

        Retorna:
        - pd.DataFrame: DataFrame preenchido e ajustado para os desdobramentos de ações.
        """
        try:
            # Cria uma cópia dos dados diários para evitar modificar o DataFrame original
            statements_daily_filled = statements_daily.copy()

            # Gera uma coluna que acompanha o último dia do trimestre correspondente para cada data
            statements_daily_filled["date_quarter"] = statements_daily_filled["date"].dt.to_period("Q").dt.end_time.dt.normalize()

            # Extrai os trimestres únicos para iteração
            unique_quarters = statements_daily["quarter"].unique()

            # Dicionário para armazenar os dados processados de cada trimestre
            filled_list = {}
            last_quarter_values = None  # Armazena os últimos valores conhecidos do trimestre anterior

            # Itera sobre cada trimestre único e processa seus dados financeiros
            for quarter in unique_quarters:
                if pd.notna(quarter):  # Garante que o trimestre seja válido (não seja NaT)
                    quarter = pd.Timestamp(quarter).normalize()  # Normaliza para o início do dia

                    # Seleciona as linhas pertencentes ao trimestre atual
                    mask = statements_daily_filled["date_quarter"] == quarter
                    quarter_data = statements_daily_filled.loc[mask].copy()  # Copia para evitar modificar o original

                    # Aplica preenchimento para garantir continuidade dos valores dentro do trimestre
                    quarter_data = quarter_data.bfill()

                    # Identifica os eventos de desdobramento de ações dentro deste trimestre
                    split_mask = (splits["date"] >= quarter_data["date"].min()) & (splits["date"] <= quarter_data["date"].max())
                    quarter_splits = splits.loc[split_mask]

                    # Processa cada evento de desdobramento encontrado no trimestre
                    for _, split_row in quarter_splits.iterrows():
                        split_date = split_row["date"]  # Data em que ocorre o desdobramento (Data-Ex)
                        split_factor = split_row["stock_splits"]  # Fator de multiplicação do ajuste

                        # Identifica as linhas ANTES da data-ex → Aplica os valores do trimestre anterior ou divide
                        before_split_mask = quarter_data["date"] < split_date

                        # Se houver valores conhecidos do trimestre anterior, aplica-os diretamente
                        if last_quarter_values is not None:
                            quarter_data.loc[before_split_mask, columns_to_update] = last_quarter_values.values
                        else:
                            # Se não houver valores anteriores, divide pelo fator de desdobramento como fallback
                            quarter_data.loc[before_split_mask, columns_to_update] = (
                                quarter_data.loc[before_split_mask, columns_to_update] / split_factor
                            ).round(2).astype(int)

                    # Armazena os últimos valores do trimestre atual para usar no próximo trimestre
                    last_quarter_values = quarter_data[columns_to_update].iloc[-1]

                    # Remove a coluna 'date_quarter' antes de consolidar os dados do trimestre
                    quarter_data = quarter_data.drop(columns=["date_quarter"], errors="ignore")

                    # Armazena os dados do trimestre processado no dicionário
                    filled_list[quarter] = quarter_data

            # Concatena todos os dados processados dos trimestres em um único DataFrame
            try:
                result = pd.concat(filled_list).sort_values(by="date").reset_index(drop=True)
            except Exception as e:
                result = pd.DataFrame(columns=self.config.statements_columns_empty_df)

        except Exception as e:
            # Registra qualquer erro e retorna um DataFrame vazio
            self.log_error(e)
            result = pd.DataFrame()

        return result

    def get_items(self, stock_data, stock_splits, statements):
        '''
        definitions
        '''
        try:
            statements_quarterly = self.get_statements_quarterly(statements)

            statements_daily = self.parse_statements_daily(statements_quarterly, stock_data)

            columns_to_update = [col for col in statements_quarterly.columns if col.startswith(self.config.stock_start)]

            items = self.parse_statements_filled(statements_quarterly, statements_daily, stock_splits, columns_to_update=columns_to_update)

        except Exception as e:
            self.log_error(e)

        return items

    def main(self, thread=True):
        '''
        definitions
        '''
        try:
            # load existing company_info data
            company_info = self.load_data(table_name=self.config.company_table, db_filepath=self.config.metadados_filepath)

            scrape_targets = self.get_scrape_targets(company_info)

            statements = {}

            # loop companies, load stock_market data and statements_data per company
            start_time = time.time()
            for i, row in scrape_targets.iterrows():
                ticker = row['ticker']
                ticker_code = row['ticker_code']
                company_name = row['company_name']

                # dados históricos de ações de uma empresa específica filtrada
                stock_data, stock_splits = self.get_company_stock_data(ticker_code)

                # demonstrações financeiras e por quarter de uma empresa específica filtrada
                statements_company = self.get_company_statements(company_name)

                if not statements_company.empty:
                    # get stock items
                    stock_statements = statements_company[statements_company['account'].str.startswith(self.config.stock_start)]
                    stocks = self.get_items(stock_data, stock_splits, stock_statements)

                    ### DFs Individuais and DFs Consolidadas
                    statements[ticker_code] = {}

                    for group_type in ['DFs Individuais', 'DFs Consolidadas']:
                        group_statements = statements_company[statements_company['type'] == group_type]
                        
                        if not group_statements.empty:
                            items = self.get_items(stock_data, stock_splits, group_statements)
                            df = pd.concat([stocks, items], axis=1).loc[:, ~pd.concat([stocks, items], axis=1).columns.duplicated()]
                        else:
                            df = stocks

                        statements[ticker_code][group_type] = df

                else:
                    if ticker_code not in statements:
                        statements[ticker_code] = {}
                    statements[ticker_code]['Demonstrativos Não Encontrados'] = pd.DataFrame()

                extra_info = [i, company_name, ticker_code]
                self.print_info(i, len(scrape_targets), start_time, extra_info)

        except Exception as e:
            self.log_error(e)

        return statements