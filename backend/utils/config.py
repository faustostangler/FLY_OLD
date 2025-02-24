import os

class Config:
    def __init__(self):
        # Carrega todas as sub-configurações
        self.paths = self._define_paths()
        self.domain = self._define_domain_config()
        self.scraping = self._define_scraping_config()
        self.selenium = self._define_selenium_config()
        self.requests = self._define_requests_config()
        self.databases = self._define_database_config()
        self.schemas = self._define_schemas()

        # Cria pastas necessárias
        self._ensure_directories()

    def _define_paths(self):
        """
        Agrupa informações de caminhos e diretórios.
        """
        base_dir = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", "..")
        )
        backend_folder = os.path.join(base_dir, "backend")
        data_folder = os.path.join(backend_folder, "data")
        bin_folder = os.path.join(backend_folder, "bin")
        utils_folder = os.path.join(backend_folder, "utils")

        return {
            "base_dir": base_dir,
            "backend_folder": backend_folder,
            "data_folder": data_folder,
            "bin_folder": bin_folder,
            "utils_folder": utils_folder,
        }

    def _ensure_directories(self):
        """
        Garante que as pastas existam antes de prosseguir.
        """
        os.makedirs(self.paths["backend_folder"], exist_ok=True)
        os.makedirs(self.paths["data_folder"], exist_ok=True)
        os.makedirs(self.paths["bin_folder"], exist_ok=True)
        os.makedirs(self.paths["utils_folder"], exist_ok=True)

        return True

    def _define_database_config(self):
        """
        Define nomes de bancos, tabelas e respectivos caminhos,
        além de backups para cada BD.
        """
        # Nomes dos arquivos de BD
        db_raw = "app_raw.db"
        db_ready = "app_ready.db"

        backup_name = "backup"

        data_folder = self.paths["data_folder"]

        # Table Names
        tbl_company_info = "tbl_company_info"
        tbl_nsd = "tbl_nsd"
        tbl_stock_data = "tbl_stock_data"
        tbl_statements_raw = "tbl_statements_raw"
        tbl_statements_normalized = "tbl_statements_normalized"
        tbl_statements_corp_events = "tbl_statements_corp_events"

        tbl_statements_ready = "tbl_statements_ready"

        # Monta caminhos
        raw_path = os.path.join(data_folder, db_raw)
        ready_path = os.path.join(data_folder, db_ready)

        # Monta nomes de backup
        backup_raw = f"{db_raw.split('.')[0]}_{backup_name}.{db_raw.split('.')[-1]}"
        backup_ready = f"{db_ready.split('.')[0]}_{backup_name}.{db_ready.split('.')[-1]}"

        # Dicionário final de configurações de BD
        return {
            # Banco RAW: Coleta e Transformação
            "raw": {
                "filename": db_raw,
                "filepath": raw_path,
                "backup_filename": backup_raw,
                "tables": {
                    "company_info": tbl_company_info,
                    "nsd": tbl_nsd,
                    "stock_data": tbl_stock_data,
                    "statements_raw": tbl_statements_raw,
                    "statements_normalized": tbl_statements_normalized,
                    "statements_corp_events": tbl_statements_corp_events,
                    # Caso surjam outras tabelas intermediárias, adicionar aqui
                }
            },

            # Banco READY: Visualização
            "ready": {
                "filename": db_ready,
                "filepath": ready_path,
                "backup_filename": backup_ready,
                "tables": {
                    "statements_ready": tbl_statements_ready
                }
            }
        }

    def _define_schemas(self):
        """
        Mapeia cada banco -> cada tabela -> script de criação (CREATE TABLE).
        Usa as variáveis definidas em _define_database_config para manter consistência.
        """

        # Recupera a config de bancos
        db_config = self.databases

        # Extrai as strings de tabela (para não escrever repetidamente no schema)
        tbl_company_info = db_config["raw"]["tables"]["company_info"]           # "company_info"
        tbl_nsd = db_config["raw"]["tables"]["nsd"]                             # "nsd"
        tbl_stock_data = db_config["raw"]["tables"]["stock_data"]               # "stock_data"

        tbl_statements_raw = db_config["raw"]["tables"]["statements_raw"]       # "statements_raw"
        tbl_statements_normalized = db_config["raw"]["tables"]["statements_normalized"] 
        tbl_statements_corp_events = db_config["raw"]["tables"]["statements_corp_events"]

        tbl_statements_ready = db_config["ready"]["tables"]["statements_ready"] # "statements_ready"

        return {
            # Banco raw
            db_config["raw"]["filename"]: {
                tbl_company_info: f"""
                    CREATE TABLE IF NOT EXISTS {tbl_company_info} (
                        cvm_code TEXT,
                        company_name TEXT,
                        ticker TEXT,
                        ticker_codes TEXT,
                        isin_codes TEXT,
                        trading_name TEXT,
                        sector TEXT,
                        subsector TEXT,
                        segment TEXT,
                        listing TEXT,
                        activity TEXT,
                        registrar TEXT,
                        cnpj TEXT,
                        website TEXT, 
                        PRIMARY KEY (company_name)
                    )
                """,
                tbl_nsd: f"""
                    CREATE TABLE IF NOT EXISTS {tbl_nsd} (
                        nsd INTEGER,
                        company_name TEXT,
                        quarter TEXT,
                        version INTEGER,
                        nsd_type TEXT,
                        dri TEXT,
                        auditor TEXT,
                        responsible_auditor TEXT,
                        protocol TEXT,
                        sent_date TEXT,
                        reason TEXT, 
                        PRIMARY KEY (nsd)
                    )
                """,
                tbl_stock_data: f"""
                    CREATE TABLE IF NOT EXISTS {tbl_stock_data} (
                        company_name TEXT,
                        ticker TEXT,
                        ticker_code TEXT,
                        date TEXT,
                        close REAL,
                        dividends REAL,
                        high REAL,
                        low REAL,
                        open REAL,
                        stock_splits INTEGER,
                        volume INTEGER,
                        PRIMARY KEY (company_name, ticker_code, date)
                    )
                """,
                tbl_statements_raw: f"""
                    CREATE TABLE IF NOT EXISTS {tbl_statements_raw} (
                        nsd INTEGER,
                        sector TEXT,
                        subsector TEXT,
                        segment TEXT,
                        company_name TEXT,
                        quarter TEXT,
                        version TEXT,
                        type TEXT,
                        frame TEXT,
                        account TEXT,
                        description TEXT,
                        value REAL,
                        PRIMARY KEY (company_name, quarter, version, type, frame, account, description)
                    )
                """,
                tbl_statements_normalized: f"""
                    CREATE TABLE IF NOT EXISTS {tbl_statements_normalized} (
                        nsd INTEGER,
                        sector TEXT,
                        subsector TEXT,
                        segment TEXT,
                        company_name TEXT,
                        quarter TEXT,
                        version TEXT,
                        type TEXT,
                        frame TEXT,
                        account TEXT,
                        description TEXT,
                        value REAL,
                        PRIMARY KEY (company_name, quarter, version, type, frame, account, description)
                    )
                """,
                tbl_statements_corp_events: f"""
                    CREATE TABLE IF NOT EXISTS {tbl_statements_corp_events} (
                        ticker_code TEXT,
                        group_type TEXT,
                        date TEXT,
                        close REAL,
                        dividends REAL,
                        high REAL,
                        low REAL,
                        open REAL,
                        stock_splits REAL,
                        volume INTEGER,
                        nsd INTEGER,
                        sector TEXT,
                        subsector TEXT,
                        segment TEXT,
                        company_name TEXT,
                        quarter TEXT,
                        version TEXT,
                        PRIMARY KEY (company_name, quarter, version, date, group_type)
                    )
                """,
            },

            # Banco ready
            db_config["ready"]["filename"]: {
                tbl_statements_ready: f"""
                    CREATE TABLE IF NOT EXISTS {tbl_statements_ready} (
                        nsd INTEGER,
                        sector TEXT,
                        subsector TEXT,
                        segment TEXT,
                        company_name TEXT,
                        quarter TEXT,
                        version TEXT,
                        type TEXT,
                        frame TEXT,
                        account TEXT,
                        description TEXT,
                        value REAL,
                        PRIMARY KEY (company_name, quarter, version, type, frame, account, description)
                    )
                """
            }
        }

    def _define_scraping_config(self):
        """
        Configurações gerais de scraping e processamento em lote
        """
        cpu = os.cpu_count() or 1
        batch_size = cpu * 10
        max_workers = cpu * 1 # ou outro cálculo
        chunk_size = 100_000
        stock_data_start_date = "1960-01-01"
        update_days = 2

        return {
            "batch_size": batch_size,
            "max_workers": max_workers,
            "chunk_size": chunk_size,
            "stock_data_start_date": stock_data_start_date,
            "update_days": update_days
        }

    def _define_selenium_config(self):
        """
        Configurações específicas do Selenium
        """
        wait_time = 2
        max_retries = 5

        registry_paths = [
            r'reg query "HKEY_CURRENT_USER\Software\Google\Chrome\BLBeacon" /v version',
            r'reg query "HKEY_LOCAL_MACHINE\Software\Google\Chrome\BLBeacon" /v version',
            r'reg query "HKEY_LOCAL_MACHINE\Software\WOW6432Node\Google\Chrome\BLBeacon" /v version',
        ]
        chrome_path_64 = r"C:\Program Files\Google\Chrome\Application\chrome.exe"
        chrome_path_32 = r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe"

        computer_name = os.environ['COMPUTERNAME']
        chromedriver_path = os.path.join(self.paths["bin_folder"], "chromedriver-win64", "chromedriver.exe")

        return {
            "wait_time": wait_time,
            "max_retries": max_retries,
            "driver": None,
            "driver_wait": None,
            "registry_paths": registry_paths,
            "chrome_path_64": chrome_path_64,
            "chrome_path_32": chrome_path_32,
            "computer_name": computer_name,
            "chromedriver_path": chromedriver_path,
        }

    def _define_requests_config(self):
        """
        Configurações de headers, user-agents, referers etc.
        """
        user_agents = [
            # Chrome (Windows, macOS, Linux, Android)
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.5735.199 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 12_6_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.5735.199 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.5735.199 Safari/537.36",
            "Mozilla/5.0 (Android 13; Mobile; SM-S908B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.5735.199 Mobile Safari/537.36",

            # Firefox (Windows, macOS, Linux, Android)
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:115.0) Gecko/20100101 Firefox/115.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 12_6_1; rv:115.0) Gecko/20100101 Firefox/115.0",
            "Mozilla/5.0 (X11; Linux x86_64; rv:115.0) Gecko/20100101 Firefox/115.0",
            "Mozilla/5.0 (Android 13; Mobile; SM-S908B; rv:115.0) Gecko/115.0 Firefox/115.0",

            # Safari (macOS, iOS)
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 12_6_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Safari/605.1.15",
            "Mozilla/5.0 (iPhone; CPU iPhone OS 16_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Mobile/15E148 Safari/604.1",
            "Mozilla/5.0 (iPad; CPU OS 16_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Mobile/15E148 Safari/604.1",

            # Microsoft Edge (Windows, macOS)
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.5735.199 Safari/537.36 Edg/114.0.1823.82",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 12_6_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.5735.199 Safari/537.36 Edg/114.0.1823.82",

            # Brave (Windows, macOS, Linux)
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.5735.199 Safari/537.36 Brave/1.57.57",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 12_6_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.5735.199 Safari/537.36 Brave/1.57.57",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.5735.199 Safari/537.36 Brave/1.57.57",

            # Opera (Windows, macOS)
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.5735.199 Safari/537.36 OPR/99.0.4788.88",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 12_6_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.5735.199 Safari/537.36 OPR/99.0.4788.88",

            # Samsung Internet (Android)
            "Mozilla/5.0 (Linux; Android 13; SM-S908B) AppleWebKit/537.36 (KHTML, like Gecko) SamsungBrowser/22.0 Chrome/114.0.5735.199 Mobile Safari/537.36",

            # Vivaldi Browser (Desktop and Android)
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.5735.199 Safari/537.36 Vivaldi/6.1.3035.111",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 12_6_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.5735.199 Safari/537.36 Vivaldi/6.1.3035.111",
            "Mozilla/5.0 (Android 13; Mobile; SM-S908B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.5735.199 Safari/537.36 Vivaldi/6.1.3035.111",

            # Yandex Browser (Russia and CIS)
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.5735.199 YaBrowser/23.7.3.652 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 12_6_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.5735.199 YaBrowser/23.7.3.652 Safari/537.36",
            "Mozilla/5.0 (Android 13; Mobile; SM-S908B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.5735.199 YaBrowser/23.7.3.652 Mobile Safari/537.36",

            # Xbox and PlayStation Browsers
            "Mozilla/5.0 (Xbox; U; Windows NT 10.0; WOW64; en-US) AppleWebKit/537.36 (KHTML, like Gecko) Edge/44.18363.8131",
            "Mozilla/5.0 (PlayStation 5; AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.5735.199 Safari/537.36",

            # Samsung Galaxy Tablet (Android)
            "Mozilla/5.0 (Linux; Android 13; SM-T970) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.5735.199 Safari/537.36",

            # Googlebot Mobile and Desktop (SEO Testing)
            "Mozilla/5.0 (Linux; Android 13; SM-S908B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.5735.199 Mobile Safari/537.36 Googlebot/2.1",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.5735.199 Safari/537.36 Googlebot/2.1"
            ]

        referers = [
            'https://www.google.com/',
            'https://www.bing.com/',
            'https://www.duckduckgo.com/',
            'https://www.facebook.com/',
            'https://twitter.com/',
            'https://www.reddit.com/',
            'https://www.youtube.com/',
            'https://www.linkedin.com/',
            'https://www.instagram.com/',
            'https://www.tiktok.com/',
            'https://www.wikipedia.org/',
            'https://www.amazon.com/',
            'https://www.ebay.com/',
            'https://www.alibaba.com/',
            'https://www.github.com/',
            'https://stackoverflow.com/',
            'https://www.quora.com/',
            'https://news.ycombinator.com/',
            'https://www.netflix.com/',
            'https://www.twitch.tv/',
            'https://www.spotify.com/',
            'https://www.medium.com/',
            'https://www.dropbox.com/',
            'https://www.paypal.com/',
            'https://www.apple.com/',
            'https://www.microsoft.com/',
            'https://www.adobe.com/'
            ]

        languages = [
            'en-US;q=1.0',  # English (United States)
            'en-GB;q=0.9',  # English (United Kingdom)
            'es-ES;q=0.9',  # Spanish (Spain)
            'es-MX;q=0.8',  # Spanish (Mexico)
            'fr-FR;q=0.9',  # French (France)
            'de-DE;q=0.9',  # German (Germany)
            'it-IT;q=0.8',  # Italian (Italy)
            'pt-BR;q=0.9',  # Portuguese (Brazil)
            'pt-PT;q=0.8',  # Portuguese (Portugal)
            'ja-JP;q=0.8',  # Japanese
            'zh-CN;q=0.8',  # Chinese (Simplified)
            'zh-TW;q=0.7',  # Chinese (Traditional)
            'ko-KR;q=0.8',  # Korean
            'ru-RU;q=0.9',  # Russian
            'ar-SA;q=0.8',  # Arabic (Saudi Arabia)
            'hi-IN;q=0.8',  # Hindi (India)
            'tr-TR;q=0.8',  # Turkish
            'nl-NL;q=0.8',  # Dutch (Netherlands)
            'sv-SE;q=0.8',  # Swedish (Sweden)
            'pl-PL;q=0.8',  # Polish
            'da-DK;q=0.8',  # Danish (Denmark)
            'no-NO;q=0.8',  # Norwegian
            'cs-CZ;q=0.8',  # Czech (Czech Republic)
            'el-GR;q=0.8',  # Greek
            'th-TH;q=0.8',  # Thai
            'id-ID;q=0.8'   # Indonesian
            ]

        return {
            "user_agents": user_agents,
            "referers": referers,
            "languages": languages,
        }

    def _define_domain_config(self):
        """
        Aqui adicionamos tudo que for específico da sua lógica de negócios:
        - strings de formatação (sep_dash, sep_pipe)
        - prefixos 'stock_prefix'
        - nomes de colunas de tabelas específicas
        - urls para scraping B3
        - definições de statements / infos de NSD
        - etc.
        """

        # Strings de formatação
        sep_dash = " - "
        sep_pipe = " | "
        stock_prefix = "00."
        indent = " " * 2

        # URLs de scraping B3
        companies_url = "https://sistemaswebb3-listados.b3.com.br/listedCompaniesPage/search?language=pt-br"
        company_url = "https://sistemaswebb3-listados.b3.com.br/listedCompaniesPage/?language=pt-br"

        # Colunas colunas de "company_info"
        columns_company_info = [
            "cvm_code", "company_name", "ticker", "ticker_codes", "isin_codes",
            "trading_name", "sector", "subsector", "segment", "listing",
            "activity", "registrar", "cnpj", "website"
        ]

        # NSD scraping settings
        columns_nsd = [
            "nsd", "company_name", "quarter", "version", "nsd_type",
            "dri", "auditor", "responsible_auditor", "protocol",
            "sent_date", "reason"
        ]
        sort_order_nsd = ["company_name", "quarter", "version"]
        default_daily_submission_estimate = 30
        safety_factor = 3

        # Configurações para statements
        statements_sheet_columns = ["company_name", "quarter", "version", "type", "frame"]
        statements_types = ["DEMONSTRACOES FINANCEIRAS PADRONIZADAS", "INFORMACOES TRIMESTRAIS"]
        statements_columns_empty_df = [
            "date", "nsd", "sector", "subsector", "segment",
            "company_name", "quarter", "version"
        ]
        financial_statements_columns = ["account", "description", "value"]
        statements_columns = [
            "nsd", "sector", "subsector", "segment", "company_name",
            "quarter", "version", "type", "frame", "account", "description", "value"
        ]
        statements_order = [
            "sector", "subsector", "segment", "company_name",
            "quarter", "account", "description", "type"
        ]
        accounts_year_end = ["3", "4"]
        accounts_cumulative_quarter = ["6", "7"]

        # Dados de mercado (historical/stock)
        historical_stock_data_columns = ["date", "close", "high", "low", "open", "volume", "stock_splits", "dividends"]
        historical_stock_data_all_columns = ["company_name", "ticker", "ticker_code"] + historical_stock_data_columns

        # Dados “standard”
        statements_index_columns = ["nsd", "sector", "subsector", "segment", "company_name", "quarter", "version"]
        statements_pivot_columns = ["account", "description", "frame", "type"]

        # splits
        split_columns = ["company_name", "ticker", "ticker_code", "date", "stock_splits"]

        # Descrições & contas
        descriptions = {
            "acoes_on": "Ações ON Ordinárias",
            "acoes_pn": "Ações PN Preferenciais",
            "acoes_on_tesouraria": "Em Tesouraria Ações ON Ordinárias",
            "acoes_pn_tesouraria": "Em Tesouraria Ações PN Preferenciais"
        }
        accounts = {
            "acoes_on": "00.01.01",
            "acoes_pn": "00.01.02",
            "acoes_on_tesouraria": "00.02.01",
            "acoes_pn_tesouraria": "00.02.02"
        }

        # Financial and Capital statements
        statements_financial_data = [
            ["DFs Consolidadas", "Demonstração do Resultado"],
            ["DFs Consolidadas", "Balanço Patrimonial Ativo"],
            ["DFs Consolidadas", "Balanço Patrimonial Passivo"],
            ["DFs Consolidadas", "Demonstração do Fluxo de Caixa"],
            ["DFs Consolidadas", "Demonstração de Valor Adicionado"],
            ["DFs Individuais", "Demonstração do Resultado"],
            ["DFs Individuais", "Balanço Patrimonial Ativo"],
            ["DFs Individuais", "Balanço Patrimonial Passivo"],
            ["DFs Individuais", "Demonstração do Fluxo de Caixa"],
            ["DFs Individuais", "Demonstração de Valor Adicionado"],
        ]
        statements_capital_config = [
            ["Dados da Empresa", "Composição do Capital"],
        ]

        # Judicial terms to remove
        words_to_remove = [
            "  EM LIQUIDACAO", " EM LIQUIDACAO", " EXTRAJUDICIAL",
            "  EM RECUPERACAO JUDICIAL", "  EM REC JUDICIAL",
            " EM RECUPERACAO JUDICIAL", " EM LIQUIDACAO EXTRAJUDICIAL", " EMPRESA FALIDA",
        ]

        # Governance levels
        governance_levels = {
            "NM": "Cia. Novo Mercado",
            "N1": "Cia. Nível 1 de Governança Corporativa",
            "N2": "Cia. Nível 2 de Governança Corporativa",
            "MA": "Cia. Bovespa Mais",
            "M2": "Cia. Bovespa Mais Nível 2",
            "MB": "Cia. Balcão Org. Tradicional",
            "DR1": "BDR Nível 1",
            "DR2": "BDR Nível 2",
            "DR3": "BDR Nível 3",
            "DRE": "BDR de ETF",
            "DRN": "BDR Não Patrocinado"
        }

        # Tipos de ações
        tipos_acoes = {
            "1": "Direitos de Subscrição de Ações Ordinárias",
            "2": "Direitos de Subscrição de Ações Preferenciais",
            "3": "Ações Ordinárias (ON)",
            "4": "Ações Preferenciais (PN)",
            "5": "Ações Preferenciais Classe A (PNA)",
            "6": "Ações Preferenciais Classe B (PNB)",
            "7": "Ações Preferenciais Classe C (PNC)",
            "8": "Ações Preferenciais Classe D (PND)",
            "9": "Recibos de Subscrição",
            "10": "BDRs – Brazilian Depositary Receipts",
            "11": "Units (Conjunto de ações ordinárias e preferenciais)",
            "12": "Cotas de Fundos de Investimento Imobiliário (FII)",
            "31": "Direitos de Subscrição de Units",
            "32": "Direitos de Subscrição de BDRs",
            "33": "Recibos de Subscrição de Units",
            "34": "Recibos de Subscrição de BDRs",
            "35": "Recibos de Subscrição de FIIs",
            "39": "Recibos de Subscrição de Outros Valores Mobiliários",
            "41": "Certificados de Depósito de Valores Mobiliários (CVM)",
            "42": "Certificados de Investimento",
            "43": "Cotas de Índices de Ações (ETFs)",
            "44": "Recibos de Subscrição de ETFs",
            "45": "Cotas de Fundos de Índices Estrangeiros (ETFs BDR)",
            "46": "Direitos de Subscrição de Fundos de Índices",
            "47": "Cotas de Fundos de Participação",
            "49": "Cotas de Fundos de Índices (Outros)",
            "50": "Outros Valores Mobiliários",
            "51": "Debêntures Simples",
            "52": "Debêntures Conversíveis",
            "56": "Cotas de Fundos de Investimento em Participações (FIP)",
            "57": "Cotas de Fundos de Investimento em Direitos Creditórios (FIDC)",
            "58": "Cotas de Fundos de Investimento em Ações (FIA)",
            "59": "Cotas de Fundos de Investimento Multimercado",
            "60": "Certificados de Recebíveis Imobiliários (CRI)",
            "61": "Certificados de Recebíveis do Agronegócio (CRA)",
            "62": "Notas Promissórias",
            "63": "Commercial Papers",
            "64": "Cotas de Fundos de Investimento em Ações Estruturadas",
            "66": "Cotas de Fundos de Investimento em Infraestrutura (FIP-IE)",
            "67": "Cotas de Fundos de Investimento em Participação Multiestratégia (FIP-ME)",
            "68": "Cotas de Fundos de Investimento em Participação de Inovação (FIP-PD&I)",
            "71": "Certificados de Operações Estruturadas (COE)",
            "81": "Certificados de Crédito Bancário (CCB)",
            "82": "Letra Financeira (LF)",
            "83": "Letra de Câmbio (LC)",
            "84": "Letra de Crédito Imobiliário (LCI)",
            "85": "Letra de Crédito do Agronegócio (LCA)",
            "86": "Cédula de Crédito Bancário (CCB)",
            "87": "Cédula de Produto Rural (CPR)",
            "88": "Letra Imobiliária Garantida (LIG)",
            "89": "Cotas de Fundos de Investimento em Crédito Privado",
            "90": "Títulos Públicos Federais (Tesouro Direto)",
            "91": "Cotas de Fundos de Investimento em Previdência"
        }

        # Map de tipos de ações
        stock_type_map = {
            "OR": {"code": "3", "description": "Ações Ordinárias (ON)"},
            "PR": {"code": "4", "description": "Ações Preferenciais (PN)"},
            "PA": {"code": "5", "description": "Ações Preferenciais Classe A (PNA)"},
            "PB": {"code": "6", "description": "Ações Preferenciais Classe B (PNB)"},
            "PC": {"code": "7", "description": "Ações Preferenciais Classe C (PNC)"},
            "PD": {"code": "8", "description": "Ações Preferenciais Classe D (PND)"},
            "RS": {"code": "9", "description": "Recibos de Subscrição"},
        }

        return {
            "sep_dash": sep_dash,
            "sep_pipe": sep_pipe,
            "stock_prefix": stock_prefix,
            "indent": indent, 

            "companies_url": companies_url,
            "company_url": company_url,

            "columns_company_info": columns_company_info,

            "columns_nsd": columns_nsd,
            "sort_order_nsd": sort_order_nsd,
            "default_daily_submission_estimate": default_daily_submission_estimate,
            "safety_factor": safety_factor,

            "statements_sheet_columns": statements_sheet_columns,
            "statements_types": statements_types,
            "statements_columns_empty_df": statements_columns_empty_df,
            "financial_statements_columns": financial_statements_columns,
            "statements_columns": statements_columns,
            "statements_order": statements_order,
            "accounts_year_end": accounts_year_end,
            "accounts_cumulative_quarter": accounts_cumulative_quarter,

            "historical_stock_data_columns": historical_stock_data_columns,
            "historical_stock_data_all_columns": historical_stock_data_all_columns,

            "statements_index_columns": statements_index_columns,
            "statements_pivot_columns": statements_pivot_columns,

            "split_columns": split_columns,

            "descriptions": descriptions,
            "accounts": accounts,

            "statements_financial_data": statements_financial_data,
            "statements_capital_config": statements_capital_config,

            "words_to_remove": words_to_remove,
            "governance_levels": governance_levels,
            "tipos_acoes": tipos_acoes,
            "stock_type_map": stock_type_map,
        }
        
        self.initial_database = 'statements initial.db'
        self.initial_table = 'statements_raw'

        self.standart_database = 'statements standart.db'
        self.standart_table = 'statements_normalized'

        self.events_database = 'statements events.db'
        self.events_table = 'statements_corp_events'

        self.final_database = 'statements final.db'
        self.final_table = 'statements_processed'

        #### OLD CONFIG
        # Define the base directory (root of the project)
        self.base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

        # Folder and file configuration
        self.backend_folder = os.path.join(self.base_dir, "backend")
        self.data_folder = os.path.join(self.backend_folder, "data")
        self.bin_folder = os.path.join(self.backend_folder, "bin")
        self.utils_folder = os.path.join(self.backend_folder, "utils")

        # Create necessary directories if they don't exist
        os.makedirs(self.backend_folder, exist_ok=True)
        os.makedirs(self.data_folder, exist_ok=True)
        os.makedirs(self.bin_folder, exist_ok=True)
        os.makedirs(self.utils_folder, exist_ok=True)

        # Main database name
        self.stock_database = "historical_data.db"
        self.metadados_database = "metadados.db"
        self.backup_name = 'backup'

        # Dynamic database names
        self.metadados_filepath = os.path.join(self.data_folder, self.metadados_database)
        self.backup_db = f"{self.metadados_database.split('.')[0]} {self.backup_name}.{self.metadados_database.split('.')[-1]}"

        self.initial_filepath = os.path.join(self.data_folder, self.initial_database)
        self.backup_db = f"{self.initial_database.split('.')[0]} {self.backup_name}.{self.initial_database.split('.')[-1]}"

        self.stock_filepath = os.path.join(self.data_folder, self.stock_database)
        self.backup_db = f"{self.stock_database.split('.')[0]} {self.backup_name}.{self.stock_database.split('.')[-1]}"

        self.standart_filepath = os.path.join(self.data_folder, self.standart_database)
        self.backup_standard_db = f"{self.standart_database.split('.')[0]} {self.backup_name}.{self.standart_database.split('.')[-1]}"

        self.events_filepath = os.path.join(self.data_folder, self.events_database)
        self.backup_events_db = f"{self.events_database.split('.')[0]} {self.backup_name}.{self.events_database.split('.')[-1]}"

        self.final_filepath = os.path.join(self.data_folder, self.final_database)
        self.backup_final_db = f"{self.final_database.split('.')[0]} {self.backup_name}.{self.final_database.split('.')[-1]}"

        # batches and other numbers
        cpu = os.cpu_count()
        self.batch_size = cpu * 10 # 250  # Batch size for data processing
        cpu_factor = 1 # 1
        self.max_workers = int(cpu * cpu_factor) + (1 if (cpu * cpu_factor) % 1 > 0 else 0) # ceil
        self.big_batch_size = int(40000 / self.max_workers)
        self.chunk_size = 100000
        self.stock_data_start_date = '1960-01-01'
        self.update_days = 2
        self.sep_dash = ' - '
        self.sep_pipe = ' | '
        self.stock_prefix = '00.'

        # Selenium settings
        self.wait_time = 2  # Wait time for Selenium operations
        self.max_retries = 5 #3
        self.driver = self.driver_wait = None  # Placeholders for Selenium driver and wait objects
        self.registry_paths = [
            r'reg query "HKEY_CURRENT_USER\Software\Google\Chrome\BLBeacon" /v version',
            r'reg query "HKEY_LOCAL_MACHINE\Software\Google\Chrome\BLBeacon" /v version',
            r'reg query "HKEY_LOCAL_MACHINE\Software\WOW6432Node\Google\Chrome\BLBeacon" /v version'
        ]
        self.chrome_path_64 = r'C:\Program Files\Google\Chrome\Application\chrome.exe'
        self.chrome_path_32 = r'C:\Program Files (x86)\Google\Chrome\Application\chrome.exe'

        # Requests
        self.USER_AGENTS = [
            # Chrome (Windows, macOS, Linux, Android)
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.5735.199 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 12_6_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.5735.199 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.5735.199 Safari/537.36",
            "Mozilla/5.0 (Android 13; Mobile; SM-S908B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.5735.199 Mobile Safari/537.36",

            # Firefox (Windows, macOS, Linux, Android)
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:115.0) Gecko/20100101 Firefox/115.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 12_6_1; rv:115.0) Gecko/20100101 Firefox/115.0",
            "Mozilla/5.0 (X11; Linux x86_64; rv:115.0) Gecko/20100101 Firefox/115.0",
            "Mozilla/5.0 (Android 13; Mobile; SM-S908B; rv:115.0) Gecko/115.0 Firefox/115.0",

            # Safari (macOS, iOS)
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 12_6_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Safari/605.1.15",
            "Mozilla/5.0 (iPhone; CPU iPhone OS 16_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Mobile/15E148 Safari/604.1",
            "Mozilla/5.0 (iPad; CPU OS 16_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Mobile/15E148 Safari/604.1",

            # Microsoft Edge (Windows, macOS)
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.5735.199 Safari/537.36 Edg/114.0.1823.82",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 12_6_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.5735.199 Safari/537.36 Edg/114.0.1823.82",

            # Brave (Windows, macOS, Linux)
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.5735.199 Safari/537.36 Brave/1.57.57",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 12_6_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.5735.199 Safari/537.36 Brave/1.57.57",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.5735.199 Safari/537.36 Brave/1.57.57",

            # Opera (Windows, macOS)
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.5735.199 Safari/537.36 OPR/99.0.4788.88",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 12_6_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.5735.199 Safari/537.36 OPR/99.0.4788.88",

            # Samsung Internet (Android)
            "Mozilla/5.0 (Linux; Android 13; SM-S908B) AppleWebKit/537.36 (KHTML, like Gecko) SamsungBrowser/22.0 Chrome/114.0.5735.199 Mobile Safari/537.36",

            # Vivaldi Browser (Desktop and Android)
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.5735.199 Safari/537.36 Vivaldi/6.1.3035.111",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 12_6_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.5735.199 Safari/537.36 Vivaldi/6.1.3035.111",
            "Mozilla/5.0 (Android 13; Mobile; SM-S908B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.5735.199 Safari/537.36 Vivaldi/6.1.3035.111",

            # Yandex Browser (Russia and CIS)
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.5735.199 YaBrowser/23.7.3.652 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 12_6_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.5735.199 YaBrowser/23.7.3.652 Safari/537.36",
            "Mozilla/5.0 (Android 13; Mobile; SM-S908B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.5735.199 YaBrowser/23.7.3.652 Mobile Safari/537.36",

            # Xbox and PlayStation Browsers
            "Mozilla/5.0 (Xbox; U; Windows NT 10.0; WOW64; en-US) AppleWebKit/537.36 (KHTML, like Gecko) Edge/44.18363.8131",
            "Mozilla/5.0 (PlayStation 5; AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.5735.199 Safari/537.36",

            # Samsung Galaxy Tablet (Android)
            "Mozilla/5.0 (Linux; Android 13; SM-T970) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.5735.199 Safari/537.36",

            # Googlebot Mobile and Desktop (SEO Testing)
            "Mozilla/5.0 (Linux; Android 13; SM-S908B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.5735.199 Mobile Safari/537.36 Googlebot/2.1",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.5735.199 Safari/537.36 Googlebot/2.1"
        ]

        self.REFERERS = [
            'https://www.google.com/',
            'https://www.bing.com/',
            'https://www.duckduckgo.com/',
            'https://www.facebook.com/',
            'https://twitter.com/',
            'https://www.reddit.com/',
            'https://www.youtube.com/',
            'https://www.linkedin.com/',
            'https://www.instagram.com/',
            'https://www.tiktok.com/',
            'https://www.wikipedia.org/',
            'https://www.amazon.com/',
            'https://www.ebay.com/',
            'https://www.alibaba.com/',
            'https://www.github.com/',
            'https://stackoverflow.com/',
            'https://www.quora.com/',
            'https://news.ycombinator.com/',
            'https://www.netflix.com/',
            'https://www.twitch.tv/',
            'https://www.spotify.com/',
            'https://www.medium.com/',
            'https://www.dropbox.com/',
            'https://www.paypal.com/',
            'https://www.apple.com/',
            'https://www.microsoft.com/',
            'https://www.adobe.com/'
        ]

        self.LANGUAGES = [
            'en-US;q=1.0',  # English (United States)
            'en-GB;q=0.9',  # English (United Kingdom)
            'es-ES;q=0.9',  # Spanish (Spain)
            'es-MX;q=0.8',  # Spanish (Mexico)
            'fr-FR;q=0.9',  # French (France)
            'de-DE;q=0.9',  # German (Germany)
            'it-IT;q=0.8',  # Italian (Italy)
            'pt-BR;q=0.9',  # Portuguese (Brazil)
            'pt-PT;q=0.8',  # Portuguese (Portugal)
            'ja-JP;q=0.8',  # Japanese
            'zh-CN;q=0.8',  # Chinese (Simplified)
            'zh-TW;q=0.7',  # Chinese (Traditional)
            'ko-KR;q=0.8',  # Korean
            'ru-RU;q=0.9',  # Russian
            'ar-SA;q=0.8',  # Arabic (Saudi Arabia)
            'hi-IN;q=0.8',  # Hindi (India)
            'tr-TR;q=0.8',  # Turkish
            'nl-NL;q=0.8',  # Dutch (Netherlands)
            'sv-SE;q=0.8',  # Swedish (Sweden)
            'pl-PL;q=0.8',  # Polish
            'da-DK;q=0.8',  # Danish (Denmark)
            'no-NO;q=0.8',  # Norwegian
            'cs-CZ;q=0.8',  # Czech (Czech Republic)
            'el-GR;q=0.8',  # Greek
            'th-TH;q=0.8',  # Thai
            'id-ID;q=0.8'   # Indonesian
        ]

        # Company Info from B3
        self.companies_url = "https://sistemaswebb3-listados.b3.com.br/listedCompaniesPage/search?language=pt-br"  # URL for the B3 companies search page
        self.company_url = "https://sistemaswebb3-listados.b3.com.br/listedCompaniesPage/?language=pt-br"  # URL for the B3 company detail page
        self.company_table = 'company_info'
        self.columns_company_info = ['cvm_code', 'company_name', 'ticker', 'ticker_codes', 'isin_codes', 'trading_name', 'sector', 'subsector', 'segment', 'listing', 'activity', 'registrar', 'cnpj', 'website']

        # NSD scraping settings
        self.nsd_table = 'nsd'
        self.columns_nsd = ['nsd', 'company_name', 'quarter', 'version', 'nsd_type', 'dri', 'auditor', 'responsible_auditor', 'protocol', 'sent_date', 'reason']  # Adjusted columns based on NSD data
        self.sort_order_nsd = ['company_name', 'quarter', 'version']
        self.default_daily_submission_estimate = 30
        self.safety_factor = 3  # Apply a safety factor to account for possible increases

        # Statements settings
        self.statements_sheet_columns = ['company_name', 'quarter', 'version', 'type', 'frame']

        self.statements_raw = 'statements_raw'
        self.statements_types = ["DEMONSTRACOES FINANCEIRAS PADRONIZADAS", "INFORMACOES TRIMESTRAIS"]
        self.statements_columns_empty_df = ['date', 'nsd', 'sector', 'subsector', 'segment', 'company_name', 'quarter', 'version',]
        self.financial_statements_columns = ['account', 'description', 'value']  # Assuming these are the financial/statements columns
        self.statements_columns = ['nsd', 'sector', 'subsector', 'segment', 'company_name', 'quarter', 'version', 'type', 'frame'] + self.financial_statements_columns
        self.statements_order = ['sector', 'subsector', 'segment', 'company_name', 'quarter', 'account', 'description', 'type', ]
        self.accounts_year_end = ['3', '4']
        self.accounts_cumulative_quarter = ['6', '7']

        # Stock Settings ['Date', 'Close', 'Dividends', 'High', 'Low', 'Open', 'Stock Splits', 'Volume']
        self.historical_stock_data_table = 'stock_data'
        self.historical_stock_data_columns = ['date', 'close', 'high', 'low', 'open', 'volume', 'stock_splits', 'dividends']
        self.historical_stock_data_all_columns = ['company_name', 'ticker', 'ticker_code'] + self.historical_stock_data_columns

        # Math settings
        self.statements_raw_math = 'math'

        # Standard settings
        self.statements_normalized = 'standard'
        self.statements_index_columns = ['nsd', 'sector', 'subsector', 'segment', 'company_name', 'quarter', 'version']
        self.statements_pivot_columns = ['account', 'description', 'frame', 'type']

        # corporate events
        self.events_file = 'statements_corp_events'

        # stock_market
        self.markets_file = 'markets'

        # splits
        self.split_columns = ['company_name', 'ticker', 'ticker_code', 'date', 'stock_splits']
        
        # ratios
        self.indicators_file = 'indicators'

        # Descriptions and accounts
        self.descriptions = {
            'acoes_on': 'Ações ON Ordinárias',
            'acoes_pn': 'Ações PN Preferenciais',
            'acoes_on_tesouraria': 'Em Tesouraria Ações ON Ordinárias',
            'acoes_pn_tesouraria': 'Em Tesouraria Ações PN Preferenciais'
        }

        self.accounts = {
            'acoes_on': '00.01.01',
            'acoes_pn': '00.01.02',
            'acoes_on_tesouraria': '00.02.01',
            'acoes_pn_tesouraria': '00.02.02'
        }

        # Financial and Capital Statements from b3 website
        self.statements_financial_data = [
            ['DFs Consolidadas', 'Demonstração do Resultado'], 
            ['DFs Consolidadas', 'Balanço Patrimonial Ativo'], 
            ['DFs Consolidadas', 'Balanço Patrimonial Passivo'], 
            ['DFs Consolidadas', 'Demonstração do Fluxo de Caixa'], 
            ['DFs Consolidadas', 'Demonstração de Valor Adicionado'], 
            ['DFs Individuais', 'Demonstração do Resultado'], 
            ['DFs Individuais', 'Balanço Patrimonial Ativo'], 
            ['DFs Individuais', 'Balanço Patrimonial Passivo'], 
            ['DFs Individuais', 'Demonstração do Fluxo de Caixa'], 
            ['DFs Individuais', 'Demonstração de Valor Adicionado'], 
        ]

        # Capital data configurations
        self.statements_capital_config = [
            ['Dados da Empresa', 'Composição do Capital'], 
        ]

        # List of judicial terms to be removed from company names
        self.words_to_remove = [
            '  EM LIQUIDACAO', ' EM LIQUIDACAO', ' EXTRAJUDICIAL', 
            '  EM RECUPERACAO JUDICIAL', '  EM REC JUDICIAL', 
            ' EM RECUPERACAO JUDICIAL', ' EM LIQUIDACAO EXTRAJUDICIAL', ' EMPRESA FALIDA', 
        ]

        # Dictionary mapping governance level abbreviations to their full descriptions
        self.governance_levels = {
            "NM": "Cia. Novo Mercado",
            "N1": "Cia. Nível 1 de Governança Corporativa",
            "N2": "Cia. Nível 2 de Governança Corporativa",
            "MA": "Cia. Bovespa Mais",
            "M2": "Cia. Bovespa Mais Nível 2",
            "MB": "Cia. Balcão Org. Tradicional",
            "DR1": "BDR Nível 1",
            "DR2": "BDR Nível 2",
            "DR3": "BDR Nível 3",
            "DRE": "BDR de ETF",
            "DRN": "BDR Não Patrocinado"
        }


        self.tipos_acoes = {
            '1': 'Direitos de Subscrição de Ações Ordinárias',
            '2': 'Direitos de Subscrição de Ações Preferenciais',
            '3': 'Ações Ordinárias (ON)',
            '4': 'Ações Preferenciais (PN)',
            '5': 'Ações Preferenciais Classe A (PNA)',
            '6': 'Ações Preferenciais Classe B (PNB)',
            '7': 'Ações Preferenciais Classe C (PNC)',
            '8': 'Ações Preferenciais Classe D (PND)',
            '9': 'Recibos de Subscrição',
            '10': 'BDRs – Brazilian Depositary Receipts',
            '11': 'Units (Conjunto de ações ordinárias e preferenciais)',
            '12': 'Cotas de Fundos de Investimento Imobiliário (FII)',
            '31': 'Direitos de Subscrição de Units',
            '32': 'Direitos de Subscrição de BDRs',
            '33': 'Recibos de Subscrição de Units',
            '34': 'Recibos de Subscrição de BDRs',
            '35': 'Recibos de Subscrição de FIIs',
            '39': 'Recibos de Subscrição de Outros Valores Mobiliários',
            '41': 'Certificados de Depósito de Valores Mobiliários (CVM)',
            '42': 'Certificados de Investimento',
            '43': 'Cotas de Índices de Ações (ETFs)',
            '44': 'Recibos de Subscrição de ETFs',
            '45': 'Cotas de Fundos de Índices Estrangeiros (ETFs BDR)',
            '46': 'Direitos de Subscrição de Fundos de Índices',
            '47': 'Cotas de Fundos de Participação',
            '49': 'Cotas de Fundos de Índices (Outros)',
            '50': 'Outros Valores Mobiliários',
            '51': 'Debêntures Simples',
            '52': 'Debêntures Conversíveis',
            '56': 'Cotas de Fundos de Investimento em Participações (FIP)',
            '57': 'Cotas de Fundos de Investimento em Direitos Creditórios (FIDC)',
            '58': 'Cotas de Fundos de Investimento em Ações (FIA)',
            '59': 'Cotas de Fundos de Investimento Multimercado',
            '60': 'Certificados de Recebíveis Imobiliários (CRI)',
            '61': 'Certificados de Recebíveis do Agronegócio (CRA)',
            '62': 'Notas Promissórias',
            '63': 'Commercial Papers',
            '64': 'Cotas de Fundos de Investimento em Ações Estruturadas',
            '66': 'Cotas de Fundos de Investimento em Infraestrutura (FIP-IE)',
            '67': 'Cotas de Fundos de Investimento em Participação Multiestratégia (FIP-ME)',
            '68': 'Cotas de Fundos de Investimento em Participação de Inovação (FIP-PD&I)',
            '71': 'Certificados de Operações Estruturadas (COE)',
            '81': 'Certificados de Crédito Bancário (CCB)',
            '82': 'Letra Financeira (LF)',
            '83': 'Letra de Câmbio (LC)',
            '84': 'Letra de Crédito Imobiliário (LCI)',
            '85': 'Letra de Crédito do Agronegócio (LCA)',
            '86': 'Cédula de Crédito Bancário (CCB)',
            '87': 'Cédula de Produto Rural (CPR)',
            '88': 'Letra Imobiliária Garantida (LIG)',
            '89': 'Cotas de Fundos de Investimento em Crédito Privado',
            '90': 'Títulos Públicos Federais (Tesouro Direto)',
            '91': 'Cotas de Fundos de Investimento em Previdência'
        }

        # Create a mapping dictionary for stock types
        self.stock_type_map = {
            'OR': {'code': '3', 'description': 'Ações Ordinárias (ON)'},
            'PR': {'code': '4', 'description': 'Ações Preferenciais (PN)'},
            'PA': {'code': '5', 'description': 'Ações Preferenciais Classe A (PNA)'},
            'PB': {'code': '6', 'description': 'Ações Preferenciais Classe B (PNB)'},
            'PC': {'code': '7', 'description': 'Ações Preferenciais Classe C (PNC)'},
            'PD': {'code': '8', 'description': 'Ações Preferenciais Classe D (PND)'},
            'RS': {'code': '9', 'description': 'Recibos de Subscrição'},
        }

        # App database schemas
        self.schema_definitions = {
            self.metadados_database: {
                self.company_table: """
                    CREATE TABLE IF NOT EXISTS company_info (
                        cvm_code TEXT,
                        company_name TEXT PRIMARY KEY,
                        ticker TEXT,
                        ticker_codes TEXT,
                        isin_codes TEXT,
                        trading_name TEXT,
                        sector TEXT,
                        subsector TEXT,
                        segment TEXT,
                        listing TEXT,
                        activity TEXT,
                        registrar TEXT,
                        cnpj TEXT,
                        website TEXT
                    )
                """,
                self.nsd_table: """
                    CREATE TABLE IF NOT EXISTS nsd (
                        nsd INTEGER PRIMARY KEY,
                        company_name TEXT,
                        quarter TEXT,
                        version INTEGER,
                        nsd_type TEXT,
                        dri TEXT,
                        auditor TEXT,
                        responsible_auditor TEXT,
                        protocol TEXT,
                        sent_date TEXT,
                        reason TEXT
                    )
                """, 
                self.historical_stock_data_table: """
                    CREATE TABLE stock_data (
                        company_name TEXT,
                        ticker TEXT,
                        ticker_code TEXT,
                        date TEXT,
                        close REAL,
                        dividends REAL,
                        high REAL,
                        low REAL,
                        open REAL,
                        stock_splits INTEGER,
                        volume INTEGER,
                        PRIMARY KEY (company_name, ticker_code, date)
                    )
                """
            },
            self.initial_database: {
                self.initial_table: """
                    CREATE TABLE IF NOT EXISTS statements_raw (
                        nsd INTEGER,
                        sector TEXT,
                        subsector TEXT,
                        segment TEXT,
                        company_name TEXT,
                        quarter TEXT,
                        version TEXT,
                        type TEXT,
                        frame TEXT,
                        account TEXT,
                        description TEXT,
                        value REAL,
                        PRIMARY KEY (company_name, quarter, version, type, frame, account, description)
                    )
                """
            },
            self.standart_database: {
                self.standart_table: """
                    CREATE TABLE IF NOT EXISTS statements_normalized (
                        nsd INTEGER,
                        sector TEXT,
                        subsector TEXT,
                        segment TEXT,
                        company_name TEXT,
                        quarter TEXT,
                        version TEXT,
                        type TEXT,
                        frame TEXT,
                        account TEXT,
                        description TEXT,
                        value REAL,
                        PRIMARY KEY (company_name, quarter, version, type, frame, account, description)
                    )
                """
            },
            self.events_database: {
                self.events_table: """
                    CREATE TABLE IF NOT EXISTS statements_corp_events (
                        ticker_code TEXT,
                        group_type TEXT,
                        date TEXT,
                        close REAL,
                        dividends REAL,
                        high REAL,
                        low REAL,
                        open REAL,
                        stock_splits REAL,
                        volume INTEGER,
                        nsd INTEGER,
                        sector TEXT,
                        subsector TEXT,
                        segment TEXT,
                        company_name TEXT,
                        quarter TEXT,
                        version TEXT, 
                        PRIMARY KEY (company_name, quarter, version, date, group_type)
                    )
                """
            }, 
            self.final_database: {
                self.final_table: """
                    CREATE TABLE IF NOT EXISTS statements_processed (
                        nsd INTEGER,
                        sector TEXT,
                        subsector TEXT,
                        segment TEXT,
                        company_name TEXT,
                        quarter TEXT,
                        version TEXT,
                        type TEXT,
                        frame TEXT,
                        account TEXT,
                        description TEXT,
                        value REAL,
                        PRIMARY KEY (company_name, quarter, version, type, frame, account, description)
                    )
                """
            },
        }
        