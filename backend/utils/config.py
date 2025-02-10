import os

class Config:
    def __init__(self):
        self.initial_database = 'statements initial.db'
        self.initial_table = 'statements_initial'

        self.standart_database = 'statements standart.db'
        self.standart_table = 'statements_standart'

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
        self.backup_standart_db = f"{self.standart_database.split('.')[0]} {self.backup_name}.{self.standart_database.split('.')[-1]}"

        self.final_filepath = os.path.join(self.data_folder, self.final_database)
        self.backup_final_db = f"{self.final_database.split('.')[0]} {self.backup_name}.{self.final_database.split('.')[-1]}"

        # batches and other numbers
        cpu = os.cpu_count()
        self.batch_size = cpu * 10 # 250  # Batch size for data processing
        cpu_factor = 1
        self.max_workers = int(cpu * cpu_factor) + (1 if (cpu * cpu_factor) % 1 > 0 else 0) # ceil
        self.big_batch_size = int(40000 / self.max_workers)
        self.chunk_size = 100000
        self.stock_data_start_date = '1960-01-01'
        self.update_days = 2
        self.joint = ' - '
        self.joint2 = ' | '
        self.stock_start = '00.'

        # Selenium settings
        self.wait_time = 2  # Wait time for Selenium operations
        self.max_retries = 3
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
        self.company_columns = ['cvm_code', 'company_name', 'ticker', 'ticker_codes', 'isin_codes', 'trading_name', 'sector', 'subsector', 'segment', 'listing', 'activity', 'registrar', 'cnpj', 'website']

        # NSD scraping settings
        self.nsd_table = 'nsd'
        self.nsd_columns = ['nsd', 'company_name', 'quarter', 'version', 'nsd_type', 'dri', 'auditor', 'responsible_auditor', 'protocol', 'sent_date', 'reason']  # Adjusted columns based on NSD data
        self.nsd_order = ['company_name', 'quarter', 'version']
        self.default_daily_submission_estimate = 30
        self.safety_factor = 3  # Apply a safety factor to account for possible increases

        # Statements settings
        self.statements_sheet_columns = ['company_name', 'quarter', 'version', 'type', 'frame']

        self.statements_file = 'statements_initial'
        self.statements_types = ["DEMONSTRACOES FINANCEIRAS PADRONIZADAS", "INFORMACOES TRIMESTRAIS"]
        self.statements_columns_empty_df = ['date', 'nsd', 'sector', 'subsector', 'segment', 'company_name', 'quarter', 'version',]
        self.financial_statements_columns = ['account', 'description', 'value']  # Assuming these are the financial/statements columns
        self.statements_columns = ['nsd', 'sector', 'subsector', 'segment', 'company_name', 'quarter', 'version', 'type', 'frame'] + self.financial_statements_columns
        self.statements_order = ['sector', 'subsector', 'segment', 'company_name', 'quarter', 'account', 'description', 'type', ]
        self.year_end_accounts = ['3', '4']
        self.cumulative_quarter_accounts = ['6', '7']

        # Stock Settings ['Date', 'Close', 'Dividends', 'High', 'Low', 'Open', 'Stock Splits', 'Volume']
        self.historical_stock_data_table = 'stock_data'
        self.historical_stock_data_columns = ['date', 'close', 'high', 'low', 'open', 'volume', 'stock_splits', 'dividends']
        self.historical_stock_data_all_columns = ['company_name', 'ticker', 'ticker_code'] + self.historical_stock_data_columns

        # Math settings
        self.statements_file_math = 'math'

        # Standard settings
        self.statements_standard = 'standard'
        self.statements_index_columns = ['nsd', 'sector', 'subsector', 'segment', 'company_name', 'quarter', 'version']
        self.statements_pivot_columns = ['account', 'description', 'frame', 'type']

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
        self.financial_data_statements = [
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
        self.statements_data_statements = [
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
                    CREATE TABLE IF NOT EXISTS statements_initial (
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
                    CREATE TABLE IF NOT EXISTS statements_standart (
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
      