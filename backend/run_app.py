from utils.company_processor import CompanyProcessor
from utils.nsd_processor import NsdProcessor
from utils.statements_processor import StatementsProcessor
from utils.market_processor import MarketProcessor
from utils.stock_processor import StockProcessor

if __name__ == '__main__':
    try:
        # Ask the user if they want to get company information
        company_processor = CompanyProcessor()
        run_company_processor = 'N'
        prompt = 'Want to update company information? (YES/NO): '
        # run_company_processor = company_processor.timed_input(prompt)
        if run_company_processor.strip().upper().startswith('Y'):
            company_processor.main()
        company_processor.close_driver()

        # Ask the user if they want to get nsd information
        nsd_processor = NsdProcessor()
        run_nsd_processor = 'N'
        prompt = 'Want to update nsd information? (YES/NO): '
        # run_nsd_processor = nsd_processor.timed_input(prompt)
        if run_nsd_processor.strip().upper().startswith('Y'):
            nsd_processor.main(thread=True)
        nsd_processor.close_driver()

        # Ask the user if they want to get finantial statements
        statements_processor = StatementsProcessor()
        run_statements_processor = 'N'
        prompt = 'Want to update statements information? (YES/NO): '
        # run_statements_processor = statements_processor.timed_input(prompt)
        if run_statements_processor.strip().upper().startswith('Y'):
            statements_processor.main(thread=True)
        statements_processor.close_driver()

        # Ask the user if they want to get finantial historical market data from yfinance
        market_processor = MarketProcessor()
        run_market_processor = 'N'
        prompt = 'Want to update markets historical data? (YES/NO): '
        # run_market_processor = market_processor.timed_input(prompt)
        if run_market_processor.strip().upper().startswith('Y'):
            market_processor.main(thread=True)
        market_processor.close_driver()

        # Ask the user if they want to get finantial historical market data direct form b3 source
        stock_processor = StockProcessor()
        run_stock_processor = 'Y'
        prompt = 'Want to update stock historical data? (YES/NO): '
        # run_stock_processor = stock_processor.timed_input(prompt)
        if run_stock_processor.strip().upper().startswith('Y'):
            stock_processor.main(thread=True)
        stock_processor.close_driver()




    except Exception as e:
        pass

    print('done')