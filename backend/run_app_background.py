from utils.company_processor import CompanyProcessor
from utils.nsd_processor import NsdProcessor
from utils.statements_processor import StatementsProcessor
from utils.historical_stock_url_processor import HistoricalStockUrlProcessor
from utils.corporate_events_processor import CorporateEventsProcessor

if __name__ == '__main__':
    try:
        # Ask the user if they want to get company information
        run_company_processor = 'N'
        prompt = 'Want to update company information? (YES/NO): '
        # run_company_processor = company_processor.timed_input(prompt)
        if run_company_processor.strip().upper().startswith('Y'):
            company_processor = CompanyProcessor()
            company_processor.main(thread=True)
            company_processor.close_driver()

        # Ask the user if they want to get nsd information
        run_nsd_processor = 'N'
        prompt = 'Want to update nsd information? (YES/NO): '
        # run_nsd_processor = nsd_processor.timed_input(prompt)
        if run_nsd_processor.strip().upper().startswith('Y'):
            nsd_processor = NsdProcessor()
            nsd_processor.main(thread=True)
            nsd_processor.close_driver()

        # Ask the user if they want to get finantial statements
        run_statements_processor = 'N'
        prompt = 'Want to update statements information? (YES/NO): '
        # run_statements_processor = statements_processor.timed_input(prompt)
        if run_statements_processor.strip().upper().startswith('Y'):
            statements_processor = StatementsProcessor()
            statements_processor.close_driver()
            statements_processor.main(thread=True)
            statements_processor.close_driver()

        # Ask the user if they want to get finantial historical stock data from b3
        run_historical_stock_url_processor = 'N'
        prompt = 'Want to update historical stock market data? (YES/NO): '
        # run_historical_stock_url_processor = historical_stock_url_processor.timed_input(prompt)
        if run_historical_stock_url_processor.strip().upper().startswith('Y'):
            historical_stock_url_processor = HistoricalStockUrlProcessor()
            historical_stock_url_processor.main(thread=True)
            historical_stock_url_processor.close_driver()

        # Ask the user if they want to get corporate events from b3
        run_corporate_events_processor = 'Y'
        prompt = 'Want to update corporate events? (YES/NO): '
        # run_corporate_events_processor = corporate_events_processor.timed_input(prompt)
        if run_corporate_events_processor.strip().upper().startswith('Y'):
            corporate_events_processor = CorporateEventsProcessor()
            corporate_events_processor.close_driver()
            corporate_events_processor.main(thread=False)
            corporate_events_processor.close_driver()



        # # Ask the user if they want to get finantial historical market data direct form b3 source
        # stock_processor = StockProcessor()
        # run_stock_processor = 'Y'
        # prompt = 'Want to update stock historical data? (YES/NO): '
        # # run_stock_processor = stock_processor.timed_input(prompt)
        # if run_stock_processor.strip().upper().startswith('Y'):
        #     stock_processor.main(thread=True)
        # stock_processor.close_driver()




    except Exception as e:
        pass

    print('done')