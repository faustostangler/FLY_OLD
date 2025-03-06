from utils.company_processor import CompanyProcessor
from utils.nsd_processor import NsdProcessor
from utils.statements_processor import StatementsProcessor
from utils.intel_processor import IntelProcessor
from utils.corporate_events_processor import EventsStatementsProcessor

from utils.historical_stock_url_processor import HistoricalStockUrlProcessor

if __name__ == '__main__':
    try:
        # Ask the user if they want to get company information
        run_company_processor = 'N'
        prompt = 'Want to update company information? (YES/NO): '
        # run_company_processor = company_processor.timed_input(prompt)
        if run_company_processor.strip().upper().startswith('Y'):
            company_processor = CompanyProcessor()
            company_processor.close_driver()
            company_processor.main(thread=True)
            company_processor.close_driver()

        # Ask the user if they want to get nsd information
        run_nsd_processor = 'N'
        prompt = 'Want to update nsd information? (YES/NO): '
        # run_nsd_processor = nsd_processor.timed_input(prompt)
        if run_nsd_processor.strip().upper().startswith('Y'):
            nsd_processor = NsdProcessor()
            nsd_processor.main(thread=True)

        # Ask the user if they want to get finantial statements
        run_statements_processor = 'N'
        prompt = 'Want to update statements information? (YES/NO): '
        # run_statements_processor = statements_processor.timed_input(prompt)
        if run_statements_processor.strip().upper().startswith('Y'):
            statements_processor = StatementsProcessor()
            statements_processor.close_driver()
            statements_processor.main(thread=True)
            statements_processor.close_driver()

        # Ask the user if they want to sstandardize the statements
        run_intel_processor = 'Y'
        prompt = 'Want to standardize statements information? (YES/NO): '
        # run_statements_processor = statements_processor.timed_input(prompt)
        if run_intel_processor.strip().upper().startswith('Y'):
            intel_processor = IntelProcessor()
            intel_processor.main(thread=True)

        # Ask the user if they want to get corporate events from b3
        run_corporate_events_processor = 'Y'
        prompt = 'Want to update corporate events? (YES/NO): '
        # run_corporate_events_processor = corporate_events_processor.timed_input(prompt)
        if run_corporate_events_processor.strip().upper().startswith('Y'):
            # corporate_events_processor = CorporateEventsProcessor()
            # corporate_events_processor.main(thread=True)
            events_states_processor = EventsStatementsProcessor()
            events_states_processor.main(thread=False)

            # corporate_events_merger = CorporateEventsMerger()
            # corporate_events_merger.main(thread=False)


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