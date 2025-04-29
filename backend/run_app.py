from utils.base_processor import BaseProcessor
from utils.company_processor import CompanyProcessor
from utils.corporate_events_processor import EventsStatementsProcessor
from utils.intel_processor import IntelProcessor
from utils.nsd_processor import NsdProcessor
from utils.statements_processor import StatementsProcessor

if __name__ == "__main__":
    try:
        base = BaseProcessor()

        # Ask the user if they want to get company information
        run_company_processor = "N"
        prompt = "Want to update company information? (YES/NO): "
        # run_company_processor = base.timed_input(prompt)
        if run_company_processor.strip().upper().startswith("Y"):
            company_processor = CompanyProcessor()
            company_processor.main(thread=True)

        # Ask the user if they want to get nsd information
        run_nsd_processor = "N"
        prompt = "Want to update nsd information? (YES/NO): "
        # run_nsd_processor = base.timed_input(prompt)
        if run_nsd_processor.strip().upper().startswith("Y"):
            nsd_processor = NsdProcessor()
            nsd_processor.main(thread=True)

        # Ask the user if they want to get finantial statements
        run_statements_processor = "N"
        prompt = "Want to update statements information? (YES/NO): "
        run_statements_processor = base.timed_input(prompt)
        if run_statements_processor.strip().upper().startswith("Y"):
            statements_processor = StatementsProcessor()
            statements_processor.main(thread=False)

        # Ask the user if they want to sstandardize the statements
        run_intel_processor = "N"
        prompt = "Want to standardize statements information? (YES/NO): "
        run_statements_processor = base.timed_input(prompt)
        if run_intel_processor.strip().upper().startswith("Y"):
            intel_processor = IntelProcessor()
            intel_processor.main(thread=False)

        # Ask the user if they want to get corporate events from b3
        run_corporate_events_processor = "Y"
        prompt = "Want to update corporate events? (YES/NO): "
        # run_corporate_events_processor = base.timed_input(prompt)
        if run_corporate_events_processor.strip().upper().startswith("Y"):
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
        base.log_error(e)
        pass

    print("done")
