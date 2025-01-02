from utils.company_processor import CompanyProcessor

if __name__ == '__main__':
    try:
        # Ask the user if they want to scrape company information
        company_processor = CompanyProcessor()
        run_company_processor = 'Y'
        prompt = 'Want to scrape company information? (YES/NO): '
        # run_company_processor = company_processor.timed_input(prompt)
        if run_company_processor.strip().upper().startswith('Y'):
            company_processor.main()
        company_processor.close_driver()

    except Exception as e:
        pass

    print('done')