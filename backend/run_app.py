from utils.company_processor import CompanyProcessor

if __name__ == '__main__':
    try:

        company_processor = CompanyProcessor()
        company_processor.main()
        pass
    except Exception as e:
        pass

    print('done')