from threading import Lock

from utils.base_processor import BaseProcessor

class MarketProcessor(BaseProcessor):
    '''
    Financial Statements
    '''
    def __init__(self):
        super().__init__()
        self.db_lock = Lock()  # Initialize a threading Lock

        # Initialize the WebDriver
        # self.driver, self.driver_wait = self._initialize_driver()

    def main(self, thread=True):
        """
        The main method to scrape NSD data, parse it, and save it to the database.
        """
        try:
            statements_data = self.load_data(table_name=self.config.statements_file, db_filepath=self.config.initial_filepath)
            self.save_to_db(dataframe=statements_data[:10000], table_name=self.config.statements_file, db_filepath=r'd:\\Fausto Stangler\\Documentos\\Python\\FLY\\backend\\data\\statements initial market.db')

        except Exception as e:
            self.logerror(e)

        return True