# ====================================================================================================
# Set of functions to communicate with SalesForce through the simple-salesforce python wrapper
#
# Documentation:
#     - simple-salesforce       : https://simple-salesforce.readthedocs.io/en/latest/
#     - salesforce (soql/sosl)  : https://developer.salesforce.com/docs/atlas.en-us.238.0.soql_sosl.meta/soql_sosl/sforce_api_calls_soql_sosl_intro.htm
#     - salesforce (objects)    : https://developer.salesforce.com/docs/atlas.en-us.238.0.object_reference.meta/object_reference/sforce_api_objects_concepts.htm
#
# Developed by @Zapata: rl-zapata.github.io
# ====================================================================================================
import json
import logging
import pandas as pd

from simple_salesforce import Salesforce

class SimpleSF:
    # ¡--- Sign-in to salesforce with the given credentials ---!
    # - https://simple-salesforce.readthedocs.io/en/latest/user_guide/examples.html
    # - Requires: the path of the json file that contains the necessary login credentials
    def __init__(self, credentials_path):
        with open(credentials_path, 'r') as credentials_file:
            credentials_items = json.load(credentials_file)
        logging.critical('API [simple-salesforce][aux]: Credentials read successfully')
        
        self.credentials = Salesforce(
            username        = credentials_items['user_name'],
            password        = credentials_items['user_password'],
            security_token  = credentials_items['security_token']
        )
        logging.critical('API [simple-salesforce][Salesforce]: Login successful')

    # ¡--- Execute a SOQL query ---!
    # - https://simple-salesforce.readthedocs.io/en/latest/user_guide/queries.html
    # - Requires: an SOQL statement to be executed, make sure that it is properly formatted and not a malformed SOQL query
    def sfQuery(self, soql):
        logging.info('API [simple-salesforece][query_all]: Sending query request')

        try:
            query_result = self.credentials.query_all(soql)
            query_result = pd.DataFrame(query_result['records'])
            logging.info('API [simple-salesforce][query_all]: Query finished successfully')

        except Exception as query_error:
            query_result = []
            logging.error(f'API [simple-salesforce][query_all]: Type ({query_error.__class__.__name__})')
            logging.error(f'API [simple-salesforce][query_all]: {query_error}')

        return query_result

    # ¡--- Execute a SOQL query ---!
    # - https://simple-salesforce.readthedocs.io/en/latest/user_guide/queries.html
    # - Requires: an SOSL statement to be executed, make sure that it is properly formatted and not a malformed SOSL query
    def sfSearch(self, sosl):
        logging.info('API [simple-salesforece][search]: Sending search request')

        try:
            search_result = self.credentials.search(sosl)
            search_result = pd.DataFrame(search_result['searchRecords'])
            logging.info('API [simple-salesforce][search]: Search finished successfully')

        except Exception as search_error:
            search_result = []
            logging.error(f'API [simple-salesforce][search]: Type ({search_error.__class__.__name__})')
            logging.error(f'API [simple-salesforce][search]: {search_error}')

        return search_result
