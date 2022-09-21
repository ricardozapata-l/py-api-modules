# ====================================================================================================
# Set of functions to communicate with the BambooHR API through regular API calls
#
# Documentation:
#     - bamboohr api v1 : https://documentation.bamboohr.com/docs
#
# Developed by @Zapata: rl-zapata.github.io
# ====================================================================================================
import json
import logging
import requests as rq
import pandas as pd

class Bamboo:
    # ¡--- Initiate an instance to BambooHR with the given credentials (set headers and auth) ---!
    # - https://documentation.bamboohr.com/docs
    # - Requires: the path of the json file with the necessary login credentials
    def __init__(self, credentials_path):
        with open(credentials_path, 'r') as credentials_file:
            credentials_items = json.load(credentials_file)
        logging.critical('API [bamboohr][aux]: Credentials read successfully')

        self.url_base = credentials_items['base_url'] + credentials_items['subdomain']
        self.headers = {
            'Accept' : 'application/json'
        }
        self.authorization = (credentials_items['api_key'], 'pass')

        logging.critical('API [bamboohr][aux]: Headers and authorization set successfully')

    # ¡--- Get the current employee directory ---!
    # - https://documentation.bamboohr.com/reference/get-employees-directory-1
    # - Requires: n/a
    def getEmployeesDirectory(self):
        url_endpoint = '/v1/employees/directory'

        logging.info('API [bamboohr][get | employees/directory]: Sending initial request')
        employees_directory_response = rq.get(
            url     = self.url_base + url_endpoint,
            headers = self.headers,
            auth    = self.authorization
        )

        if employees_directory_response.status_code == 200:
            logging.info('API [bamboohr][get | employees/directory]: Request successful, converting results')
            employees_directory_response = pd.json_normalize(employees_directory_response.json()['employees'])
        else:
            logging.error('API: [harvest][get | employees/directory]: Could not complete request')
            employees_directory_response = pd.DataFrame()

        return employees_directory_response
