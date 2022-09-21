# ====================================================================================================
# Set of functions to communicate with the Greenhouse API through regular API calls
#
# Documentation:
#     - greenhouse api v1 : https://developers.greenhouse.io/harvest.html
#
# Developed by @Zapata: rl-zapata.github.io
# ====================================================================================================
import base64
import json
import logging
import requests as rq
import pandas as pd

class Greenhouse:
    # ¡--- Initiate an instance to Harvest(Greenhouse) with the given credentials (set headers and auth) ---!
    # - https://developers.greenhouse.io/harvest.html#authentication
    # - Requires: the path of the json file with the necessary login credentials
    def __init__(self, credentials_path):
        with open(credentials_path, 'r') as credentials_file:
            credentials_items = json.load(credentials_file)
        logging.critical('API [greenhouse][aux]: Credentials read successfully')

        self.url_base = credentials_items['base_url']

        self.authorization = base64.b64encode(f'{credentials_items["api_key"]}:'.encode('utf-8'))
        self.headers = {
            'Authorization' : f'Basic {self.authorization.decode("utf-8")}',
            'Accept'        : 'application/json'
        }
        logging.critical('API [greenhouse][aux]: Headers and authorization set successfully')


    # ¡--- Get the current list of jobs ---!
    # - https://developers.greenhouse.io/harvest.html#get-list-jobs
    # - Requires: n/a
    def getJobsList(self):
        url_endpoint = self.url_base + 'jobs?per_page=500'

        logging.info('API [greenhouse][get | jobs]: Sending initial request')
        jobs_list_result = rq.get(
            url     = url_endpoint,
            headers = self.headers
        )

        if jobs_list_result.status_code == 200:
            logging.info('API [greenhouse][get | jobs]: Request successful, converting results')
            jobs_list_result = pd.json_normalize(jobs_list_result.json())
        else:
            logging.error('API [greenhouse][get | jobs]: Couldn\'t complete request')
            jobs_list_result = pd.DataFrame()

        return jobs_list_result