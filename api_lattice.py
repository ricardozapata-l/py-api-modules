# ====================================================================================================
# Set of functions to communicate with the Lattice API through regular API calls
#
# Documentation:
#     - lattice api v1 : https://developers.lattice.com/reference/introduction
#
# Developed by @Zapata: rl-zapata.github.io
# ====================================================================================================
import json
import logging
import requests as rq
import pandas as pd

class Lattice:
    # ¡--- Initiate an instance to Lattice with the given credentials (set headers) ---!
    # - https://developers.lattice.com/reference/authentication
    # - Requires: the path of the json file with the necessary login credentials
    def __init__(self, credentials_path):
        with open(credentials_path, 'r') as credentials_file:
            credentials_items = json.load(credentials_file)
        logging.critical('API [lattice][aux]: Credentials read successfully')

        self.url_base = credentials_items['base_url']
        self.headers = {
            'Authorization' : f'Bearer {credentials_items["api_key"]}',
            'Accept'        : 'application/json'
        }
        logging.critical('API [lattice][aux]: Headers set successfully')

    # ¡--- Get the full list of users ---!
    # - https://developers.lattice.com/reference/api_users
    # - Requires: n/a
    def getListAllUsers(self):
        url_endpoint = '/users?limit=100'
        url_page = ''

        logging.info('API [lattice][get | users]: Sending initial request')
        while True:
            users_result = rq.get(
                url     = self.url_base + url_endpoint + url_page,
                headers = self.headers,
            )

            if users_result.status_code == 200:
                if url_page == '':
                    logging.info('API: [lattice][get | users]: Initial request successful, checking for pagination results')
                    list_all_users_result = pd.json_normalize(users_result.json()['data'])

                else:
                    logging.info(f'API [lattice][get | users]: Results for page complete')
                    list_all_users_page = pd.json_normalize(users_result.json()['data'])
                    list_all_users_result = pd.concat([list_all_users_result, list_all_users_page])

                if users_result.json()['hasMore'] == True:
                    logging.info('API [lattice][get | users]: More results detected, sending additional request')
                    url_page = f'&startingAfter={users_result.json()["endingCursor"]}'
                else:
                    logging.info('API [lattice][get | users]: No more results detected')
                    break
            else:
                logging.error('API [lattice][get | users]; Could not complete request')
                print(url_page)
                list_all_users_result = pd.DataFrame()
                break
                
        return list_all_users_result.reset_index(drop=True)