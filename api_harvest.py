# ====================================================================================================
# Set of functions to communicate with the Harvest API through regular API calls
#
# Documentation:
#     - harvest api v2 : https://help.getharvest.com/api-v2/
#
# Developed by @Zapata: rl-zapata.github.io
# ====================================================================================================
import json
import logging
import re
import requests as rq
import pandas as pd

class Harvest:
    # ¡--- Initiate an instance to Harvest with the given credentials (set the headers) ---!
    # - https://help.getharvest.com/api-v2/authentication-api/authentication/authentication/
    # - Requires: the path of the json file with the necessary login credentials
    def __init__(self, credentials_path):
        with open(credentials_path, 'r') as credentials_file:
            credentials_items = json.load(credentials_file)
        logging.critical('API [harvest][aux]: Credentials read successfully')

        self.credentials = {
            'Authorization'     : 'Bearer ' + credentials_items['user_token'],
            'Harvest-Account-Id': credentials_items['user_id'],
            'User-Agent'        : credentials_items['user_agent']
        }
        self.url_base = 'https://api.harvestapp.com/v2/'

        logging.critical('API [harvest][headers]: Headers set successfully')

    # ¡--- Get the list of tasks worked on with ina given time period
    # - https://help.getharvest.com/api-v2/timesheets-api/timesheets/time-entries/
    # - Requires: a valid start and end date in YYYYMMDD format
    # - Optional: a list with the required fields to return
    def getTimeEntries(self, request_start, request_end, request_fields=[]):
        logging.info('API [harvest][get | time_entries]: Sending initial request')
        time_entries_url = f'{self.url_base}time_entries?from={request_start}&to={request_end}'
        time_entries_call = rq.get(time_entries_url, headers=self.credentials)

        if time_entries_call.status_code == 200:
            logging.info('API [harvest][get | time_entries]: Initial request successful, getting paginated results')
            time_entries_result = pd.json_normalize(time_entries_call.json()['time_entries']) 
            page_last = time_entries_call.json()['links']['last']
            page_last = int(re.findall('&page=(\d+)', page_last)[0])

            for page_cnt in range(2, page_last):
                logging.info(f'API [harvest | time_entries]: Getting results for page [{page_cnt} | {page_last - 1}]')
                page_call = rq.get(
                    f'{time_entries_url}&page={page_cnt}',
                    headers = self.credentials
                )

                page_call = pd.json_normalize(page_call.json()['time_entries'])
                time_entries_result = pd.concat([time_entries_result, page_call])

            if any(request_fields) == True:
                time_entries_result = time_entries_result[request_fields]
            time_entries_result = time_entries_result.reset_index(drop=True)
            time_entries_result.columns = time_entries_result.columns.str.replace('.', '_')

        else:
            time_entries_result = []
            logging.error('API [harvest][get | time_entries]: Could not complete request')

        return time_entries_result

    # ¡--- Get the time report list of each client within a given time frame ---!
    # - https://help.getharvest.com/api-v2/reports-api/reports/time-reports/
    # - Requires: a valid time frame in YYYYMMDD date, with at most a 365 day time span
    # - Simple implementation of this endpoint, linked to the full version (getTimeReportClientsFull), can still be used separately
    def getTimeReportClientsSimple (self, request_start, request_end):
        logging.info('API [harvest][get | reports/time/clients]: Sending request')
        time_report_clients_url = f'{self.url_base}reports/time/clients?from={request_start}&to={request_end}'
        time_report_clients_call = rq.get(time_report_clients_url, headers=self.credentials)

        if time_report_clients_call.status_code == 200:
            time_report_clients_result = pd.json_normalize(time_report_clients_call.json()['results'])
            logging.info('API [harvest][get | reports/time/clients]: Request retrieved successfully')

        else:
            time_report_clients_result = []
            logging.error('API [harvest][get | reports/time/clients]: Could not complete request')

        return time_report_clients_result

    # ¡--- Get the full summary of hours accumulated by each client in a given time period ---!
    # - Requires: 
    #   - a valid start and end date as an arrow time object
    #   - a valid span type (month, year)
    #   - a request exact option dictionary for the start and end dates
    # - Optional: a list of the required field to return
    def getTimeReportClientsFull (self, request_start, request_end, request_span, request_exact, request_merge):
        request_span_valid = ['month', 'year']
        if request_span not in request_span_valid:
            raise ValueError(f'request_span must be one of: {request_span_valid}')
        else:
            import arrow

        request_span = list(arrow.Arrow.span_range(request_span, request_start, request_end))
        request_span_total = len(request_span)

        for time_span_cnt, time_span_ent in enumerate(request_span):
            logging.info(f'API [harvest][aux | reports/time/clients]: Setting properties for call [{time_span_cnt + 1} | {request_span_total}]')
            if time_span_cnt == 0 and request_exact['start'] == True:
                call_start   = time_span_ent[0].replace(month=request_start.month, day=request_start.day)
                call_end     = time_span_ent[1]

            elif time_span_cnt == request_span_total-1 and request_exact['end'] == True:
                call_start   = time_span_ent[0]
                call_end     = time_span_ent[1].replace(month=request_end.month, day=request_end.day)

            else:
                call_start   = time_span_ent[0]
                call_end     = time_span_ent[1]

            time_report_clients_ent = self.getTimeReportClientsSimple(
                request_start = call_start.format('YYYYMMDD'), 
                request_end = call_end.format('YYYYMMDD')
            )

            if time_span_cnt == 0:
                time_report_clients_full = time_report_clients_ent.drop(columns=['total_hours',	'currency',	'billable_amount'])
            else:
                time_report_clients_full = pd.merge(
                    time_report_clients_full,
                    time_report_clients_ent[['client_id', 'client_name', 'billable_hours']],
                    how='outer',
                    on=['client_id', 'client_name']
                ).fillna(0)

                if request_merge == True:
                    time_report_clients_full['billable_hours'] = time_report_clients_full['billable_hours_x'] + time_report_clients_full['billable_hours_y']
                    time_report_clients_full = time_report_clients_full.drop(columns=['billable_hours_x', 'billable_hours_y'])
                else:
                    time_report_clients_full = time_report_clients_full.rename(
                        columns={
                            'billable_hours_x':f'billable_hours_{time_span_cnt}',
                            'billable_hours_y':f'billable_hours_{time_span_cnt+1}',
                        }
                    )
        logging.info('API [harvest][aux | time/reports/clients]: Information gathered successfully')

        return time_report_clients_full

# --------------------------------------------------------------------------------------
# Change it so it also includes the total amount of hours, not just the billable ones
# --------------------------------------------------------------------------------------