#====================================================================================================
# Script to work with AWS's sdk
#
# Developed by @Zapata: rl-zapata.github.io
# ====================================================================================================
import boto3
import logging
import pandas as pd
import time

def redshift(region, credentials, sql):
    client = boto3.client('redshift-data', region_name=region)

    query_req = client.execute_statement(
        ClusterIdentifier   = credentials['cluster'],
        Database            = credentials['database'],
        DbUser              = credentials['user'],
        Sql                 = sql
    )
    query_id = query_req['Id']

    # create waiter for the query
    query_status = ''
    wait_cnt = 0
    while query_status != 'FINISHED':
        
        query_des = client.describe_statement(Id=query_id)
        query_status = query_des['Status']

        if query_status == 'FAILED' or wait_cnt == 15:
            logging.error('SDK [AWS | REDSHIFT]: Query failed or timed-out')
            return 0
        else:
            logging.info('SDK [AWS | REDSHIFT]: Waiting [10 seconds] for query to finish')
            wait_cnt += 1
            time.sleep(10)

    # convert the results into a dataframe
    logging.info(('SDK [AWS | REDSHIFT]: Query successful, converting into data-frame'))
    query_res = client.get_statement_result(Id=query_id)

    res_col = []
    for column in query_res['ColumnMetadata']:
        res_col.append(column['name'])

    res_content = []
    for ent_row in query_res['Records']:
        ent_content = []
        for ent_col in ent_row:
            ent_content.append(list(ent_col.values())[0])

        res_content.append(ent_content)

    query_df = pd.DataFrame(columns=res_col, data=res_content)

    return query_df