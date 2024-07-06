
# Purpose: The objective of report to identify peak hour period for nozzle at the petrol pump (Product/overall)
# Product means based on petrol Pump, product, transaction date
# Overall means based on Petrol pump, transaction date
# Description: using with help of sales data we will calculate the total sale for peak hour period.
# Approach: 1. read the sales data and do manipulation and cleaned data insert into the master table.
import pandas as pd
import numpy as np
import warnings
import datetime
from datetime import timedelta, datetime
warnings.filterwarnings("ignore")


def main(ro_codes):
    try:
        #issue_data = fetch_issue_data(DB, ro_codes)
        issue_data = pd.read_excel(r'issue_data.xlsx')
        issue_data['time_diff'] = abs(issue_data.apply(lambda row : (row['transaction_start_time'] - row['transaction_end_time']).total_seconds(), axis = 1))# calculate difference between the time
        issue_data['run_time_in_mins'] = round((issue_data.time_diff + 30)/60, 2)# roundoff decimal values
    
        issue_data = issue_data[['ro_code', 'prodcode', 'fcc_txn_id', 'transaction_date', 'transaction_start_time', 'nozzle_no', 'parent_du_no', 'quantity', 'run_time_in_mins']]
        issue_data_prod = issue_data.set_index('transaction_start_time')# set column as index
        # aggregate data 
        issue_data_prod = issue_data_prod.groupby(['ro_code', 'prodcode', 'transaction_date']).resample('2h').agg({'fcc_txn_id':'count', 'quantity': 'sum','run_time_in_mins':'sum', 'nozzle_no':'nunique','parent_du_no':'nunique'}).reset_index()        
        #rename the column names
        issue_data_prod.rename(columns={'fcc_txn_id':'total_transaction','quantity':'total_quantity', 'run_time_in_mins':'nozzle_run_time','nozzle_no':'no_of_nozzle_iss', 'parent_du_no':'no_of_du_iss'}, inplace = True)
        #sort dataframe
        issue_data_prod.sort_values(by=['total_transaction','total_quantity'],ascending=[False,False], inplace = True)
        #issue_data_prod['last_two_hour'] = [str(x)[11:19] for x in issue_data_prod['transaction_start_time']]
        #issue_data_prod['transaction_date'] = np.where(issue_data_prod['last_two_hour'] == '00:00:00',issue_data_prod['transaction_date']-timedelta(1), issue_data_prod['transaction_date'])
        # issue_data_prod = issue_data_prod.groupby(['ro_code', 'prodcode', 'transaction_date'])['ro_code', 'prodcode', 'transaction_date','transaction_start_time', 'total_transaction','total_quantity', 'nozzle_run_time', 'no_of_nozzle_iss', 'no_of_du_iss'].apply(lambda grp: grp.nlargest(2,columns = ['total_transaction','total_quantity'])).reset_index()
        #issue_data_prod = issue_data_prod.groupby(['ro_code', 'prodcode', 'transaction_date'])['transaction_start_time', 'total_transaction','total_quantity', 'nozzle_run_time', 'no_of_nozzle_iss', 'no_of_du_iss'].apply(lambda grp: grp.nlargest(2,columns = ['total_transaction','total_quantity'])).reset_index()
        issue_data_prod['du_run_time'] = issue_data_prod['nozzle_run_time']
               
        #ro_tuple = tuple(issue_data['ro_code'].unique())
        #nozzle_data = fetch_nozzle_data_prod(ro_tuple)
        nozzle_data = pd.read_excel(r'fetch_nozzle_data_prod.xlsx')
        #du_data = fetch_du_data_prod(ro_tuple)
        du_data = pd.read_excel(r'fetch_du_data_prod.xlsx')
        #ro_info = fetch_ro_master_data(ro_tuple)
        ro_info = pd.read_excel(r'ro_master_data.xlsx')
        issue_data_prod = ro_info.merge(issue_data_prod, on = 'ro_code', how = 'inner')#join dataframe
        issue_data_prod = issue_data_prod.merge(nozzle_data, on = ['ro_code', 'prodcode'], how = 'inner')
        issue_data_prod = issue_data_prod.merge(du_data, on = ['ro_code', 'prodcode'], how = 'inner')
        issue_data_prod['nozzle_utilization'] = round(issue_data_prod['nozzle_run_time']/(issue_data_prod['no_of_nozzle_master'] * 120) * 100,2)
        issue_data_prod['du_utilization'] = round(issue_data_prod['du_run_time']/(issue_data_prod['no_of_du_master'] * 120) * 100,2)
        issue_data_prod['last_two_hour'] = [str(x)[11:19] for x in issue_data_prod['transaction_start_time']]#seperate time from datetime
        # issue_data_prod['transaction_day'] = np.where(issue_data_prod['last_two_hour'] == '00:00:00',issue_data_prod['transaction_day']-timedelta(1), issue_data_prod['transaction_day'])
        
        two_hr_bucket = pd.read_excel(r'map_two_hr.xlsx',engine='openpyxl')
        two_hr_bucket['bucket'] = two_hr_bucket['bucket'].astype(str)#convert datatype 
        issue_data_prod['last_two_hour'] = issue_data_prod['last_two_hour'].astype(str)# change datatype of columns
        issue_data_prod = issue_data_prod.merge(two_hr_bucket, left_on = ['last_two_hour'], right_on = ['bucket'], how = 'left')
        issue_data_prod.rename(columns= {'seq':'peak_hour_bucket'}, inplace = True)#rename column names
            
        issue_data_over = issue_data.set_index('transaction_start_time')# set column as index
        issue_data_over = issue_data_over.groupby(['ro_code', 'transaction_date']).resample('2h').agg({'fcc_txn_id':'count', 'quantity': 'sum','run_time_in_mins':'sum', 'nozzle_no':'nunique','parent_du_no':'nunique'}).reset_index()
        issue_data_over.rename(columns={'fcc_txn_id':'total_transaction','quantity':'total_quantity', 'run_time_in_mins':'nozzle_run_time','nozzle_no':'no_of_nozzle_iss', 'parent_du_no':'no_of_du_iss'}, inplace = True)
        issue_data_over.sort_values(by=['total_transaction','total_quantity'],ascending=[False,False], inplace = True)
        #issue_data_over = issue_data_over.groupby(['ro_code', 'transaction_date'])['ro_code', 'transaction_date','transaction_start_time', 'total_transaction', 'total_quantity','nozzle_run_time', 'no_of_nozzle_iss', 'no_of_du_iss'].apply(lambda grp: grp.nlargest(2,['total_transaction','total_quantity'])).reset_index()
        issue_data_over['du_run_time'] = issue_data_over['nozzle_run_time']
        
        #nozzle_data = fetch_nozzle_data(ro_tuple)
        nozzle_data = pd.read_excel(r'fetch_nozzle_data.xlsx')
        #du_data = fetch_du_data(ro_tuple)
        du_data = pd.read_excel(r'fetch_du_data.xlsx')
        # ro_info = fetch_ro_master_data(ro_tuple)
        # ro_info = pd.read_excel(r'fetch_nozzle_data_prod.xlsx')
        issue_data_over = ro_info.merge(issue_data_over, on = 'ro_code', how = 'inner')
        issue_data_over = issue_data_over.merge(nozzle_data, on = 'ro_code', how = 'inner')
        issue_data_over = issue_data_over.merge(du_data, on = 'ro_code', how = 'inner')
        issue_data_over['nozzle_utilization'] = round(issue_data_over['nozzle_run_time']/(issue_data_over['no_of_nozzle_master'] * 120) * 100,2)
        issue_data_over['du_utilization'] = round(issue_data_over['du_run_time']/(issue_data_over['no_of_du_master'] * 120) * 100,2)
        issue_data_over['last_two_hour'] = [str(x)[11:19] for x in issue_data_over['transaction_start_time']]
        issue_data_over['last_two_hour'] = issue_data_over['last_two_hour'].astype(str)
        
        issue_data_over = issue_data_over.merge(two_hr_bucket, left_on = ['last_two_hour'], right_on = ['bucket'], how = 'inner')
        issue_data_over.rename(columns= {'seq':'peak_hour_bucket'}, inplace = True)

        issue_data_prod['ro_code'] = issue_data_prod['ro_code'].astype(str)
        issue_data_prod['prodcode'] = issue_data_prod['prodcode'].astype(str)
        issue_data_prod['transaction_date'] = issue_data_prod['transaction_date'].astype(str)
        issue_data_prod.sort_values(by=['ro_code', 'prodcode', 'transaction_date'], inplace = True)
        
        issue_data_over['ro_code'] = issue_data_over['ro_code'].astype(str)
        issue_data_over['transaction_date'] = issue_data_over['transaction_date'].astype(str)
        issue_data_over.sort_values(by=['ro_code','transaction_date'], inplace = True)
        
        issue_data_prod = issue_data_prod[['salesorg', 'salesoff', 'sales_area', 'class_of_market', 'ro_code', 'transaction_date', 'peak_hour_bucket', 'prodcode', 'total_transaction',
                                           'total_quantity', 'no_of_nozzle_master', 'no_of_nozzle_iss', 'nozzle_run_time', 'nozzle_utilization', 'no_of_du_master', 'no_of_du_iss',
                                           'du_run_time', 'du_utilization']]
        
        issue_data_over = issue_data_over[['salesorg', 'salesoff', 'sales_area', 'class_of_market', 'ro_code', 'transaction_date', 'peak_hour_bucket', 'total_transaction',
                                           'total_quantity', 'no_of_nozzle_master', 'no_of_nozzle_iss', 'nozzle_run_time', 'nozzle_utilization', 'no_of_du_master', 'no_of_du_iss','du_run_time',
                                           'du_utilization']]
        # issue_data_prod.to_excel('nozzle_utilization_anlysis.xlsx', sheet_name = 'product_level')
        # issue_data_over.to_excel('nozzle_utilization_anlysis.xlsx', sheet_name = 'overall')

        if issue_data_prod.shape[0]>0:
            print('Insert data into the table')

    except Exception as exc:
        DB.execute_rollback_analytics_analyticsdb()
        print('Transaction Rolled Back due to Exception: ', str(exc))
        raise exc
    else:
        DB.execute_commit_analytics_analyticsdb()

if __name__ == '__main__' :
    #current_datetime = pd.datetime.now()
    # current_datetime = datetime.strptime('2022-07-17',"%Y-%m-%d")
    # today = current_datetime.date()
    # seven_days = today - timedelta(6)
    total_cnt = ro_code_master['ro_code'].count()
    ro_code_master = tuple(ro_code_master['ro_code'].unique())
    
    N = 1000
    i = 0
    j = N
    
    while True:
        print("\nProcessing Records From", i, "to", min(j, total_cnt), "Out Of", total_cnt, flush=1)
        ro_codes = ro_code_master[i:j]
        
        batch_start_time = pd.datetime.now()
        
        main(ro_codes)
        
        batch_end_time = pd.datetime.now()
        
        print("Main Block Execution Time: ", batch_end_time - batch_start_time)
        
        if len(ro_codes) < N:
            break
        else:
            i = j
            j = j + N
    
i = 0, j = 500
i = 500, j = 1000
i = 1000, j = 1000 + 500 = 1500
i = 49500, j = 50000
"hello"

issue_data_prod['last_two_hour'] = [str(x)[11:19] for x in issue_data_prod['transaction_start_time']]
issue_data_prod['transaction_date'] = np.where(issue_data_prod['last_two_hour'] == '00:00:00',issue_data_prod['transaction_date']-timedelta(1), issue_data_prod['transaction_date'])
issue_data_prod = issue_data_prod.groupby(['ro_code', 'prodcode', 'transaction_date'])['ro_code', 'prodcode', 'transaction_date','transaction_start_time', 'total_transaction','total_quantity', 'nozzle_run_time', 'no_of_nozzle_iss', 'no_of_du_iss'].apply(lambda grp: grp.nlargest(2,columns = ['total_transaction','total_quantity'])).reset_index()
issue_data_prod = issue_data_prod.groupby(['ro_code', 'prodcode', 'transaction_date'])['transaction_start_time', 'total_transaction','total_quantity', 'nozzle_run_time', 'no_of_nozzle_iss', 'no_of_du_iss'].apply(lambda grp: grp.nlargest(2,columns = ['total_transaction','total_quantity'])).reset_index()
 
