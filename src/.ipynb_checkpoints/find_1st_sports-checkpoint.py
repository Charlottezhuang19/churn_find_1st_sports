from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import silhouette_samples, silhouette_score
from sklearn.decomposition import PCA
import pandas as pd
import numpy as np 
import missingno as msno
import matplotlib.pyplot as plt
import matplotlib.cm as cm 
import seaborn as sns
from odps import ODPS 
import os
import yaml
from tqdm import tqdm
import argparse

def load_data(sql_query,period_starting_date,period_ending_date,first_starting_date,first_ending_date,model_list):
    # create the connection
    conf = yaml.safe_load(open('../config/credential.yml'))
    connection = ODPS(
    conf['user']['ALI_ACCESS_KEY_ID'],
    conf['user']['ALI_ACCESS_KEY_SECRET'],
    project=conf['user']['PROJECT'],
    endpoint=conf['user']['ENDPOINT'],
    )
    # load the sql qeury
    with open(sql_query) as file:
        lines = file.readlines()
    sql = ''
    for line in lines:
        sql += line

    # replace the param with the true value
    sql = sql.replace("{PERIOD_STARTING_DATE}", f"{period_starting_date}")
    sql = sql.replace("{PERIOD_ENDING_DATE}", f"{period_ending_date}")
    sql = sql.replace("{FIRST_STARTING_DATE}", f"{first_starting_date}")
    sql = sql.replace("{FIRST_ENDING_DATE}", f"{first_ending_date}")
    sql = sql.replace("{MODEL_LIST}", f"{model_list}")
    #sql = sql.replace("{THREE_YEARS_AGO}", f"{three_years_ago}")
    #sql = sql.replace("{OPEN_DATE}", f"{open_date}")
    # sql = sql.replace("{PRESENT_DATE}", f"{present_date}")
    # print(sql)
    # execute the sql query
    query = connection.execute_sql(sql, hints={'odps.sql.submit.mode': 'script'})
    result = query.open_reader(tunnel=True)

    # export to pandas dataframe
    df = result.to_pandas(n_process=1)

    return df

def data_processing(df):
    
    
    col_sum = df['mem_cnt'].sum()
    df_pa = df.groupby('sports_1')['mem_cnt'].sum().reset_index(name='mem_cnt')
    df_pa['p(a)'] = df_pa['mem_cnt']/col_sum

    df_pb = df.groupby('sports_2_name')['mem_cnt'].sum().reset_index(name='mem_cnt')
    df_pb['p(b)'] = df_pb['mem_cnt']/col_sum

    df['joint_prob'] = df['mem_cnt']/col_sum
    merged_df = df.merge(df_pa,on = 'sports_1',how = 'left').merge(df_pb,on = 'sports_2_name',how = 'left')
    merged_df = merged_df[['sports_1','sports_2_name','mem_cnt_x','joint_prob','p(a)','p(b)']]
    merged_df['p(a)*p(b)'] = merged_df['p(a)'] * merged_df['p(b)']
    merged_df['p(a,b) - p(a)*p(b)'] = merged_df['joint_prob'] - merged_df['p(a)*p(b)']
    merged_df = merged_df.sort_values(by ='p(a,b) - p(a)*p(b)' ,ascending = False)
    result_df = merged_df[merged_df['sports_2_name'] == 'landing page models'].reset_index(drop = True)
    return result_df

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--start', 
                        type=str,
                        default = '2024-04-30')
    parser.add_argument('--end', 
                        type=str,
                        default = '2024-10-30')
    parser.add_argument('--first_start', 
                        type=str)
    parser.add_argument('--first_end', 
                        type=str,
                        default = '2024-05-28')
    
    args = parser.parse_args()
    if args.first_start is None:
        args.first_start = args.start
    
    sql_query = '../sql/query_for_probability.sql'

    PERIOD_STARTING_DATE = '2024-04-30'
    PERIOD_ENDING_DATE = '2024-10-30'
    FIRST_STARTING_DATE = '2024-04-30'
    FIRST_ENDING_DATE = '2024-05-28'
    #MODEL_LIST = "'8601260','8919753'"
    model_list_df = pd.read_csv('../data/model_list.csv')
    original_list = model_list_df['model_code'].to_list()
    quoted_elements = [f"'{str(num)}'" for num in original_list]
    # 2. 使用逗号连接所有元素
    MODEL_LIST  = ", ".join(quoted_elements)
    
    #df = load_data(sql_query,period_starting_date = FIRST_STARTING_DATE,period_ending_date=PERIOD_ENDING_DATE,first_starting_date=FIRST_STARTING_DATE,first_ending_date=FIRST_ENDING_DATE)
    
    df = load_data(sql_query,period_starting_date = args.start,period_ending_date=args.end,first_starting_date=args.first_start,first_ending_date=args.first_end,model_list=MODEL_LIST)
    result_df = data_processing(df)
    result_df.to_csv('../output/result.csv')
    
    