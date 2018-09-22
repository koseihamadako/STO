import pandas as pd
import json

# load data
def load_json(path):
    data = []
    for line in open(path, 'r', encoding="utf8"):
        data.append(json.loads(line))
    df = pd.DataFrame(data)
    
    if 'properties' in df.columns:
        # 'unpacking' properties dict and converting unix timestamp
        df = df.drop('properties', 1).assign(**pd.DataFrame.from_records(df.properties.tolist()))
    
    if 'products' in df.columns:
        # flatten products list of dicts
        products = [item for sublist in df.products for item in sublist]
        # 'unpacking' products dict and converting unix timestamp
        df = df.drop('products', 1).assign(**pd.DataFrame.from_records(products))
    
    df['date'] = pd.to_datetime(df['timestamp'],unit='s')
    # Monday=1
    df['weekday'] = df['date'].dt.dayofweek + 1
    # drop constant & uneccessary cols
    df.drop(['timestamp'],1,inplace=True)
    # sort for future explo
    df = df.sort_values(by='date')  
    return df

# remove constant cols
def rem_const(dataframe):
    for col in dataframe.columns:
        if dataframe[col].value_counts().values[0] == dataframe.shape[0]:
            dataframe.drop(col,1,inplace=True)
    return dataframe

# add freq/last/mean diff to df
def add_metrics(dataframe):
    if dataframe.empty:
        out = [0,0,0]
    else:
        dataframe['freq'] = dataframe.groupby('customer_id')['customer_id'].transform('count')
        dataframe['last'] = dataframe.groupby('customer_id')['date'].transform('last')
        dataframe.sort_values(by=['customer_id','date'], inplace=True)

        # mean difference in minutes between dataframe updates for each customer
        dataframe['diff'] = dataframe.groupby(['customer_id'])['date'].transform(lambda x: x.diff().dt.seconds.div(60))
        dataframe['mean_diff'] = dataframe.groupby('customer_id')['diff'].transform('mean').fillna(0)
        #dataframe.drop('diff', 1,inplace=True)
        out = [dataframe.iloc[0,:].freq,dataframe.iloc[0,:]['last'],dataframe.iloc[0,:].mean_diff]
    return out
        


