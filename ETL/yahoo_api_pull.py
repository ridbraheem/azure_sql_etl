import pandas as pd
import yfinance as yf
import json
import numpy as np
from sqlalchemy import create_engine
import pyodbc
import urllib

DB = {'servername': '',
      'database': '',
      'username': '',
      'password': '',
      'driver': 'SQL Server Native Client 11.0'}

conn = pyodbc.connect('DRIVER='+ DB['driver']
                      +';PORT=1433;SERVER='
                      + DB['servername']
                      +';PORT=1443;DATABASE='
                      + DB['database'] +';UID='
                      + DB['username']
                      +';PWD='
                      + DB['password'])
cursor = conn.cursor()
params = urllib.parse.quote_plus("DRIVER={SQL Server Native Client 11.0}"
                                 + ";SERVER="
                                 + DB['servername']
                                 + ";DATABASE="
                                 + DB['database'] 
                                 + ";UID="
                                 + DB['username']
                                 + ";PWD="
                                 + DB['password'])

engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)

def get_dow_tickers():
    
    df = pd.read_html('https://en.wikipedia.org/wiki/Dow_Jones_Industrial_Average')
    df = df[1]
    df[['Symbol', 'New']] = df['Symbol'].str.split(':',expand=True)
    df['New'] = np.where(df['New'].isna(), df['Symbol'], df['New'])
    df = df['New'].str.strip()
    df = list(df)
    df = [i.upper() for i in df]
    
    return df

def get_ticker_info():

    df = tickers
    df = [yf.Ticker(x) for x in df]
    df = [x.info for x in df]
    df = pd.json_normalize(df)
    df = df[['symbol', 'shortName', 'industry','sector', 'country','website']]
    
    return df

def get_sector():
    
    df = tickers_info
    dfu = df.sector.unique()
    df = pd.DataFrame()
    df['sector'] = dfu.tolist()
    df['sectorid'] = df.index
    
    return df


def ticker_load():
    
    df1 = tickers_info
    df2 = sector
    df = pd.merge(df1,df2,on ='sector',how ='inner')
    df['companyid'] = df.index
    df = df.drop(columns = ['sector'])

    return df

def get_holders():

    df = tickers
    df = [yf.Ticker(x) for x in df]
    dfx = []
    for x in df:
        df = x.institutional_holders
        df['symbol'] = x
        dfx.append(df)
    dfx = pd.concat(dfx).reset_index(drop='index')
    dfx['symbol'] = dfx.symbol.astype('str').str.replace(r"yfinance.Ticker object <", " ",
                                                         regex=True).replace(r">", " ",regex=True).str.strip()
    
    return dfx

def get_holders_list():
    
    df = get_holders()
    dfh = df.Holder.unique()
    df = pd.DataFrame()
    df['Holder'] = dfh.tolist()
    df['Holderid'] = df.index
    
    return df


def holding_details():

    df1 = getHolders
    df2 = majorHolders
    df3 = companyProfile[['symbol','companyid']]
    df = pd.merge(df1,df2,on ='Holder',how ='inner')
    df = df.drop(columns = ['Holder','% Out'])
    df = pd.merge(df,df3,on ='symbol',how ='inner')
    df = df.drop(columns = ['symbol']).rename(columns={'Date Reported':'DateReported'})
    df['DateReported'] = pd.to_datetime(df['DateReported']).dt.date

    return df


def yahoo_change():
    
    df = tickers
    df2 = companyProfile[['symbol','companyid']]
    df = [yf.Ticker(x) for x in df]
    dfx = []
    for x in df:
        df = x.history(period="5d")
        df['symbol'] = x
        dfx.append(df)
    dfx = pd.concat(dfx)
    dfx['symbol'] = dfx.symbol.astype('str').str.replace(r"yfinance.Ticker object <", " ",
                                                             regex=True).replace(r">", " ",regex=True).str.strip()
    dfx = dfx.reset_index().drop(columns = ['Low','High','Volume','Dividends','Stock Splits']).rename(columns={'Open':'DayOpen',
                                                                                                       'Close': 'DayClose'})
    dfx['Date'] = pd.to_datetime(dfx['Date']).dt.date
    dfx = pd.merge(df2,dfx,on ='symbol',how ='inner')
    dfx = dfx.drop(columns = ['symbol'])

    return dfx



#### Variables before loading
tickers = get_dow_tickers()
tickers_info = get_ticker_info()
getHolders = get_holders()

##### Variables for loading table
sector = get_sector()
majorHolders = get_holders_list()
companyProfile = ticker_load()
holdingDetails = holding_details()
YahooDailyChange = yahoo_change()

# YahooDailyChange.to_sql('YahooDailyChange', index=False, con=engine, if_exists = 'append')

