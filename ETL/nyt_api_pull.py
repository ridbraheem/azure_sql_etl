from sqlalchemy import create_engine
from datetime import datetime,timedelta
from time import sleep
import time
import pandas as pd
import numpy as np
import json
import pyodbc
import requests
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

# https://github.com/achin83/Yelp-API/blob/master/YELP.ipynb
# https://github.com/learn-co-students/ds-skills2-python-api-intro-yelp-nyc-ds-skills-011519
def get_company_info():
    
    dquery = pd.read_sql("""SELECT companyid, shortName FROM companyProfile""", conn)

    dquery['short'] = dquery.shortName.astype('str').str.replace(r"\(.*\)", " ",
                                                             regex=True).replace(r"Inc.", " ",
                                                             regex=True).replace(r",", " ",
                                                             regex=True).replace(r"Corporation", " ",
                                                             regex=True).replace(r" Company", " ",
                                                             regex=True).str.strip()
    dquery
    
    return dquery


def get_nyt_api():
    
    today = datetime.today().strftime('%Y%m%d')
    lastweek = datetime.strftime(datetime.now() - timedelta(7), '%Y%m%d')
    api_key = ""
    dfc = get_company_info()
    dfc = dfc['short'].values.tolist()
    
    dfx = []

    for x in company:

        parameters = {
        "query": x,
        "begin_date": lastweek,
        "end_date": today,
        "sort": "newest",
        "api-key": api_key
        }

        url = "https://api.nytimes.com/svc/search/v2/articlesearch.json"

        try:
            response = requests.get(url, params=parameters).json()['response']['docs']
            df = pd.json_normalize(response)[['pub_date',
                                              'headline.print_headline',
                                              'subsection_name',
                                              'web_url',
                                              'source']].rename(columns={'pub_date': 'PubDate',
                                                                         'headline.print_headline': 'headline',
                                                                         'subsection_name': 'subsection',
                                                                         'web_url': 'weblink'})
            df['short'] = parameters["query"]
            dfx.append(df)
        except:
            continue

    dfx = pd.concat(dfx).reset_index(drop='index')
    dfx['newsid'] = dfx.index
    dfx['PubDate'] = dfx['PubDate'].str[:10]
    dfx['PubDate'] = pd.to_datetime(dfx['PubDate']).dt.date
    dfx = pd.merge(dquery,dfx,on ='short',how ='inner').drop(columns = ['short','shortName'])

    return dfx


get_nyt_api().to_sql('nytapi', con=engine, index=False, if_exists = 'append')

