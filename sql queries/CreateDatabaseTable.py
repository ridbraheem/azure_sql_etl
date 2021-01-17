from sqlalchemy import create_engine
import pyodbc

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

sector_table = (""" CREATE TABLE sector (
    sectorid int PRIMARY KEY,
    sector nvarchar(50) NOT NULL UNIQUE
);
""")

majorHolders_table = (""" CREATE TABLE majorHolders (
    Holderid int PRIMARY KEY,
    Holder nvarchar(50) NOT NULL UNIQUE
);
""")

companyProfile_table = (""" CREATE TABLE companyProfile (
    companyid int PRIMARY KEY,
    sectorid int REFERENCES sector(sectorid), 
    shortName nvarchar(50) NOT NULL UNIQUE,
    industry nvarchar(50) NOT NULL,
    country nvarchar(50),
    website nvarchar(100) NOT NULL UNIQUE,
    symbol nvarchar(7) NOT NULL UNIQUE
);
""")

holdingDetails_table = (""" CREATE TABLE holdingDetails (
    companyid int NOT NULL REFERENCES companyProfile(companyid),
    Holderid int NOT NULL REFERENCES majorHolders(Holderid), 
    DateReported date,
    Value bigint NOT NULL,
    Shares bigint NOT NULL,
);
""")

addconstraint_hold = (""" ALTER TABLE  dbo.holdingDetails
    ADD CONSTRAINT pk_myConstraint PRIMARY KEY (companyid,Holderid)
""")

yahoodailychange_table = (""" CREATE TABLE YahooDailyChange (
    Date date NOT NULL,
    companyid int NOT NULL REFERENCES companyProfile(companyid), 
    DayOpen float NOT NULL,
    DayClose float NOT NULL
);
""")

addconstraint_yahoo = (""" ALTER TABLE  dbo.YahooDailyChange
    ADD CONSTRAINT pk_myConstraint_yahoo PRIMARY KEY (companyid,Date)
""")

nyt_api = (""" CREATE TABLE nytapi (
    newsid int PRIMARY KEY,
    companyid int NOT NULL REFERENCES companyProfile(companyid), 
    PubDate date NOT NULL,
    headline nvarchar(MAX),
    subsection nvarchar(50),
    weblink nvarchar(MAX) NOT NULL,
    source nvarchar(50),
    )
""")


create_table_queries = [sector_table, majorHolders_table, companyProfile_table,
                        holdingDetails_table,addconstraint_hold,yahoodailychange_table,addconstraint_yahoo, nyt_api]



def create_tables(engine, conn):
    for query in create_table_queries:
        cursor.execute(query)
        conn.commit()


create_tables(cursor, conn)

