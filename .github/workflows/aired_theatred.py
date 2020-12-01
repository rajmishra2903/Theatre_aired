import requests
import sqlite3
import datetime
import requests
import pandas as pd
from sqlalchemy import create_engine
import pymysql

response = requests.get("http://data.tmsapi.com/v1.1")
print(response)

#Connecting to sqlite
conn = sqlite3.connect('abc.db')
#Creating a cursor object using the cursor() method
cursor = conn.cursor()
#Doping EMPLOYEE table if already exists.
cursor.execute("DROP TABLE IF EXISTS EMPLOYEE")
#Creating table as per requirement
sql ='''CREATE TABLE MOVIEs_AIRED(
   TITLE VARCHAR(50) NOT NULL,
   RELEASE_YEAR VARCHAR(50),
   GENRES VARCHAR(50),
   DESCRIPTION VARCHAR(50),
   CHANNEL VARCHAR(50)
)'''
cursor.execute(sql)
print("Table created successfully........")
# Commit your changes in the database
conn.commit()
#Closing the connection
conn.close()
conn = sqlite3.connect('abc.db')
#Creating a cursor object using the cursor() method
cursor = conn.cursor()
cursor_1 = conn.cursor()
#Doping EMPLOYEE table if already exists.
cursor.execute("DROP TABLE IF EXISTS EMPLOYEE")
#Creating table as per requirement
sql ='''CREATE TABLE MOVIE_THEATRED(
   TITLE VARCHAR(50) NOT NULL,
   RELEASE_YEAR VARCHAR(50),
   GENRES VARCHAR(50),
   DESCRIPTION VARCHAR(50),
   THEATRE VARCHAR(50)
)'''
cursor.execute(sql)
print("Table created successfully........")
# Commit your changes in the database
conn.commit()

#Finding current Time
currnt_time=datetime.datetime.now().isoformat()
print(currnt_time)

#Reading Data from API for theatre
request_url = 'http://data.tmsapi.com/v1.1/movies/showings?startDate=2020-11-23&zip=78701&api_key=6fwmg2u2p6bnw8e5gtbtsnpq'
headers = {'api_secret': '6fwmg2u2p6bnw8e5gtbtsnpq', 'zip_code': '78701',
                   'start_date': '2020-11-23', 'line_up_id': 'USA-TX42500-X',
                   'date_time': '2020-11-23T16:19:32.729216'}
data = requests.get(request_url,headers=headers)
print(data.headers)
print(data.text)

#Reading Data from API for aired
request_url_1 = 'http://data.tmsapi.com/v1.1/movies/showings?startDate=2020-11-23&zip=78701&api_key=6fwmg2u2p6bnw8e5gtbtsnpq'
headers_1 = {'api_secret': '6fwmg2u2p6bnw8e5gtbtsnpq', 'zip_code': '78701',
                   'start_date': '2020-11-23', 'line_up_id': 'USA-TX42500-X',
                   'date_time': '2020-11-23T16:19:32.729216'}
data_air = requests.get(request_url_1,headers=headers_1)

print(data_air.text)

print(data.json())

#Creating Dataframe for aired and theatred
df_theatre = pd.DataFrame(data.json())
df_aired = pd.DataFrame(data_air.json())
df_aired_cols= df_aired[['title','releaseYear','genres','descriptionLang','entityType']]
print(df_aired_cols)
print(df_aired.columns)
print(df_aired.head())
print(df_theatre.head())


tableName = "MOVIE_AIRED"
dataFrame = df_aired
sqlEngine = create_engine('mysql://dt_admin:dt2016@localhost:3308/dreamteam_db', pool_recycle=3600)
dbConnection = sqlEngine.connect()
try:
    frame = dataFrame.to_sql(tableName, dbConnection, if_exists='fail');
except ValueError as vx:
    print(vx)
except Exception as ex:
    print(ex)


tableName1 = "MOVIE_THEATRED"
dataFrame1 = df_theatre
sqlEngine = create_engine('mysql://dt_admin:dt2016@localhost:3308/dreamteam_db', pool_recycle=3600)
dbConnection = sqlEngine.connect()
try:
    frame = dataFrame1.to_sql(tableName1, dbConnection, if_exists='fail');
except ValueError as vx:
    print(vx)
except Exception as ex:
    print(ex)

df_aired.applymap(str)

#Q1 Group by Genre
theatre_by_genre = df_theatre.groupby(["title", "genre"])["movies"].count()
aired_by_genre = df_aired.groupby(["title", "genre"])["movies"].count()

#Q2 Join on Genre
Join_on_df = pd.merge(df_aired,df_theatre,on='genre')

#Q3 Top 5 Genre
theatre_top_5 = df_theatre['genre'].groupby(df_theatre['Borough']).value_counts()
print (theatre_top_5)
print (theatre_top_5.groupby(level=[0,1]).nlargest(5))
aired_top_5 = df_aired['genre'].groupby(df_aired['Borough']).value_counts()
print (aired_top_5)
print (aired_top_5.groupby(level=[0,1]).nlargest(5))
