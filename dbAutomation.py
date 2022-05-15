#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Sat May 14 14:13:59 2022
caaturday
❀◕ ‿ ◕❀
"""

# In[1]
import pandas as pd
import psycopg2 

# In[2]

def procesing_data(path):
    df = pd.read_csv(r'/Users/caaturday/Documents/Projects/{0}.csv'.format(path))
    
    #processing data
    replacements = {
        'timedelta64[ns]': 'varchar',
        'object': 'varchar',
        'float64': 'float',
        'int64': 'int',
        'datetime64': 'timestamp'
    }
    
    col_str = ", ".join("{} {}".format(n, d) for (n, d) in zip(df.columns, df.dtypes.replace(replacements)))
    
    return df, col_str


def connecting_db(df, db_name, my_file):
    # connecting to the database
    db_conn = psycopg2.connect("dbname= {0} user=caaturday".format(db_name))
    cur = db_conn.cursor()
    print("Opened Database Successfully")
    
    
    #save df to csv
    df.to_csv("{0}.csv".format(my_file), header=df.columns, index=False, encoding='utf-8')
    
    #open the csv file, save it as an object
    my_file = open("{0}.csv".format(my_file))
    print('file opened in memory')
    
    return db_conn, cur, my_file


def table(tbl_name, col_str, db_conn, cur):
# create table 
    
    cur.execute("create table %s (%s);" % (tbl_name, col_str))
    print('{0} was created successfully'.format(tbl_name)) 
    
    
    #upload to db
    SQL_STATEMENT = """
    COPY %s FROM STDIN WITH
        CSV
        HEADER
        DELIMITER AS ','
    """
    cur.copy_expert(sql=SQL_STATEMENT % tbl_name, file=my_file)
    print('file copied to db')
    
    
    db_conn.commit()
    cur.close()
    print('table {0} imported to db completed'.format(tbl_name))


# In[3]

path = 'SQL/Corona Virus Project/CovidVaccinations'
db_name = 'covid_vacc'
my_file = 'CovidVaccinations'
tbl_name = 'CovidVaccinations'

df, col_str = procesing_data(path)
db_conn, cur, my_file = connecting_db(df, db_name, my_file)
table(tbl_name, col_str, db_conn, cur)

# In[4]
## clean up functions and update db to hold both covid csv files as two tables. 
