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


def connecting_db(db_name):
    # connecting to the database
    conn = psycopg2.connect("dbname= {0} user=caaturday".format(db_name))
    cur = conn.cursor()
    print("Opened Database Successfully")
    
    return conn, cur, my_file

def to_csv(df, my_file):
    #save df to csv
    df.to_csv("{0}.csv".format(my_file), header=df.columns, index=False, encoding='utf-8')
    
    #open the csv file, save it as an object
    my_file = open("{0}.csv".format(my_file))
    print('file opened in memory')
    
    return my_file


def drop_table(tbl_name, cur):
    SQL_STATEMENT = """
    DROP TABLE %s 
    """
    cur.execute(SQL_STATEMENT % tbl_name)
    conn.commit()
    print('table dropped succesfully')


# create table 
def create_table(tbl_name, col_str, cur):
    cur.execute("create table %s (%s);" % (tbl_name, col_str))
    print('{0} was created successfully'.format(tbl_name)) 


# populate database with data
def database_pop(tbl_name, cur, my_file):
    SQL_STATEMENT = """
    COPY %s FROM STDIN WITH
        CSV
        HEADER
        DELIMITER AS ','
    """
    cur.copy_expert(sql=SQL_STATEMENT % tbl_name, file=my_file)
    print('file copied to db')

# commit and close 
def close_db(conn, cur):
    conn.commit()
    cur.close()
    print('data imported to db completed')


# In[3]

############################################ TABLE ONE 
path = 'SQL/Corona Virus Project/CovidDeaths'
db_name = 'covid_project'
my_file = 'CovidDeaths'

df, col_str = procesing_data(path)
my_file = to_csv(df, 'CovidDeaths')


#    CONNECTING TO DB

conn, cur, my_file = connecting_db(db_name) 

drop_table('CovidDeaths', cur) # drop table that already exists in covid_projects db
create_table('CovidDeaths', col_str, cur)
database_pop('CovidDeaths', cur, my_file)


############################################   SECOND TABLE 
path = 'SQL/Corona Virus Project/CovidVaccinations'
my_file = 'CovidVaccinations'

df, col_str = procesing_data(path)
my_file = to_csv(df, 'CovidDeaths')

drop_table('CovidVaccinations', cur) # drop table that already exists in covid_projects db
create_table('CovidVaccinations', col_str, cur)
database_pop('CovidVaccinations', cur, my_file)



close_db(conn, cur)


