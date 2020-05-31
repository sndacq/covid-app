from flask import Flask, request
from config import *
import csv
import psycopg2
import collections


app = Flask(__name__)


@app.route('/')
def main():
    data = parse_csv()
    main_db_connection(data)
    return 'App started!'

def parse_csv():
    CSV_FILE_NAME = 'covid_19_data.csv' 
    print(f'Parsing csv file: {CSV_FILE_NAME}...')

    with open(CSV_FILE_NAME, mode='r') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        data = list(csv_reader)
        # TODO: other columns are in float type

        # Remove csv header
        data.pop(0)

        return data

def main_db_connection(data):
    try:
        connection = psycopg2.connect(user = USER,
                                      password = PASSWORD,
                                      host = HOST,
                                      port = PORT,
                                      database = DATABASE)

        cursor = connection.cursor()
        # Check if table already exists
        table_exists = check_table(cursor)
        if not table_exists:            
            create_table(cursor)
            connection.commit()
            print('Table succssfully created')
            # Insert data to table
            insert_query(data, connection)

    except (Exception, psycopg2.Error) as error :
        print (error)
    finally:
        if(connection):
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")


def insert_query(data, connection):
    cursor = connection.cursor()
    sql_insert_query = f'''INSERT INTO {TABLE_NAME} (seqno, ob_date, state, country, last_update, confirmed, deaths, recovered) 
                           VALUES (%s, %s, %s, %s, %s, %s, %s, %s)'''

    # executemany() to insert multiple rows rows
    result = cursor.executemany(sql_insert_query, data)
    connection.commit()
    print(cursor.rowcount, f"Record inserted successfully into {TABLE_NAME} table")


def check_table(cursor):
    query = f'''SELECT EXISTS(SELECT 1 FROM information_schema.tables 
                WHERE table_catalog='{DATABASE}' AND 
                      table_schema='public' AND 
                      table_name='{TABLE_NAME}');'''
    cursor.execute(query)
    table_exists = cursor.fetchone()[0] 
    print(f'Table exists: {table_exists}')
    return table_exists


def create_table(cursor):
    create_table_query = '''
        CREATE TABLE covid_observations (
            seqno       serial  PRIMARY KEY NOT NULL,
            ob_date     DATE    NOT NULL,
            state       VARCHAR,
            country     VARCHAR,
            last_update TIMESTAMP,
            confirmed   REAL,
            deaths      REAL,
            recovered   REAL
            ); 
        '''

    cursor.execute(create_table_query)

@app.route('/top/confirmed', methods=['GET'])
def get_top_confirmed():

    ob_date = request.args.get('observation_date', '')
    max_results = request.args.get('max_results', '')

    # TODO: validate args

    data = get_all_data(ob_date)
    if data:
        return clean_data(data, ob_date, int(max_results))

    return 'Error'



def get_all_data(ob_date):
    data = None
    try:
        connection = psycopg2.connect(user = USER,
                                      password = PASSWORD,
                                      host = HOST,
                                      port = PORT,
                                      database = DATABASE)

        cursor = connection.cursor()
        cursor.execute(f"""SELECT country, confirmed, deaths, recovered 
                            from {TABLE_NAME}
                            where ob_date <= '{ob_date}'
                            """)
        data = list(cursor.fetchall())
        print(f'{len(data)} entries found')
  
    except (Exception, psycopg2.Error) as error :
        print(error)

    finally:
        if(connection):
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")
            return data


def clean_data(data, ob_date, max_results):
    countries = set(item[0] for item in data)
    clean = {}
    for country in countries:
        clean[country] = { 'confirmed': 0,
                           'deaths': 0,
                           'recovered': 0,}
    for item in data:
        clean[item[0]]['country'] = item[0]
        clean[item[0]]['confirmed'] += item[1]
        clean[item[0]]['deaths'] += item[2]
        clean[item[0]]['recovered'] += item[3]

    result = collections.OrderedDict(sorted(clean.items(), key=lambda t:t[1]["confirmed"]))

    final_countries = []
    for i in range(max_results):
        final_countries.append(result.popitem()[1])

    final_result = { "observation_date": ob_date,
                     "countries": final_countries
    }

    return final_result

