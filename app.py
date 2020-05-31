from flask import Flask, request
import csv
import psycopg2

#config variables
USER = 'postgres'
PASSWORD = 'admin'
HOST = '127.0.0.1'
PORT = '5432'
DATABASE = 'covid_data'
TABLE_NAME = 'covid_observations'

app = Flask(__name__)


@app.route('/')
def main():
    data = parse_csv()
    main_db_connection(data)
    return 'Hello, World!'

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
        print ("Error while connecting to PostgreSQL", error)
        return error
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
    return f'top confirmed!{ob_date}, {max_results}'