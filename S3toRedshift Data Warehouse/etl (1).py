import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    
    '''run each query in the copy_table_queries list, imported from sql_queries'''
    
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    
    '''run each query in the insert_table_queries list, imported from sql_queries'''
    
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    
    '''connects to database using the parameters in dwh.cfg then runs the functions to load tables and insert data'''
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    print('connecting to cluster...')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    print('loading staging tables')
    load_staging_tables(cur, conn)
    
    print('inserting tables')
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()