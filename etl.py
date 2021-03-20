# Import libraries
import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


# Define functions to load staging tables and to insert data to the star schema tables
def load_staging_tables(cur, conn):
    """
    Copies data from S3 JSON files to Redshift stage tables
    using the queries from 'copy_table_queries' list.
    """        
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()
        print("Query completed")


def insert_tables(cur, conn):
    """
    Inserts data from stage tables to analytics tables 
    using the queries from 'copy_table_queries' list.
    """            
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()
        print("Query completed")


def main():
    """
    - Reads config parameters from the dwh.cfg file    
    
    - Establishes connection with the redshift cluster and database and gets
    cursor to it.  
    
    - Copies data from S3 JSON files to Redshift stage tables
    
    - Inserts data to analytics tables
    
    - Finally, closes the connection. 
    """        
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()