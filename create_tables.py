# Import libraries
import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


# Define functions to drop and create tables in the database
def drop_tables(cur, conn):
    """
    Drops each table using the queries in `drop_table_queries` list.
    """    
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Creates each table using the queries in `create_table_queries` list. 
    """    
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


# Define the main function for this script
def main():
    """
    - Reads config parameters from the dwh.cfg file    
    
    - Establishes connection with the redshift cluster and database and gets
    cursor to it.  
    
    - Drops all the tables.
    
    - Creates all tables needed. 
    
    - Finally, closes the connection. 
    """    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()