# read/write sql
import psycopg2

# local modules
from sql_queries import create_table_queries, drop_table_queries
from info import db_parameters, db_parameters_local


def create_database():
    """
    Creates and connects to sparkifydb in the local machine
    by connecting to a different database to perform drop and create sparkifydb.
    """
    """ set up the initial parameters dictionary for PostgreSQL"""
    # initial database to assit with DROP and CREATE new database
    parameters_dict_initial = {
        "host": "localhost",
        "database": "atahankocak",
        "port": "5432"
    }
    parameters_dict_project = {
        "host": "localhost",
        "database": "sparkifydb",
        "port": "5432"
    }

    try:
        conn = psycopg2.connect(**db_parameters_local)
        print("1 - Connected to the existing {} in {} to run the DROP and CREATE project database scripts.".format(db_parameters_local['database'], db_parameters_local['host']))
    except psycopg2.Error as e:
        print("Error: Could not make connection to the initial Postgres Database to drop and create the project database")
        print(e)

    # set auto-commmit to auto-write
    conn.set_session(autocommit=True)

    try:
        cur = conn.cursor()
    except psycopg2.Error as e:
        print("Error: Could not get cursor to the Database")
        print(e)

    # create sparkify database with UTF8 encoding
    try:
        cur.execute("DROP DATABASE IF EXISTS sparkifydb")
        cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")
        print("\n2 - Project database: {} is created".format(parameters_dict_project['database']))
    except psycopg2.Error as e:
        print("Issue creating database")
        print(e)

    # close connection to default database
    conn.close()

    # connect to sparkify database
    try:
        conn = psycopg2.connect(**parameters_dict_project)
        print()
    except psycopg2.Error as e:
        print("Error: Could not make connection to the Postgres Database")
        print(e)
    try:
        cur = conn.cursor()
    except psycopg2.Error as e:
        print("Error: Could not get cursor to the Database")
        print(e)

    print("3 - Connection and cursor are established.")
    return cur, conn


def drop_tables(cur, conn):
    """
    Drops each table using the queries in `drop_table_queries` list.
    """
    try:
        for query in drop_table_queries:
            cur.execute(query)
            conn.commit()
        print("\n4 - Existing tables (if exists) are dropped.")
    except Exception.Error as e:
        print("Issue with DROP queries")
        print(e)


def create_tables(cur, conn):
    """
    Creates each table using the queries in `create_table_queries` list.
    """
    try:
        for query in create_table_queries:
            cur.execute(query)
            conn.commit()
        print("\n5 - New tables are created.")
    except Exception.Error as e:
        print("Issue with CREATE queries.")
        print(e)


def main():
    """
    1- Drops (if exists) and re-creates the sparkify database.
    2- Establishes connection with the sparkify database and gets cursor to it.
    3- Drops all the tables.
    4- Creates all tables needed.
    5- Finally, closes the connection.
    """
    # 1, 2
    cur, conn = create_database()

    #3
    drop_tables(cur, conn)

    #4
    create_tables(cur, conn)

    #5
    conn.close()
    print("\nSuccess!")


if __name__ == "__main__":
    main()
