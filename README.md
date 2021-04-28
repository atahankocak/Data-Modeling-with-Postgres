## Data-Modeling-with-Postgres

Hello, 

I would like to intorduce an ETL pipeline written in Python and SQL. Now, let's go over the files.

### Files
1) **init.py:** A friend once said that it is always a good practice to have this file in the repository. I left it blank in this execrise.
2) **requirements.txt:** A list of required libraries and their versions. If you are working in different Python environment for projects, this will come in handy.
3) **info.py:** A in-house module to help with credentialing of connection/servers, and file paths. 
4) **sql_queries:** The file where the SQL queries stored , called, and run in the main processing file.
5) **create_tables:** Processes to read sql_queries.py and create a new database and tables.
6) **etl_insert.py:** One of the main versions of the ETL architecture. It processes the source datasets, connects to the database, and uses all the above to create and populate the database and the tables in Postgres. It uses INSERT command. And, it feels a bit clunky. Therefore, I developed a secodn version.
7) **etl_copy.py:**  This is the 2nd version of the main process. It differs where it processes the data. It uses COPY command after creating csv files out of each dataframe related to the table structures in the database. It is faster than the 1st version. 
  
