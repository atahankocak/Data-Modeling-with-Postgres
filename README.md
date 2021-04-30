## Data-Modeling-with-Postgres

Hello, 

I would like to intorduce an ETL pipeline written in Python and SQL.

Now, let's go over the files.

### Files
1) **init.py:** A friend once said that it is always a good practice to have this file in the repository. I left it blank in this execrise.
2) **requirements.txt:** A list of required libraries and their versions. If you are working in different Python environment for different projects, this will come in handy.
3) **info.py:** A in-house module to help with credentialing of connection/server and file paths. 
4) **sql_queries:** The file where the SQL queries stored , called, and run in the main processing file.
5) **create_tables:** Processes to read sql_queries.py and create a new database and the tables.
6) **etl_insert.py:** One of the main versions of the ETL architecture. It processes the source datasets, connects to the database, and uses all the above to create and populate the database and the tables in Postgres. It uses INSERT command. To be honest, it feels a bit clunky. Therefore, I developed a second version to help with the weight problem :).
7) **etl_copy.py:**  This is the 2nd version of the main process. It differs where it processes the data. It uses COPY command after creating csv files out of each dataframe related to the table structures in the database. It is faster than the 1st version. 
  
Now that we have a basic understanding of each file in the repo, we can jump right into the medthodology.

### Methodolgy
The 1st version (etl_insert.py) uses INSERT command to each iteration (line) of the data. In other words, it reads and writes the JSON files (or group of records) one by one.  This may become a very long process to run if the user is dealing with a a huge folder with many many json files, hence more time and cost. The second version of the process steps in to overcome that potential issue of time and cost. It combines the JSON files prior to writing into the database. It develops two main dataframes under two folders (with many many many many JSON's) and transforms to match the structure of each table. Then, it creates csv files out of the dataframes. Finally, it utilizes the COPY command to write the csv files into the proper tables. 

*I added a timing logic in each process file. To compare each process in terms of time, hence cost.*

Here is how you can run them.

### Running The Project
The very first thing to do is making sure that you have installed Postgres in your local or virtual machine. You can run the following command in your terminal 

``` sh
$ brew install postgresql
```
If you do not have Homebrew, I recommend to get it in your system. For mac users, you can find this website: (https://www.digitalocean.com/community/tutorials/how-to-install-and-use-homebrew-on-macos). Please check if you have the required libraries/packages/modules. Please see the "requirements.txt" to identfy what you would need to install in terms of Python packages. 

*I had some issues installing "psycopg2" library in the terminal. If you experience that, you can try running "!pip install psycopg2" in Jupyter notebook.* 

The next step is to update the "info.py" with your connection parameters used in psycopg2 and directories of the datasets (JSON) in your physical or virtual machine. Once that is done, you can use the terminal or Jupyter Notebook (with "!" in the begining of the command) to run "python create_tables.py". It will set up the database and create the tables for you.  Then, you can go ahead and run "python etl_insert.py" or "python etl_copy.py" to populate the tables in your new database. 

### Last note
In this particular exercise, I processed two JSON files with song and their log data for a project. However, this logic can be implemented into any dataset comeing in JSON format. 
Please contact me if need more information about this etl pipeline or you need my help with implementing a similar pipline in your business. You can drop a note in the Linkedin messages. 


Kind regards,
Atahan
