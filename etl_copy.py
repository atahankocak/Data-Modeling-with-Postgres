# system modules
import os
import glob

# time
from datetime import datetime

# sql connection
import psycopg2

# data munging
import pandas as pd

# local modules
from sql_queries import *
from info import db_parameters_local, filepath_song, filepath_log


def connect(paramaters_dict):
    """
        Create a connection to the database and establish a cursor to run the queries.
    """
    # connect
    try:
        conn = psycopg2.connect(**paramaters_dict)
        print("Connection to databse {} is established.".format(paramaters_dict['database']))
    except psycopg2.Error as e:
        print("Error: Could not make connection to the Postgres Database")
        print(e)

    # cursor
    try:
        cur = conn.cursor()
        print("Cursor is set.")
    except psycopg2.Error as e:
        print("Error: Could not get cursor to the Database")

    return conn, cur


def get_files(filepath):
    """
        Gathers all the json file directories in a list. In other words, partial "E" of the ETL (excluding the REST API or scraping part in the extraction).
    """
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))
    return all_files


def write_to_df(filepath):
    """
        Write the JSON files under gathered directories into df.
    """
    temp_df = pd.DataFrame()
    #columns = columns
    for file in get_files(filepath):
        data = pd.read_json(file, lines=True)
        temp_df = temp_df.append(data, ignore_index = True)
    #    temp_df.drop_duplicates(inplace=True)
    return temp_df

def csv_path(df_name):
    """
        Identifies the path as a name with the location of the file.
    """
    csv_path = "{}/{}.csv".format(os.getcwd(), df_name)
    return csv_path


def write_to_csv(df, df_name):
    """
        Writes df to csv (to utilize COPY command in PostgreSQL)
    """
    # create temp csv
    df = df.drop_duplicates(ignore_index=True)
    df.to_csv(csv_path(df_name), header=False, index=False)


def load_to_db(df, df_name, cur, conn):
    """
        Uses COPY command to load the csv files into the database and removes once done with them.
    """
    #_ = re.findall('(?>=_).*', temp_df)[0]
    query_copy = "COPY {} \
                  FROM '{}' \
                  CSV DELIMITER ',';".format(df_name, csv_path(df_name))

    cur.execute(query_copy)
    conn.commit()
    os.remove(csv_path(df_name))


def process_song_file(filepath, cur, conn):
    """
        Loads the main dataframa and performs the following for each table:
            1 - Filters using the mask provided
            2 - Drops duplicate to achieve a level or normalization.
            3 - Names the dataframe to assits in selection of csv and naming of table in database.
            4 - Writes dataframe in csv.
            5 - Loads csv in database.
    """
    # initilize song file
    df = write_to_df(filepath)

    # copy song records
    song_df = df.loc[:, ('song_id', 'title', 'artist_id', 'year', 'duration')]  # 1 - filter mask
    song_df= song_df.drop_duplicates()                                          # 2 - drop duplicates to assist with the normalization of the table
    df_name = "songs"                                                           # 3 - name the df to assit in the selection of csv file to process and naming of the table in database.
    write_to_csv(song_df, df_name)                                              # 4 - write to csv
    load_to_db(song_df, df_name, cur, conn)                                     # 5 - load to database


    # copy artist records
    artist_df = df.loc[:, ('artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude')]   #1
    artist_df = artist_df.drop_duplicates() #2
    df_name = "artists"     #3

    write_to_csv(artist_df, df_name)    #4
    load_to_db(artist_df, df_name, cur, conn)   #5


def process_log_file(filepath, cur, conn):
    """
        Same steps in "process_song_file" process is followed witha a few additons.
    """
    # initilize log file
    df = write_to_df(filepath_log)

    # filter by NextSong action to get the log oriented data under the proper category.
    df = df[df['page'] == "NextSong"]

    # sort the df by userId and date to assit with users table -
        # Our aim is to capture the latest level per user since we will utilize ON CONFLIT (userId) DO UPDATE SET level=EXCLUDED.level
    df = df.sort_values(by=['userId', 'ts'], ascending=True)

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')

    # copy time data records
    time_data = [df['ts'].values, t.dt.hour.values, t.dt.day.values, t.dt.isocalendar().week.values \
               , t.dt.month.values, t.dt.year.values, t.dt.weekday.values]
    column_labels = ['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday']   #1
    time_df = pd.DataFrame(dict(zip(column_labels, time_data)))    # 1 - since we use 2 sources, we need zip the records to produce a single list to copy
    time_df = time_df.drop_duplicates() #2
    df_name = "time"    #3

    write_to_csv(time_df, df_name)  #4
    load_to_db(time_df, df_name, cur, conn)     #5

    # copy user table
    user_df = df.loc[:, ('userId', 'firstName', 'lastName', 'gender', 'level')] #1
    user_df['userId'] = user_df['userId'].apply(pd.to_numeric)  # convert to numeric type to avoid duplications caused by the multiple data formats of the feature
    user_df = user_df.drop_duplicates(subset='userId')  #2
    df_name = "users"   #3

    write_to_csv(user_df, df_name)  #4
    load_to_db(user_df, df_name, cur, conn) #5

    #copy songPlay table
    songplay_data = []      # initilize the songplay list
    for index, row in df.iterrows():

        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay = [row.ts, row.userId, row.level, songid, artistid, \
                    row.sessionId, row.location, row.userAgent]
        songplay_data.append(songplay)

    # create colum labels and DataFrame
    column_labels = ['start_time', 'user_id', 'level', 'song_id', 'artist_id', 'session_id', 'location', 'user_agent']
    songplay_df = pd.DataFrame(songplay_data, columns=column_labels)    #1

    songplay_df['songPlay_id'] = songplay_df.index + 1   # add the Primary Key (since thse are going to id's start from 1 instead of 0)

        # reorder columns to assign PK to the 1st column
    first_column = ['songPlay_id']
    other_columns = [column for column in songplay_df.columns if column not in first_column]
    songplay_df = songplay_df[first_column + other_columns]

    songplay_df['songPlay_id'] = songplay_df['songPlay_id'].apply(pd.to_numeric)    #conver to numberic since the data came in both str and int formats
        # usual process
    songplay_df = songplay_df.drop_duplicates() #2
    df_name = 'songplays'   #3

    write_to_csv(songplay_df, df_name)  #4
    load_to_db(songplay_df, df_name, cur, conn) #5


def main():

    # initiate measure processing time
    startTime = datetime.now()

    """
        1 - Establish the connection to database.
        2 - Runs the "T" and "L" part of the ETL pipeline.
        3 - Ends the cursor and closes the connection.
    """
    # connection
    conn, cur = connect(db_parameters_local)

    # data processing
    try:
        process_song_file(filepath_song, cur, conn)
        print("Success! The song file is processed and distributed.")
    except Exception.Error as e:
        print("Error: Can't process the song file. Please check the process")


    try:
        process_log_file(filepath_log, cur, conn)
        print("Success! The log file is processed and distributed.")
    except:
        print("Error: Can't process the song file. Please check the process")

    # close connections
    cur.close()
    conn.close()
    print("Connection is closed.")

    # print measure time
    print("Processing time: ", datetime.now() - startTime)

if __name__ == "__main__":
    main()
