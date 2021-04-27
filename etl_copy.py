# system modules
import os
import glob

# sql connection
import psycopg2

# data munging
import pandas as pd

# local modules
from sql_queries import *
from db_parameters import parameters



# filepaths to the datasets to be inserted
filepath_song = 'data/song_data'
filepath_log = 'data/log_data'

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
        Gathers all the json file directories in a list.
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
    csv_path = "{}/{}.csv".format(os.getcwd(), df_name)
    return csv_path


def write_to_csv(df, df_name):
    # create temp csv
    df = df.drop_duplicates(ignore_index=True)
    df.to_csv(csv_path(df_name), header=False, index=False)


def load_to_db(df, df_name, cur, conn):
    #_ = re.findall('(?>=_).*', temp_df)[0]
    query_copy = "COPY {} \
                  FROM '{}' \
                  CSV DELIMITER ',';".format(df_name, csv_path(df_name))

    cur.execute(query_copy)
    conn.commit()
    os.remove(csv_path(df_name))


def process_song_file(filepath, cur, conn):
    # open song file
    df = write_to_df(filepath)

    # copy song record
    song_df = df.loc[:, ('song_id', 'title', 'artist_id', 'year', 'duration')]
    song_df= song_df.drop_duplicates()
    df_name = "songs"
    write_to_csv(song_df, df_name)
    load_to_db(song_df, df_name, cur, conn)


    # copy artist record
    artist_df = df.loc[:, ('artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude')]
    artist_df = artist_df.drop_duplicates()
    df_name = "artists"
    write_to_csv(artist_df, df_name)
    load_to_db(artist_df, df_name, cur, conn)


def process_log_file(filepath, cur, conn):
    # open log file
    df = write_to_df(filepath_log)

    # filter by NextSong action
    df = df[df['page'] == "NextSong"]

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')

    # copy time data records
    time_data = [df['ts'].values, t.dt.hour.values, t.dt.day.values, t.dt.isocalendar().week.values \
               , t.dt.month.values, t.dt.year.values, t.dt.weekday.values]
    column_labels = ['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday']
    time_df = pd.DataFrame(dict(zip(column_labels, time_data)))
    time_df = time_df.drop_duplicates()
    df_name = "time"
    write_to_csv(time_df, df_name)
    load_to_db(time_df, df_name, cur, conn)

    # copy user table
    user_df = df.loc[:, ('userId', 'firstName', 'lastName', 'gender', 'level')]
    user_df['userId'] = user_df['userId'].apply(pd.to_numeric)
    user_df = user_df.drop_duplicates(subset='userId')
    df_name = "users"
    #print(user_df)
    write_to_csv(user_df, df_name)
    load_to_db(user_df, df_name, cur, conn)

    #copy songPlay table
    songplay_data = []
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
    songplay_df = pd.DataFrame(songplay_data, columns=column_labels)

    # add the PK (start from 1 instead of 0)
    songplay_df['songPlay_id'] = songplay_df.index + 1

    # reorder columns to assign PK to the 1st column
    first_column = ['songPlay_id']
    other_columns = [column for column in songplay_df.columns if column not in first_column]
    songplay_df = songplay_df[first_column + other_columns]
    songplay_df['songPlay_id'] = songplay_df['songPlay_id'].apply(pd.to_numeric)

    songplay_df = songplay_df.drop_duplicates()
    df_name = 'songplays'

    write_to_csv(songplay_df, df_name)
    load_to_db(songplay_df, df_name, cur, conn)


def main():
    """
        1 - Establish the connection to database.
    """
    # connection
    try:
        conn = psycopg2.connect(**parameters)
        print("Connection to databse {} is established.".format(parameters['database']))
    except psycopg2.Error as e:
        print("Error: Could not make connection to the Postgres Database")
        print(e)

    # cursor
    try:
        cur = conn.cursor()
        print("Cursor is set.")
    except psycopg2.Error as e:
        print("Error: Could not get cursor to the Database")

    # data processing
    process_song_file(filepath_song, cur, conn)
    process_log_file(filepath_log, cur, conn)

    cur.close()
    conn.close()
    print("Connection is closed.")


if __name__ == "__main__":
    main()
