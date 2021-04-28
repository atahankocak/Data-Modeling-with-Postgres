# system
import os
import glob

# time
from datetime import datetime

# sql connector
import psycopg2

# data munging
import pandas as pd

# local libraries
from sql_queries import *
from info import db_parameters_local, filepath_song, filepath_log


def process_song_file(cur, filepath):
    """
        Creates the dataframes to be inserted in the songs and artists tables.
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df.loc[:, ('song_id', 'title', 'artist_id', 'year', 'duration')].values.tolist()[0]
    cur.execute(song_table_insert, song_data)

    # insert artist record
    artist_data = df.loc[:, ('artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude')].values.tolist()[0]
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
        Creates  user, time, and songplay tables.
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == "NextSong"]

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')

    # insert time data records
    time_data = [df.loc[:, ('ts')].values, t.dt.hour.values, t.dt.day.values, t.dt.isocalendar().week.values \
               , t.dt.month.values, t.dt.year.values, t.dt.weekday.values]
    column_labels = ['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday']
    time_df = pd.DataFrame(dict(zip(column_labels, time_data)))

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df.loc[:, ('userId', 'firstName', 'lastName', 'gender', 'level')]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():

        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = [row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent]
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
        Processes the file groups, since the files are read one by one...
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    # initiate time measurement
    startTime = datetime.now()

    try:
        conn = psycopg2.connect(**db_parameters_local)
        print("Connection to databse {} is established.".format(db_parameters_local['database']))
    except psycopg2.Error as e:
        print("Error: Could not make connection to the Postgres Database")
        print(e)

    # cursor
    try:
        cur = conn.cursor()
        print("Cursor is set.")
    except psycopg2.Error as e:
        print("Error: Could not get cursor to the Database")

    # processing
    try:
        process_data(cur, conn, filepath=filepath_song, func=process_song_file)
        print("Datasets in {} is processed\n".format(filepath_song))
    except Exception.Error as e:
        print("Problem processing dataset in {}.".format(filepath_song))
        print(e)

    try:
        process_data(cur, conn, filepath=filepath_log, func=process_log_file)
        print("Datasets in {} is processed\n".format(filepath_song))
    except Exception.Error as e:
        print("Problem processing dataset in {}.".format(filepath_log))
        print(e)

    cur.close()
    conn.close()

    # print processing time
    print("Processing time ", datetime.now() - startTime)

if __name__ == "__main__":
    main()
