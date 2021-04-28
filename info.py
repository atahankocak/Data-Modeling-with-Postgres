"""
    Database Creds: Please update with your won local db parameters
"""

db_parameters_local = {
    "host": "localhost",
    "database": "sparkifydb",
    "port": "5432"
}

"""
Creates and connects to sparkifydb in the local machine
by connecting to a different database to perform drop and create sparkifydb.
"""
db_parameters = {
    "host": "localhost",
    "database": "atahankocak",
    "port": "5432"
}

"""
    Update with where you save your data files.
"""

filepath_song = 'data/song_data'
filepath_log = 'data/log_data'
