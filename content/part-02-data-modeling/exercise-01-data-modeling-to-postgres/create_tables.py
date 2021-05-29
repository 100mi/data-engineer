import psycopg2
from pathlib import Path
from glob import glob
import logging
import sys
from pathlib import Path
import json 
from pandas import DataFrame

# FILE DIRECTORIES 
FILE_PATH = Path(__file__).resolve()
DATA_FOLDER = FILE_PATH.parents[0].joinpath("data")

# Create Custom Logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
# Create Handler
stream_handler = logging.StreamHandler(stream=sys.stdout)
# Set Logging Level for Stream Handler
stream_handler.setLevel(logging.DEBUG)
# Set Formatter for Logging
formatter = logging.Formatter(
    "%(levelname)s — %(message)s"
)
stream_handler.setFormatter(formatter)
# Add Handler to the Logger
logger.addHandler(stream_handler)


# {table_name : query}
def perform_queries(query, cursor):
    # There are siultaneous queries that are needs to be executed
    # This will take a dictionary and loop over queries one by one 
    try : 
        cursor.execute(query)
    except Exception as e: 
        logger.error(f"{type(e).__name__} : SQL Query not iplemented")
        raise e
    else : 
        logger.debug(f"SQL Query successfully executed")             


def get_data_from_json(
    dataset_type, 
    dataset_folder=DATA_FOLDER
):
    # datset_type :  log data & song data 
    # provide flaog got which to use 
    

    # as of now pass all so that all the raw files be present
    if dataset_type not in ["log","song"] : 
        raise ValueError(f"'dataset_type' can only take values from ['log','song'], but '{dataset_type}' was passed.")

    else :
        logger.info(f"Getting each dataset from {dataset_type}")
        logger.debug(f"DATA FOLDER EXISTS : {dataset_folder.exists()}")
        
        details_list = list()
        for each_file in glob(str(DATA_FOLDER)+f"/{dataset_type}*/**/*.json", recursive=True):
            with open(each_file) as f:
                for each_line in f.readlines():
                    details_list.append(each_line)
        
        # frame = DataFrame(details_list)
        # frame.drop_duplicates(keep="first",inplace=True)
        yield from set(details_list)

def to_database(
    host,
    user,
    password,
    dbname,
    method,
):
    # Connection to postgres 
    # method will be skip and drop 
    # Drop will delete all existing tables in the starting ``

    try : 
        if method not in ["skip","drop_all"]:
            raise ValueError(
                f"{method} is an invalid value for agrument 'method'"
                )
    except Exception as e:
        logger.critical(e, exc_info=True)
        raise e

    
    # if arguments are correct then proceed connection to database 
    try : 
        con = psycopg2.connect(
            f"host={host} user={user} password={password} dbname={dbname}"
        )
    except Exception as e :
        logger.error(f"{type(e).__name__} : Connection not establised to {dbname}")
        raise 
    else : 
        # if connection is made succesfull then processs
        con.set_session(autocommit=True)
        cur = con.cursor()
        logger.debug(f"'{dbname}' CONNECTED and Cursor created.")   
    
    # Prcess to create Dimension tables 
    # 4 dimension tables are needed to be creaed 
    
    queries = {
        "users":"CREATE TABLE IF NOT EXISTS users (user_id VARCHAR PRIMARY KEY, first_name VARCHAR, last_name VARCHAR, gender VARCHAR, level VARCHAR);",
        "songs":"CREATE TABLE IF NOT EXISTS songs (song_id VARCHAR, title VARCHAR, artist_id VARCHAR, year INTEGER, duration FLOAT);",
        "artists":"CREATE TABLE IF NOT EXISTS artists (artist_id VARCHAR, name VARCHAR, location VARCHAR, latitude FLOAT, longitude FLOAT);",
        "time":"CREATE TABLE IF NOT EXISTS time (start_time INTEGER, hour INTEGER, day INTEGER, week INTEGER, month INTEGER, year INTEGER, weekday VARCHAR)",
    }
    
    try : 
        if method == "drop_all":
            for table in queries.keys():
                cur.execute(f"drop table IF EXISTS {table} cascade")
                logger.info(f"Drop Table : {table}")
    except Exception as e:
        logger.error("Error while dropping existing Tables")
        logger.error(f"{type(e).__name__} : {e}")


    # queries to create tables from scratch 
    try : 
        for query in queries : 
            perform_queries(queries[query], cursor=cur)
    except Exception as e: 
        logger.critical("Error occured while creating tables")
        raise e

    # get raw dataset from log data to put it insert into users table 
    try : 
        for each_data in get_data_from_json(dataset_type="log") :
            each_data_dict = json.loads(each_data)
            query = f"INSERT INTO users (user_id, first_name, last_name, gender, level) VALUES ('{each_data_dict.get('userId')}', '{each_data_dict.get('firstName','NA')}', '{each_data_dict.get('lastName','NA')}', '{each_data_dict.get('gender','NA')}', '{each_data_dict.get('level','NA')}') ON CONFLICT (user_id) DO UPDATE SET level = EXCLUDED.level;"
            cur.execute(query)
    except Exception as e :
        logger.error("Data Insert operation Failed")
        raise e 
    else : 
        logger.info("USER table succesfully Populated")

    # close connection 
    con.close()
    cur.close()