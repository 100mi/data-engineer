import psycopg2
from pathlib import Path
from glob import glob
import logging
import sys
from pathlib import Path
import json 

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
def perform_queries(dict_obj, cursor):
    for table_name in dict_obj :
        try : 
            cursor.execute(dict_obj.get(table_name))
        except Exception as e: 
            logger.error(f"{type(e).__name__} : Operation on '{table_name}' not completed")
            raise e
        else : 
            logger.debug(f"'{table_name}' successfully created")             


def get_data_from_json(
    dataset_type, 
    dataset_folder=DATA_FOLDER
):
    # log data & song data 
    # provide flaog got which to use 
    logger.info("function started")
    if dataset_type not in ["log","song"] : 
        raise ValueError(f"'dataset_type' can only take values from ['log','song'], but '{dataset_type}' was passed.")
    else :
        logger.debug(f"DATA FOLDER EXISTS : {dataset_folder.exists()}")
        for each_file in glob(str(DATA_FOLDER)+f"/{dataset_type}*/**/*.json", recursive=True):
            with open(each_file) as f:
                yield from f.readlines()
                

def insert_data_to_tables() :
    pass

def to_database(
    host,
    user,
    password,
    dbname,
    raw_data = None
):
    # Connection to postgres 
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
        "users":"CREATE TABLE IF NOT EXISTS users (user_id varchar, first_name varchar, last_name varchar, gender varchar, level varchar);",
        "songs":"CREATE TABLE IF NOT EXISTS songs (song_id varchar, title varchar, artist_id varchar, year int, duration decimal);",
        "artists":"CREATE TABLE IF NOT EXISTS artists (artist_id varchar, name varchar, location varchar, latitude decimal, longitude decimal);",
        "time":"CREATE TABLE IF NOT EXISTS time (start_time int, hour int, day int, week int, month int, year int, weekday varchar)",
    }
    perform_queries(queries, cursor=cur)

    # close database 
    cur.close()
    con.close()


if __name__ == "__main__" : 

    for each in get_data_from_json(dataset_type="log"):
        print(each)


    # to_database(
    #     host= "localhost",
    #     user= "riddle",
    #     password= "riddle",
    #     dbname= "sparkify"
    # )