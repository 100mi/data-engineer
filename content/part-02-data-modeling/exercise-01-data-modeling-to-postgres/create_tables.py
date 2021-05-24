import psycopg2
from pathlib import Path


# Connection to postgres 
con = psycopg2.connect(
    "host=127.0.0.1 user=somi password=somitra@100"
)

# Create Database 

# Create Tables 

# close database 
con.close()