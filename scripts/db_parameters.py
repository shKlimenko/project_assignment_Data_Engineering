import os
from dotenv import load_dotenv

load_dotenv()

DB_PARAMS = {
    "database": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT")
}

LOGS_DB_PARAMS = {
    "database": os.getenv("LOGS_DB_NAME"),
    "user": os.getenv("LOGS_DB_USER"),
    "password": os.getenv("LOGS_DB_PASSWORD"),
    "host": os.getenv("LOGS_DB_HOST"),
    "port": os.getenv("LOGS_DB_PORT")
}

DB_PARAMS_LOC = {
    "database": os.getenv("DB_NAME_LOC"),
    "user": os.getenv("DB_USER_LOC"),
    "password": os.getenv("DB_PASSWORD_LOC"),
    "host": os.getenv("DB_HOST_LOC"),
    "port": os.getenv("DB_PORT_LOC")
}