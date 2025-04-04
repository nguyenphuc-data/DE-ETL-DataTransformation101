from dotenv import load_dotenv
import os
from urllib.parse import urlparse

load_dotenv()

def get_database_config():
    jdbc_url = os.getenv("DB_URL")

    parser_url = urlparse(jdbc_url.replace("jdbc:","",1))

    host = parser_url.hostname
    port = parser_url.port
    database = parser_url.path.strip("/")

    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")

    return {
        "host" : host,
        "port" : port,
        "user" : user,
        "password" : password,
        "database" : database
    }