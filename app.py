import os
import pyodbc
import struct
import pandas as pd

from azure import identity
from flask import Flask, request, jsonify

AZURE_SQL_CONNECTIONSTRING = os.environ["AZURE_SQL_CONNECTIONSTRING"]

def get_conn(connection_string: str):
    credential = identity.DefaultAzureCredential(exclude_interactive_browser_credential=False)
    token_bytes = credential.get_token("https://database.windows.net/.default").token.encode("UTF-16-LE")
    token_struct = struct.pack(f'<I{len(token_bytes)}s', len(token_bytes), token_bytes)
    SQL_COPT_SS_ACCESS_TOKEN = 1256
    conn = pyodbc.connect(connection_string, attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token_struct})
    return conn
