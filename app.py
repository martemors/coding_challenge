""" Disclaimer: 
Hola! Este proyecto se encuentra en un estadío intermedio entre pseudo-código y código desarrollado en python, propiamente dicho.
Su intención es demostrar un concepto, una secuencia de pasos lógicos para poder abordar las secciones 1 y 2 del Globant's Data Engineering Coding Challenge.
No tiene por intención ser un código concluyente, finalizado y en funcionamiento.
"""

import os # para acceder a las environment variables
import pyodbc # controlador pyodbc para establecer las conexiones a la base de datos SQL usando python.
import struct # módulo para parsear los tokens a strings.
import pandas as pd #librería para manipular los csv.

from azure import identity
from azure.storage.blob import BlobServiceClient
from flask import Flask, request, jsonify
#Storing variables in config file
from config import *

#Set app
app = Flask(__name__)

# Azure SQL Database connection string
AZURE_SQL_CONNECTIONSTRING = os.environ[AZURE_SQL_CONNECTIONSTRING]

# Azure SQL Database access token configuration
SQL_COPT_SS_ACCESS_TOKEN = SQL_COPT_SS_ACCESS_TOKEN

# Conn to Azure database
def get_conn(connection_string: str):
    credential = identity.DefaultAzureCredential(exclude_interactive_browser_credential=False)
    token_bytes = credential.get_token(TOKEN_URL).token.encode("UTF-16-LE")
    token_struct = struct.pack(f'<I{len(token_bytes)}s', len(token_bytes), token_bytes)
    SQL_COPT_SS_ACCESS_TOKEN = SQL_COPT_SS_ACCESS_TOKEN
    conn = pyodbc.connect(connection_string, attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token_struct})
    return conn


"""
Section 1: API. Data ingestion to DB.
"""

# Elegí Azure blob storage como source para los csv / punto de inicio del workflow. 
# Azure Blob Storage config
AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
container_name = os.getenv(container_name)
blob_name = os.getenv(blob_name)
target_table_name = os.getenv(target_table_name)


@app.route('/upload_blob_to_sql', methods=['POST'])
def upload_blob_to_sql():
    try:
        # Acceso al blob
        blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
        container_client = blob_service_client.get_container_client(container_name)
        blob_client = container_client.get_blob_client(blob_name)
        
         # Check if blob exists
        if not blob_client.exists():
            return jsonify({'error': 'Blob not found'}), 400

        # Read CSV file from Blob storage
        csv_content = blob_client.download_blob().readall().decode('utf-8')
        df = pd.read_csv(pd.compat.StringIO(csv_content))

        # Connect to Azure SQL Database
        conn = pyodbc.connect(AZURE_SQL_CONNECTION_STRING)
        cursor = conn.cursor()

        # Insert data into existing table in Azure SQL Database. 
        df.to_sql(target_table_name, conn, index=False, if_exists='append')

        # Close the connection
        conn.close()

        # If succesful:
        return jsonify({'message': 'Data uploaded successfully'}), 200
    
    # If failed:
    except Exception as e:
        return jsonify({'error': str(e)}), 500


"""
Section 2: SQL. Endpoints for stakeholder's requirements.
"""

# Endpoint to get
@app.route('/employees_by_job_and_department', methods=['GET'])
def employees_by_job_and_department():
    try:
        conn = pyodbc.connect(AZURE_SQL_CONNECTIONSTRING)
        cursor = conn.cursor()
        #Execution of SQL query in endpoint_queries/employees_by_job_and_department.
        cursor.execute(employees_by_job_and_department.sql)
        column_names = [desc[0] for desc in cursor.description]
        data = cursor.fetchall()
        df = pd.DataFrame(data, columns=column_names)
        response = df.to_dict()
        # If succesful:
        return jsonify(response), 200
    # If failed:
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/employees_by_high_hiring_departments_2021', methods=['GET'])
def employees_by_high_hiring_departments_2021():
    try:
        conn = pyodbc.connect(AZURE_SQL_CONNECTIONSTRING)
        cursor = conn.cursor()
        #Execution of SQL query in endpoint_queries/employees_by_high_hiring_departments_2021.
        cursor.execute(employees_by_high_hiring_departments_2021.sql) 
        column_names = [desc[0] for desc in cursor.description]
        data = cursor.fetchall()
        df = pd.DataFrame(data, columns=column_names)
        response = df.to_dict()
        # If succesful:
        return jsonify(response), 200
    # If failed:
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
# debug= True is a keeper, it's useful for detailed traceback when developing.
    app.run(debug=True)


"""
Bonus Track: Testing. Test Scenarios & approach.

    Tecnologías: Podría implementarse el uso de pytest para la automatización del unit testing y algun componente en dbt
    para validar los esquemas DDL (nombres de columnas, constrains, tipos de datos).

    1) Test: Verificar que mi script carga correctamente los archivos CSV a Azure SQL Database.
      - Mandar un POST request al endpoint de carga con un sample CSV file.
      - Confirmar a través de un assert que la respuesta es positiva (200)
      - Verificar que la data del archivo se haya insertado en la tabla correspondiente.

    2) Test: Verificar que el '/employees_by_job_and_department' endpoint devuelva la data esperada.
      - Ejecutar def employees_by_job_and_department.
      - Confirmar que se produce un JSON file con la información esperada.

    3) Test: Verificar que el '/employees_by_high_hiring_departments_2021' endpoint devuelva la data esperada.
      - Ejecutar def employees_by_high_hiring_departments_2021.
      - Confirmar que se produce un JSON file con la información esperada.

    4) Test: Evaluar comportamiento y respuesta a fallas [Etapa de carga]. Verificar que el script identifica y devuelve respuestas de error apropiadas.
      - Correr el ejecutable apuntando intencionalmente a un CSV no válido.
      - Confirmar a través de un assert que se recibe la respuesta de error esperada.

    5) Test: Evaluar comportamiento y respuesta a fallas [Etapa de conexión]. Verificar que el script identifica y devuelve respuestas de error apropiadas.  
      - Alterar los datos de conexión a la base de datos, alterar los permisos de acceso.
      - Confirmar a través de un assert que se recibe un error del servidor (500).

"""
