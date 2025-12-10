"""_summary_.

PROYECTO           : [PRJ-25-002] CDS - PLANIFICACIÓN DE LA DEMANDA
NOMBRE             : 01_managedatastorages
ARCHIVOS  DESTINO  : ---
ARCHIVOS  FUENTES  : ---
OBJETIVO           : Definir procesos para obtener y manejar data y sus fuentes
TIPO               : PY
OBSERVACION        : -
SCHEDULER          : CLOUD RUN
VERSION            : 1.0
DESARROLLADOR      : SÁNCHEZ AGUILAR LUIS ÁNGEL
PROVEEDOR          : MINSAIT
FECHA              : 10/12/2025
DESCRIPCION        : Paquete que contiene procesos para obtener y manejar data y sus fuentes
"""

# Librerías Básicas
import logging
import io

# Librerías para Datos
import pandas as pd
import pandas_gbq

# Librerías de GCP
from google.cloud import bigquery
from google.cloud import storage
from google.api_core import exceptions as google_exceptions

class GestorAlmacenDatos:
    """Clase para la gestión de datos y sus fuentes."""

    def __init__(self, bq_cliente, cs_cliente):
        """
        Inicializa la clase.
        """
        logging.info("Inicializando la clase de Gestor de Fuentes de Datos...")

        self.bq_cliente = bq_cliente
        self.cs_cliente = cs_cliente
    
    ###################################################################################
    # FUNCIÓN PARA CONFIRMAR SI UNA QUERY SE EJECUTA CORRECTAMENTE O NO
    ###################################################################################
    def verificar_tabla(self, query_validation):
        """
        Función para verificar si una tabla existe.
        """

        # ACCEDEMOS AL CLIENTE DE BIGQUERY E INTENTAMOS VER SI HAY RESULTADOS DEL MES
        try:
            query_validation_client = self.bq_cliente.query(query_validation)
            results = query_validation_client.result()
            first_result = next(iter(results), None)
            return True
        except google_exceptions.NotFound:
            return False
        except google_exceptions.Forbidden as e:
            return False
        except Exception as e:
            raise Exception("Error al intentar validar si existe alguna tabla.") from e

      ###################################################################################
    # FUNCIÓN PARA CONFIRMAR SI UNA QUERY TIENE RESULTADOS O NO
    ###################################################################################
    def verificar_resultados_tabla(self, query_validation):
        """
        Función para verificar si una tabla y sus resultados de query existen.
        """

        # ACCEDEMOS AL CLIENTE DE BIGQUERY E INTENTAMOS VER SI HAY RESULTADOS DEL MES
        try:
            query_validation_client = self.bq_cliente.query(query_validation)
            results = query_validation_client.result()
            first_result = next(iter(results), None)
            return True if first_result is not None else False
        except google_exceptions.NotFound:
            raise Exception("No existe la tabla.") from e
        except google_exceptions.Forbidden as e:
            raise Exception("No existe la tabla.") from e
        except Exception as e:
            raise Exception("Error al intentar validar si existe alguna tabla.") from e

    ###################################################################################
    # FUNCIÓN PARA CONFIRMAR SI UN ARCHIVO EN UN BUCKET EXISTE O NO
    ###################################################################################
    def verificar_archivo_gcs(self, bucket_name, file_path):
        """
        Función para verificar si un archivo existe o no en un bucket/ ruta de cloud storage
        """
        # CREAR OBJETO BUCKET Y ARCHIVO
        bucket = self.cs_cliente.bucket(bucket_name)     
        blob = bucket.blob(file_path)

        # INTENTAR CONFIRMAR SI EXISTE EL ARCHIVO
        try:
            return blob.exists()    
        except google_exceptions.NotFound:
            return False
        except Exception as e:
            raise Exception("Error al intentar validar si existe el archivo.") from e

    ###################################################################################
    # FUNCIÓN PARA CONVERTIR XLSX UBICADO EN CLOUD STORAGE EN UN DATAFRAME DE LOS SKUS DE DESARROLLO
    ###################################################################################
    def obtener_sku_analizados(self, bucket_name, file_path, sheet_name):
        """
        Función para obtener los SKU analizados en desarrolllo almacenados en un xlsx en Cloud Storage
        """
        # CREAR OBJETO BUCKET Y ARCHIVO
        bucket = self.cs_cliente.bucket(bucket_name)     
        blob = bucket.blob(file_path)

        # DESCARGAR EL ARCHIVO DE GCS EN BYTES
        try:
            excel_bytes = blob.download_as_bytes()
        except Exception as e:
            print(f"Error al descargar el archivo desde GCS: {e}")
            raise
        
        # SIMULAR BYTES COMO ARCHIVO EN DISCO
        excel_file = io.BytesIO(excel_bytes)

        # LEER ARCHIVO EXCEL Y CONVERTIR EN DATAFRAME
        df = pd.read_excel(excel_file, sheet_name=sheet_name)
        
        return df
    
    ###################################################################################
    # FUNCIÓN PARA OBTENER ESQUEMA DE BIGQUERY A PARTIR DE DATAFRAME
    ###################################################################################
    def obtener_esquema_de_dataframe(self, df):
        """
        Función para generar la lista de bigquery.SchemaField mapeando los dtypes de Pandas de un dataframe.
        """
        dtype_mapping = {
            'object': 'STRING',
            'int64': 'INT64',
            'float64': 'FLOAT64',
            'bool': 'BOOL',
            'boolean': 'BOOL',
            'datetime64[ns]': 'TIMESTAMP', 
            'datetime64[us, UTC]': 'TIMESTAMP',
            'int32': 'INT64'
        }
        
        schema = []
        
        for column, dtype in df.dtypes.items():
            dtype_str = str(dtype)
            bq_type = dtype_mapping.get(dtype_str)
            
            if bq_type is None:
                if dtype_str[:13] == "datetime64[us":
                    bq_type = "TIMESTAMP"
                else:
                    bq_type = "STRING"
                
            schema.append(
                bigquery.SchemaField(column, bq_type)
            )
            
        return schema
    
    ###################################################################################
    # FUNCIÓN PARA CREAR TABLA PARTICIONADA
    ###################################################################################
    def crear_tabla_particionada(self, nueva_tabla, df_schema, columna_particionada):
        """
        Función para crear tablas particionadas en caso no existan.
        """
        #CONFIGURAR PARTICIÓN POR RANGO DE ENTEROS
        range_config = bigquery.RangePartitioning(
            field=columna_particionada,
            range_=bigquery.PartitionRange(
                start = 202301,    
                end = 211812,
                interval = 1,
            )
        )

        # CREAR LA TABLA PARTICIONADA
        schema = self.obtener_esquema_de_dataframe(df_schema)
        table = bigquery.Table(nueva_tabla, schema=schema)
        table.range_partitioning = range_config
        self.bq_cliente.create_table(table) 

    ###################################################################################
    # CARGAR DATOS EN BIGQUERY
    ###################################################################################
    def cargar_datos_bigquery(self, df, path_table, project_exe, if_exists = 'replace'):
        """
        Cargar Datos en Bigquery.
        """
        # CARGAR LA DATA
        pandas_gbq.to_gbq(
            df,
            destination_table=path_table,
            project_id=project_exe,
            if_exists=if_exists
        )