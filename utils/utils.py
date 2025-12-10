"""_summary_.

PROYECTO           : [PRJ-25-002] CDS - PLANIFICACIÓN DE LA DEMANDA
NOMBRE             : utils
ARCHIVOS  DESTINO  : ---
ARCHIVOS  FUENTES  : ---
OBJETIVO           : Definir funciones que se usarán en la ejecución general.
TIPO               : PY
OBSERVACION        : -
SCHEDULER          : CLOUD RUN
VERSION            : 1.0
DESARROLLADOR      : SÁNCHEZ AGUILAR LUIS ÁNGEL
PROVEEDOR          : MINSAIT
FECHA              : 10/12/2025
DESCRIPCION        : Paquete que contiene funciones necesarias que
se utilizaran durante la ejecución.
"""
# seccion Imports
import json
import pandas as pd

# seccion Funciones
def readJsonFile(oriPathFileJson=""):
    """Abre y carga archivo json.

    :param oriPathFileJson: Ruta del archivo json, por defecto es vacío
    :type oriPathFileJson: str, opcional
    :raises Exception: Mensaje de error en caso no se pueda abrir el archivo.
    :return: Archivo json
    :rtype: json
    """
    try:
        f = open(oriPathFileJson, "r")
        data = json.loads(f.read())
        return data
    except Exception as e:
        raise Exception(e)

def convert_df_to_bq_schema(df):
    """
    Convierte los tipos de datos de las columnas de un DataFrame a sus similares en BigQuery.

    :param df: DataFrame a convertir
    :type df: pandas.DataFrame
    :return: DataFrame con los tipos de datos convertidos
    :rtype: pandas.DataFrame
    """
    for column in df.columns:
        if pd.api.types.is_integer_dtype(df[column]):
            df[column] = df[column].astype('Int64')
        elif pd.api.types.is_float_dtype(df[column]):
            df[column] = df[column].astype('float64')
        elif pd.api.types.is_bool_dtype(df[column]):
            df[column] = df[column].astype('bool')
        elif pd.api.types.is_datetime64_any_dtype(df[column]):
            df[column] = df[column].astype('datetime64[ns]')
        else:
            df[column] = df[column].astype('str')
    return df