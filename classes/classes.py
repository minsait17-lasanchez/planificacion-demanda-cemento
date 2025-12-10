"""_summary_.

PROYECTO           : [PRJ-25-002] CDS - PLANIFICACIÓN DE LA DEMANDA
NOMBRE             : classes
ARCHIVOS  DESTINO  : Tablas en BigQuery
ARCHIVOS  FUENTES  : Tablas en BigQuery
OBJETIVO           : Definir la clase Planificador Demanda PT Cemento
TIPO               : PY
OBSERVACION        : -
SCHEDULER          : CLOUD RUN
VERSION            : 1.0
DESARROLLADOR      : SÁNCHEZ AGUILAR LUIS ÁNGEL
PROVEEDOR          : MINSAIT
FECHA              : 10/12/2025
DESCRIPCION        : Clase que contiene funciones para la planificación comercial de producto terminado (cemento).
"""           

# Librerías Propias
from utils.utils import readJsonFile  
from classes._01_managedbstorages import GestorAlmacenDatos
from classes._02_preparedata import GestorPreparacionDatos
from classes._04_managemodel import GestorModelo
from classes._05_forecastmonthly import GestorProyeccion
from classes._06_monitorforecast import GestorMonitoreo
from classes._07_simulateforecast import GestorSimulacion

# Librerías Básicas
import tempfile
import warnings
import logging
import shutil
import sys
import os

import datetime
from dateutil.relativedelta import relativedelta

# Librerías de GCP
from google.cloud import bigquery
from google.cloud import storage

# Configurar logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
warnings.filterwarnings("ignore")

class PlanificadorDemandaPTCemento:
    """Clase para la planificación de demanda de Producto Terminado - Cemento."""

    def __init__(self):
        """
        Inicializa la clase con el proyecto, dataset de BigQuery y acceso a modelo de Machine Learning.
        """
        logging.info("Inicializando la clase PlanificadorDemandaPTCemento...")

        self.bq_cliente = bigquery.Client()
        self.cs_cliente = storage.Client()

        # Leer parámetros del archivo JSON
        parameters = readJsonFile('./config/parameters.json')
        self.project_id_env = os.getenv("PROJECT_DDV_ID")
        self.dataset_id = parameters['variables_generales']['DATASET_DDV_ID']
        self.project_id_anl = os.getenv("PROJECT_ANL_ID")

        ####################################################################
        # CONSTANTES, VARIABLES Y QUERYS
        ####################################################################
        # CONSTANTES INFORMATIVAS
        self.clase_producto_log = parameters['variables_informativas']['TIPO_PRODUCTO_LOG']
        
        # CONSTANTES DE TIPO DE PRODUCTO
        self.clase_producto = parameters['variables_informativas']['TIPO_PRODUCTO_TABLA']
        self.TIPO_MODELO_ML = parameters['variables_informativas']['TIPO_MODELO_ML']
        self.TIPO_MODELO_SIMPLE = parameters['variables_informativas']['TIPO_MODELO_SIMPLE']
        self.TIPO_MODELO_0 = parameters['variables_informativas']['TIPO_MODELO_0']

        self.PROJECT_EXE = self.project_id_env #"pe-pacasmayo-cds-01anl-gcp-dev"

        # VARIABLES DE TABLAS INPUTS EN BIGQUERY
        self.PROJECT_ID = self.project_id_env #"pe-pacasmayo-cds-01ddv-gcp-prd"
        self.DATASET = self.dataset_id #"cp_ddv_0400"

        self.TABLE_CONSUMO_DEMANDA = parameters['variables_input_demanda_comercial']['TABLA']
        self.COLUMNA_CONSUMO_DEMANDA = parameters['variables_input_demanda_comercial']['CANTIDAD']
        self.COLUMNA_FECHA_CONSUMO_DEMANDA = parameters['variables_input_demanda_comercial']['FECHA']
        self.INPUT_CLASIFICACION = parameters['variables_input_demanda_comercial']['CLASIFICACION']
        self.INPUT_SOCIEDAD = parameters['variables_input_demanda_comercial']['SOCIEDAD']
        self.INPUT_CENTRO = parameters['variables_input_demanda_comercial']['CENTRO']
        self.INPUT_MATERIAL = parameters['variables_input_demanda_comercial']['MATERIAL']
        self.INPUT_MEDIDA = parameters['variables_input_demanda_comercial']['MEDIDA']
        self.INPUT_ULTIMO_CONSUMO = parameters['variables_input_demanda_comercial']['FECHA_ULTIMO_CONSUMO']

        self.PATH_CONSUMO_DEMANDA = self.PROJECT_ID + "." + self.DATASET + "." + self.TABLE_CONSUMO_DEMANDA
        self.COLUMNAS_CONSUMO_DEMANDA = [
            self.INPUT_CLASIFICACION, 
            self.INPUT_SOCIEDAD, 
            self.INPUT_CENTRO, 
            self.INPUT_MATERIAL, 
            self.INPUT_MEDIDA, 
            self.INPUT_ULTIMO_CONSUMO,
            self.COLUMNA_FECHA_CONSUMO_DEMANDA, 
            self.COLUMNA_CONSUMO_DEMANDA
        ]
        self.COLUMNAS_CONSUMO_DEMANDA_QUERY = ",\n\t".join(self.COLUMNAS_CONSUMO_DEMANDA)

        # VARIABLES DE TABLAS INTERMEDIAS
        self.mes_col = parameters['variables_generales']['COLUMNA_AGRUPACION_MES']

        # VARIABLES DE ARCHIVOS INPUTS EN CLOUD STORAGE
        self.CS_PROJECT = self.project_id_anl
        self.BUCKET_SKU_ANALIZADOS = os.getenv("BUCKET_ANL_ID")
        self.FILE_PATH_SKU_ANALIZADOS = parameters['variables_input_skus_analizados']['RUTA_ARCHIVO']
        self.SHEET_SKU_ANALIZADOS = parameters['variables_input_skus_analizados']['HOJA_ARCHIVO']
        self.PATH_SKU_ANALIZADOS = "gs://" + self.BUCKET_SKU_ANALIZADOS + "/" + self.FILE_PATH_SKU_ANALIZADOS

        # VARIABLES DE MODELO EN VERTEX AI
        self.MODEL_PROJECT = self.CS_PROJECT
        self.MODEL_BUCKET_NAME_GCS = os.getenv("BUCKET_ANL_ID")
        self.MODEL_REGION = parameters['variables_modelos_ml']['REGION']
        self.MODEL_NAME = parameters['variables_modelos_ml']['NOMBRE']
        self.MODEL_VERSION_LABEL = parameters['variables_modelos_ml']['IDENTIFICADOR_VERSION']
        self.MODEL_PREFIX_GCS = parameters['variables_modelos_ml']['PREFIX_GCS'] 
        self.MODEL_URI_GCS_GENERAL = "gs://" + self.MODEL_BUCKET_NAME_GCS + "/" + self.MODEL_PREFIX_GCS + "/"
        self.MODEL_TEMP_PATH = os.path.join(tempfile.gettempdir(), "ag_model_temp")
        os.makedirs(self.MODEL_TEMP_PATH, exist_ok=True)

        # VARIABLES DE TABLAS OUTPUTS EN BIGQUERY
        self.PROJECT_ID_OUTPUT = self.PROJECT_ID #"pe-pacasmayo-cds-01ddv-gcp-prd" #pe-pacasmayo-cds-01anl-gcp-dev
        self.DATASET_OUTPUT = self.DATASET #"cp_ddv_0400"

        self.TABLE_PROYECCION_DEMANDA = parameters['variables_output_proyeccion_demanda']['TABLA'] 
        self.COLUMNA_PERIODO_OUTPUT = parameters['variables_output_proyeccion_demanda']['PERIODO'] 

        self.TABLE_MONITOREO = parameters['variables_output_monitoreo_demanda']['TABLA'] 
        self.COLUMNA_PERIODO_MONITOREO = parameters['variables_output_monitoreo_demanda']['PERIODO'] 

        self.TABLA_PRE = self.PROJECT_ID_OUTPUT + "." + self.DATASET_OUTPUT + "."
        self.PATH_PROYECCION_DEMANDA = self.TABLA_PRE + self.TABLE_PROYECCION_DEMANDA
        self.PATH_MONITOREO = self.TABLA_PRE + self.TABLE_MONITOREO
 
        # VARIABLES DE RANGOS DE TIEMPO
        self.ventana_segmentacion = parameters['variables_rangos_meses']['SEGMENTACION'] 
        self.ventana_historico = parameters['variables_rangos_meses']['HISTORICO'] 
        self.num_meses_proyeccion = parameters['variables_rangos_meses']['PROYECCION'] 
        self.ventana_ventas = parameters['variables_rangos_meses']['VENTAS'] 

        # VARIABLES - UMBRALES DE SEGMENTACION ABC - XYZ - FSN
        self.abc_umb_inf = parameters['variables_umbrales_segmentacion']['ABC_INFERIOR'] 
        self.abc_umb_sup = parameters['variables_umbrales_segmentacion']['ABC_SUPERIOR'] # C, B, A
        self.xyz_umb_inf = parameters['variables_umbrales_segmentacion']['XYZ_INFERIOR'] 
        self.xyz_umb_sup = parameters['variables_umbrales_segmentacion']['XYZ_SUPERIOR'] # X, Y, Z
        self.fsn_umb_inf = parameters['variables_umbrales_segmentacion']['FSN_INFERIOR'] 
        self.fsn_umb_sup = parameters['variables_umbrales_segmentacion']['FSN_SUPERIOR'] # N, S, F

        # VARIABLES - UMBRALES DE SEGMENTACION RFM
        self.r_umb_inf = parameters['variables_umbrales_segmentacion']['R_INFERIOR']
        self.r_umb_sup = parameters['variables_umbrales_segmentacion']['R_SUPERIOR'] # 3, 2, 1
        self.f_umb_inf = parameters['variables_umbrales_segmentacion']['F_INFERIOR']
        self.f_umb_sup = parameters['variables_umbrales_segmentacion']['F_SUPERIOR'] # 1, 2, 3

        # CONSTANTE DE CONFIGURACIÓN DE AUTOGLUON
        self.MODELO_AUTOGLUON = parameters['variables_modelos_autogluon']['NOMBRE']
        self.MODELO_AUTOGLUON_PATH = parameters['variables_modelos_autogluon']['RUTA_AUTOGLUON']
        self.MODELO_AUTOGLUON_SUFFIX = parameters['variables_modelos_autogluon']['SUFIJO']
        self.MODELO_AUTOGLUON_METRICA_EVALUACION = parameters['variables_modelos_autogluon']['METRICA_EVALUACION']
        self.TIEMPO_ENTRENAMIENTO_MAXIMO = parameters['variables_modelos_autogluon']['TIEMPO_ENTRENAMIENTO_MAX']
        self.CONFIGURACION_AUTOGLUON = {
            self.MODELO_AUTOGLUON: [
                {
                    "model_path": self.MODELO_AUTOGLUON_PATH,
                    "ag_args": {"name_suffix": self.MODELO_AUTOGLUON_SUFFIX}
                }
            ]
        }
        self.CONFIGURACION_AUTOGLUON_PREDICTOR = self.MODELO_AUTOGLUON + self.MODELO_AUTOGLUON_SUFFIX + "[" + self.MODELO_AUTOGLUON_PATH + "]"

        logging.info("Clase inicializada correctamente.")

        self.DataManager = GestorAlmacenDatos(self.bq_cliente, self.cs_cliente)
        self.PreManager = GestorPreparacionDatos(self.COLUMNA_CONSUMO_DEMANDA)
        self.ModelManager = GestorModelo(self.cs_cliente, self.ventana_segmentacion, self.num_meses_proyeccion, self.MODEL_TEMP_PATH)
        self.ForecastManager = GestorProyeccion(self.COLUMNA_FECHA_CONSUMO_DEMANDA, self.num_meses_proyeccion, self.ventana_ventas, self.CONFIGURACION_AUTOGLUON_PREDICTOR, self.TIPO_MODELO_ML, self.TIPO_MODELO_SIMPLE, self.TIPO_MODELO_0, self.ModelManager)
        self.MonitorManager = GestorMonitoreo(self.COLUMNA_CONSUMO_DEMANDA, self.COLUMNA_FECHA_CONSUMO_DEMANDA)
        self.SimulationManager = GestorSimulacion(self)

    ####################################################################
    # LÓGICA PRINCIPAL DEL JOB
    ####################################################################
    def ejecutar(self):
        if datetime.now() <= datetime(2025, 12, 11):
            fecha_final_proyeccion = datetime.now()
            fecha_inicial_proyeccion = fecha_final_proyeccion - relativedelta(months=self.num_meses_proyeccion)

            rango_meses_simulacion = []
            while fecha_inicial_proyeccion <= fecha_final_proyeccion:
                rango_meses_simulacion.append(fecha_inicial_proyeccion)
                fecha_inicial_proyeccion += relativedelta(months=1)

            for mes in rango_meses_simulacion:
                self.SimulationManager.ejecutar_simulacion(mes)
                if os.path.exists(self.MODEL_TEMP_PATH):
                    shutil.rmtree(self.MODEL_TEMP_PATH)
        else:
            self.SimulationManager.ejecutar_simulacion(datetime.now())

        sys.exit(0)  # Finaliza el proceso y cierra Cloud Run
        