"""_summary_.

PROYECTO           : [PRJ-25-002] CDS - PLANIFICACIÓN DE LA DEMANDA
NOMBRE             : 04_managemodel
ARCHIVOS  DESTINO  : ---
ARCHIVOS  FUENTES  : ---
OBJETIVO           : Definir procesos para manejar las distintas necesidades de utilizar un modelo de machine learning
TIPO               : PY
OBSERVACION        : -
SCHEDULER          : CLOUD RUN
VERSION            : 1.0
DESARROLLADOR      : SÁNCHEZ AGUILAR LUIS ÁNGEL
PROVEEDOR          : MINSAIT
FECHA              : 10/12/2025
DESCRIPCION        : Paquete que contiene procesos para manejar las distintas necesidades de utilizar un modelo de machine learning
"""

# Librerías Básicas
from urllib.parse import urlparse
import logging
import os

# Librerías para Datos
import pandas as pd
import numpy as np

# Librerías para Auto Selección de Modelos
from autogluon.timeseries import TimeSeriesPredictor
from autogluon.timeseries import TimeSeriesDataFrame

# Librerías de GCP
from google.cloud import aiplatform

class GestorModelo:
    """Clase para la gestión de los procesos que manejan las distintas necesidades de utilizar un modelo de machine learning."""

    def __init__(self, cs_cliente, window, months, temp_path):
        """
        Inicializa la clase.
        """
        logging.info("Inicializando la clase de Gestor de Modelo...")

        self.cs_cliente = cs_cliente
        self.ventana_segmentacion = window
        self.num_meses_proyeccion = months
        self.MODEL_TEMP_PATH = temp_path

    ###################################################################################
    # FUNCIÓN PARA VERIFICAR LA EXISTENCIA DEL MODELO BASE Y SU VERSIÓN DE MES ACTUAL
    ###################################################################################
    def verificar_modelo(self, model_name, version_label, version_model, project_id, region):
        """
        Función para verificar la existencia del modelo base y la versión del mes actual
        """
        aiplatform.init(project = project_id, location = region)
        all_versions = []

        # BUSCAR LA EXISTENCIA DEL MODELO EN SÍ
        try:
            models = aiplatform.Model.list(filter=f"display_name={model_name}")

            # SI NO SE ENCUENTRA MODELO CON EL NOMBRE, SE RETORNA FALSE Y LISTA VACÍA DE VERSIONES
            if not models:
                return False, []

            # ELEGIR EL PRIMER MODELO QUE COINCIDE CON EL NOMBRE Y OBTENER LA LISTA DE TODAS LAS VERSIONES
            base_model = models[0].resource_name
            modelo_padre = aiplatform.models.ModelRegistry(model=base_model)
            all_versions = modelo_padre.list_versions()

            # VERIFICAR SI EXISTE LA VERSIÓN DEL MES ACTUAL DEL MODELO
            version_del_mes_actual_existe = False

            for version in all_versions:
                version_id = version.version_id
                version_resource_name = f"{base_model}@{version_id}"
                model_version = aiplatform.Model(version_resource_name)
                labels = model_version.labels
                if labels and labels.get(version_label) == version_model:
                    version_del_mes_actual_existe = True
                    break
            return version_del_mes_actual_existe, all_versions

        except Exception as e:
            raise Exception("Error al intentar validar modelo.") from e

    ###################################################################################
    # FUNCIÓN PARA DESCARGAR EN EL ENTORNO LOCAL DE CLOUD STORAGE
    ###################################################################################
    def descargar_modelo_de_gcs(self, gcs_uri, local_dir):
        """
        Función para descargar en el entorno local de Cloud Storage
        """
        if not gcs_uri.startswith("gs://"):
            raise ValueError(f"Ruta GCS inválida: {gcs_uri}")

        # SEPARAR EN NOMBRE DE BUCKET Y EL RESTO DE LA URI
        bucket_name, prefix = gcs_uri.replace("gs://", "").split("/", 1)

        # CREAR LISTA DE ARCHIVOS DEL BUCKET
        blobs = self.cs_cliente.list_blobs(bucket_name, prefix=prefix)

        for blob in blobs:
            # DESCARGAR EL CONTENIDO
            rel_path = blob.name[len(prefix):].lstrip("/")
            local_path = os.path.join(local_dir, rel_path)
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            blob.download_to_filename(local_path)

    ###################################################################################
    # FUNCIÓN PARA OBTENER EL MODELO DE LA VERSIÓN ACTUAL
    ###################################################################################
    def obtener_modelo(self, model_name, version_label, version_model, project_id, region):
        """
        Función para obtener el modelo de la versión del mes actual
        """
        LOCAL_MODEL_DIR = "/temp_propio/cemento_chronos_model"

        aiplatform.init(project = project_id, location = region)

        # OBTENER MODELO POR EL NOMBRE CON TODAS SUS VERSIONES
        models = aiplatform.Model.list(filter=f"display_name={model_name}")
        base_model = models[0].resource_name

        modelo_padre = aiplatform.models.ModelRegistry(model=base_model)
        all_versions = modelo_padre.list_versions()

        for version in all_versions:
            version_id = version.version_id
            version_resource_name = f"{base_model}@{version_id}"
            model_version = aiplatform.Model(version_resource_name)
            labels = model_version.labels
            if labels and labels.get(version_label) == version_model:
                break
    
        version_model_uri = model_version.uri

        self.descargar_modelo_de_gcs(
            gcs_uri = version_model_uri,
            local_dir = self.MODEL_TEMP_PATH
        )

        modelo_autogluon = TimeSeriesPredictor.load(self.MODEL_TEMP_PATH)
        return modelo_autogluon
    
    ###################################################################################
    # FUNCIÓN PARA CREAR FEATURES DE TIEMPO QUE CAPTEN CICLOS, PERIODOS, ETC
    ###################################################################################
    def crear_time_features(self, data, id_cols, date_col, value_col, num_month = 12, there_is_period = False):
        df_copy = data.copy()

        # ASEGURAMOS EL ORDEN PARA CREAR VARIABLES DE LAGS DE TIEMPO CORRECTAMENTE
        df_copy = df_copy.sort_values(by = id_cols + [date_col])

        # CREAR VARIABLES DE LAGS DE TIEMPO
        for lag in range(1, num_month):
            df_copy[f'{value_col}_LAG{str(lag)}'] = df_copy.groupby(id_cols)[value_col].shift(lag).interpolate(method='linear').fillna(method='ffill').fillna(method='bfill')

        # CREAR VARIABLES DE PROMEDIO DE VENTANAS DE TIEMPO
        for window in range(2, num_month):
            df_copy[f'{value_col}_MEAN{str(window)}'] = df_copy.groupby(id_cols)[value_col].shift(1).rolling(window=window).mean().interpolate(method='linear').fillna(method='ffill').fillna(method='bfill')

        # SEPARAR LA FECHA EN VARIABLES DE MES Y AÑO
        if there_is_period:
            df_copy['MES'] = (df_copy[date_col] - pd.DateOffset(months=1)).dt.month.interpolate(method='linear').fillna(method='ffill').fillna(method='bfill')
            df_copy['ANIO'] = (df_copy[date_col] - pd.DateOffset(months=1)).dt.year.interpolate(method='linear').fillna(method='ffill').fillna(method='bfill')

        return df_copy

    ###################################################################################
    # FUNCIÓN PARA PREPARAR DATAFRAME COMPATIBLE CON AUTOGLUON
    ###################################################################################
    def preparar_para_autogluon(self, df, id_skus, mes_col, val_col, prefix_str, need_dynamic_time_features = False, need_known_time_features = False, need_static_segment_features = False):
        """
        Función para preparar dataframe compatible con Autogluon
        """
        # ADAPTAR COLUMNAS QUE ENTIENDE LA LIBRERÍA AUTOGLUON PARA ENTRENAR MODELO
        df['item_id'] = df[id_skus].astype(str).agg('_'.join, axis=1)
        df[mes_col] = pd.to_datetime(df[mes_col])
        df = df.rename(columns={
            mes_col: 'timestamp',
            val_col: 'target'
        })

        # DEFINIR VARIABLES ESTÁNDAR, DE LAG Y PROMEDIOS DE VENTANAS DE TIEMPO
        col_autogluon_ini = ['item_id', 'timestamp', 'target']
        features_base_num = []
        if need_dynamic_time_features:
            features_base_num = features_base_num + [f"{val_col}_LAG{str(i)}" for i in range(1, self.num_meses_proyeccion)]
            features_base_num = features_base_num + [f"{val_col}_MEAN{str(i)}" for i in range(2, self.num_meses_proyeccion)]
        
        # VARIABLES DINÁMICAS EN EL TIEMPO NO CONOCIDAS EN EL FUTURO
        dynamic_covariates = col_autogluon_ini + features_base_num

        # DEFINIR VARIABLES DESCRIPTIVAS DEL TIEMPO
        df_ag = df[dynamic_covariates].copy()

        known_covariates = []
        if need_known_time_features:
            # ESTACIONALIDAD MENSUAL (CICLO DE 12 MESES)
            df_ag['mes'] = df_ag['timestamp'].dt.month
            df_ag['mes_sin'] = np.sin(2 * np.pi * df_ag['mes'] / 12)
            df_ag['mes_cos'] = np.cos(2 * np.pi * df_ag['mes'] / 12)

            # ESTACIONALIDAD TRIMESTRAL (CICLO DE 4 TRIMESTRES)
            df_ag['trimestre'] = df_ag['timestamp'].dt.quarter
            df_ag['trim_sin'] = np.sin(2 * np.pi * df_ag['trimestre'] / 4)
            df_ag['trim_cos'] = np.cos(2 * np.pi * df_ag['trimestre'] / 4)

            # ESTACIONALIDAD SEMESTRAL (CICLO DE 2 SEMESTRES)
            df_ag['semestre'] = np.where(df_ag['mes'] <= 6, 1, 2)
            df_ag['sem_sin'] = np.sin(2 * np.pi * df_ag['semestre'] / 2)
            df_ag['sem_cos'] = np.cos(2 * np.pi * df_ag['semestre'] / 2)

            # CONTADOR DE MESES LINEAL
            fecha_min = df_ag['timestamp'].min()
            df_ag['contador_lineal'] = (
                (df_ag['timestamp'].dt.year - fecha_min.year) * 12 +
                (df_ag['timestamp'].dt.month - fecha_min.month)
            )

            # CONTADOR DE MESES CUADRÁTICO
            df_ag['contador_cuadratico'] = df_ag['contador_lineal']**2

            # CONTADOR DE MESES LOGARÍTMICO
            df_ag['contador_log'] = np.log1p(df_ag['contador_lineal'])

            # VARIABLES CONOCIDAS EN EL FUTURO
            known_covariates = ["mes", "mes_sin", "mes_cos", "trimestre", "trim_sin", "trim_cos", "semestre", "sem_sin", "sem_cos", "contador_lineal", "contador_cuadratico", "contador_log"]

        df_ag = df_ag.reset_index(drop = True)
        
        if need_static_segment_features:
            # VARIABLES ESTÁTICAS EN EL TIEMPO NO CONOCIDAS EN EL FUTURO
            ventana_segmentacion_str = str(self.ventana_segmentacion).zfill(2)
    
            if not prefix_str:
                prefix_str = val_col

            abc_col_demanda = f"ABC{prefix_str}{ventana_segmentacion_str}M"
            xyz_col_demanda = f"XYZ{prefix_str}{ventana_segmentacion_str}M"
            fsn_col_demanda = f"FSN{prefix_str}{ventana_segmentacion_str}M"
            static_covariates_df = df[["item_id", abc_col_demanda, xyz_col_demanda, fsn_col_demanda]].drop_duplicates(subset=["item_id"]).reset_index(drop = True)

            # CREAR DATAFRAME DE SERIES DE TIEMPO PARA AUTOGLUON CON VARIABLES ESTÁTICAS
            df_ts_ag = TimeSeriesDataFrame.from_data_frame(
                df = df_ag[dynamic_covariates + known_covariates],
                id_column = "item_id",
                timestamp_column = "timestamp",
                static_features_df = static_covariates_df,
            )

        else:
            # CREAR DATAFRAME DE SERIES DE TIEMPO PARA AUTOGLUON SIN VARIABLES ESTÁTICAS
            df_ts_ag = TimeSeriesDataFrame.from_data_frame(
                df = df_ag[dynamic_covariates + known_covariates],
                id_column = "item_id",
                timestamp_column = "timestamp"
            )

        return df_ts_ag, known_covariates

    ###################################################################################
    # FUNCIÓN PARA ENTRENAR UN MODELO DE AUTOGLUON
    ###################################################################################
    def entrenar_modelo_autogluon(self, df_entrenamiento, id_skus, mes_col, val_col, prefix_str):
        """
        Función para entrenar modelo de Autogluon
        """
        df = df_entrenamiento.copy()

        # PREPARAR DATAFRAME DE AUTOGLUON COMPATIBLE CON EL ENTRENAMIENTO
        df_ts_ag, known_covariates = self.preparar_para_autogluon(
            df, 
            id_skus = id_skus, 
            mes_col = mes_col, 
            val_col = val_col, 
            prefix_str = prefix_str,
            need_dynamic_time_features = True, 
            need_known_time_features = True, 
            need_static_segment_features = True
        )

        # SE CONFIGURA EL PREDICTOR
        predictor_mae_18_time = TimeSeriesPredictor(
            prediction_length = self.num_meses_proyeccion,
            label = 'target',
            eval_metric = self.MODELO_AUTOGLUON_METRICA_EVALUACION,
            path = self.MODEL_TEMP_PATH,
            known_covariates_names=known_covariates,
            quantile_levels = [0.05, 0.25, 0.5, 0.75, 0.95]
        )

        # SE ENTRENA EL PREDICTOR CON EL DATAFRAME SERIE DE TIEMPO DE AUTOGLUON
        predictor_mae_18_time.fit(
            train_data = df_ts_ag,
            time_limit = self.TIEMPO_ENTRENAMIENTO_MAXIMO,
            num_val_windows = 12,
            presets='high_quality',
            hyperparameters = self.CONFIGURACION_AUTOGLUON,
            verbosity = 0
        )

        return predictor_mae_18_time
    
    ###################################################################################
    # FUNCIÓN PARA GUARDAR EL MODELO EN GOOGLE CLOUD STORAGE
    ###################################################################################
    def guardar_modelo_gcs(self, gcs_uri):
        """
        Función para guardar el modelo en Google Cloud Storage
        """
        parsed_uri = urlparse(gcs_uri)
        bucket_name = parsed_uri.netloc
        path_gcs_sin_bucket = parsed_uri.path.lstrip('/')

        bucket = self.cs_cliente.bucket(bucket_name)

        for root, _, files in os.walk(self.MODEL_TEMP_PATH):
            for file in files:
                file_path = os.path.join(root, file)
                relative_path = os.path.relpath(file_path, self.MODEL_TEMP_PATH)
                blob_path = os.path.join(path_gcs_sin_bucket, relative_path)
                
                blob = bucket.blob(blob_path)
                blob.upload_from_filename(file_path)

    ###################################################################################
    # FUNCIÓN PARA ACTUALIZAR EL MODELO AL MES ACTUAL
    ###################################################################################
    def actualizar_modelo(self, df_entrenamiento, id_skus, mes_col, val_col, prefix_str, versiones, model_name, version_label, version_model, project_id, region, uri_gcs, prefix_gcs):
        """
        Función para actualizar el modelo al mes actual
        """
        version_aliases = ["last-training", version_model]
        version_description = f"Modelo Chronos para Cemento - Versión Mensual {version_model}"

        aiplatform.init(project = project_id, location = region)
        current_version_artifact_uri = os.path.join(uri_gcs, f"{version_model}") + "/"

        # DEFINIR LOS DATOS PARA ENTRENAMIENTO Y AGREGAR VARIABLES DE TIEMPO
        df_train = df_entrenamiento.copy()
        df_sf_time = self.crear_time_features(
            data = df_train,
            id_cols = id_skus,
            date_col = mes_col,
            value_col = val_col,
            num_month = self.num_meses_proyeccion
        )      

        # ENTRENAR MODELO DE AUTOGLUON CON LA NUEVA DATA Y GUARDARLO EN GCS
        modelo_entrenado = self.entrenar_modelo_autogluon(
            df_sf_time, 
            id_skus = id_skus, 
            mes_col = mes_col, 
            val_col = val_col, 
            prefix_str = prefix_str
        )

        # GUARDAR MODELO EN GOOGLE CLOUD STORAGE
        self.guardar_modelo_gcs(current_version_artifact_uri)
        
        
        # CUANDO NO HAY NINGÚN MODELO CON EL NOMBRE INDICADO
        if not versiones: 
            # REGISTRAR EL MODELO POR PRIMERA VEZ CUANDO NO HAY NINGÚN MODELO
            try:
                model = aiplatform.Model.upload(
                    display_name = model_name,
                    artifact_uri = current_version_artifact_uri,
                    serving_container_image_uri = "gcr.io/deeplearning-platform-release/base-cpu",
                    is_default_version = True,
                    version_aliases = version_aliases,
                    version_description = version_description,
                    labels = {version_label: version_model},          # útil para filtrar en el registry
                    sync = True
                )

                return modelo_entrenado
            except Exception as e:
                raise("No se pudo registrar Modelo Por Primera Vez")

        # CUANDO SI HAY MODELO CON EL NOMBRE INDICADO PERO NO VERSIÓN ACTUALIZADA
        else:
            # OBTENEMOS EL NOMBRE DEL MODELO PADRE
            models = list(aiplatform.Model.list(filter=f'display_name="{model_name}"'))
            parent_model_name = models[0].resource_name if models else None
            
            # REGISTRAR LA NUEVA VERSIÓN DEL MODELO ACTUALIZADO AL MES
            try:
                model = aiplatform.Model.upload(
                    display_name = model_name,
                    artifact_uri = current_version_artifact_uri,
                    serving_container_image_uri = "gcr.io/deeplearning-platform-release/base-cpu",
                    parent_model = parent_model_name,
                    is_default_version = True,
                    version_aliases = version_aliases,
                    version_description = version_description,
                    labels = {version_label: version_model},          # útil para filtrar en el registry
                    sync = True
                )

                return modelo_entrenado
            except Exception as e:
                raise("No se pudo registrar el Modelo Actualizado")