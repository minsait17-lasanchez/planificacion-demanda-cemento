"""_summary_.

PROYECTO           : [PRJ-25-002] CDS - PLANIFICACIÓN DE LA DEMANDA
NOMBRE             : 07_simulateforecast
ARCHIVOS  DESTINO  : ---
ARCHIVOS  FUENTES  : ---
OBJETIVO           : Definir procesos para simular la proyección en una fecha específica
OBSERVACION        : -
SCHEDULER          : CLOUD RUN
VERSION            : 1.0
DESARROLLADOR      : SÁNCHEZ AGUILAR LUIS ÁNGEL
PROVEEDOR          : MINSAIT
FECHA              : 10/12/2025
DESCRIPCION        : Paquete que contiene procesos para simular la proyección en una fecha específica
"""

# Librerías Básicas
import logging

import datetime
from dateutil.relativedelta import relativedelta

# Librerías para Datos
import pandas_gbq

class GestorSimulacion:
    """Clase para gestionar una simulación de proyección mensual en una fecha dada."""

    def __init__(self, self_planificador):
        """
        Inicializa la clase.
        """
        logging.info("Inicializando la clase de Gestor de Simulación de la Proyección...")

        self.p = self_planificador

    ###################################################################################
    # FUNCIÓN PARA AJUSTAR LA QUERY A LA FECHA QUE SE DESEA
    ###################################################################################
    def ajustar_queries(self, fecha):
        """
        Función para ajustar las queries a la fecha deseada.
        """
        simulated_date_obj = fecha
        simulated_date_str = simulated_date_obj.strftime('%Y-%m-%d')
        simulated_first_day_of_prev_month_obj = simulated_date_obj.replace(day=1) - relativedelta(months=1)
        simulated_first_day_of_prev_month_str = simulated_first_day_of_prev_month_obj.strftime('%Y-%m-%d') # Ej: '2024-02-01'

        today = fecha
        
        hoy = today
        mes_anterior = hoy - relativedelta(months=1)
        mes_anterior_primer_dia = mes_anterior.replace(day = 1).strftime('%Y-%m-%d')

        self.p.MODEL_VERSION = "v" + today.strftime("%Y%m")

        self.p.fecha_base = datetime.strptime(simulated_date_str, '%Y-%m-%d')

        # QUERYS
        self.p.sql_planificacion_demanda_validation = f"""
            SELECT 
                {self.p.COLUMNAS_CONSUMO_DEMANDA_QUERY}
            FROM `{self.p.PATH_CONSUMO_DEMANDA}`
            WHERE CLASIFICACION = '{self.p.clase_producto}'
            LIMIT 1
        """

        self.p.sql_planificacion_demanda = f"""
            WITH 
            ULTIMO_PERIODO AS (
                SELECT
                    DATE('{simulated_first_day_of_prev_month_str}') AS ultimo_mes_inicio
            )
            
            SELECT 
                {self.p.COLUMNAS_CONSUMO_DEMANDA_QUERY}
            FROM `{self.p.PATH_CONSUMO_DEMANDA}` AS t, ULTIMO_PERIODO AS p
            WHERE 
                DATE_TRUNC({self.p.COLUMNA_FECHA_CONSUMO_DEMANDA}, MONTH) >= DATE_SUB(p.ultimo_mes_inicio, INTERVAL {str(self.p.ventana_historico)} MONTH)
                AND DATE_TRUNC({self.p.COLUMNA_FECHA_CONSUMO_DEMANDA}, MONTH) <= p.ultimo_mes_inicio -- El último mes completo es ultimo_mes_inicio
                AND CLASIFICACION = '{self.p.clase_producto}'
        """

        self.p.sql_proyeccion_demanda_validation = f"""
            SELECT 1
            FROM `{self.p.PATH_PROYECCION_DEMANDA}`
            LIMIT 1
        """

        self.p.sql_proyeccion_demanda_mes_validation = f"""
            SELECT 1
            FROM `{self.p.PATH_PROYECCION_DEMANDA}`
            WHERE {self.p.COLUMNA_PERIODO_OUTPUT} = CAST(FORMAT_DATE('%Y%m', DATE('{simulated_date_str}')) AS INT64)
                AND CLASIFICACION = '{self.p.clase_producto}'
            LIMIT 1
        """

        self.p.sql_proyeccion_demanda = f"""
            SELECT *
            FROM `{self.p.PATH_PROYECCION_DEMANDA}`
            WHERE {self.p.COLUMNA_PERIODO_OUTPUT} = CAST(FORMAT_DATE('%Y%m', DATE('{simulated_date_str}')) AS INT64)
                AND CLASIFICACION = '{self.p.clase_producto}'
        """

        self.p.sql_monitoreo_validation = f"""
            SELECT 1
            FROM `{self.p.PATH_MONITOREO}`
            LIMIT 1
        """

        self.p.sql_monitoreo_mes_validation = f"""
            SELECT 1
            FROM `{self.p.PATH_MONITOREO}`
            WHERE {self.p.COLUMNA_PERIODO_MONITOREO} = CAST(FORMAT_DATE('%Y%m', DATE('{simulated_date_str}')) AS INT64)
                AND CLASIFICACION = '{self.p.clase_producto}'
            LIMIT 1
        """

        self.p.sql_monitoreo = f"""
            SELECT *
            FROM `{self.p.PATH_MONITOREO}`
            WHERE {self.p.COLUMNA_PERIODO_MONITOREO} = CAST(FORMAT_DATE('%Y%m', DATE('{simulated_date_str}')) AS INT64)
                AND CLASIFICACION = '{self.p.clase_producto}'
        """

        self.p.sql_proyeccion_monitoreable = f"""
            SELECT t.*
            FROM `{self.p.PATH_PROYECCION_DEMANDA}` AS t
            WHERE t.CLASIFICACION = '{self.p.clase_producto}'
                AND t.{self.p.COLUMNA_PERIODO_OUTPUT} = (
                    SELECT t1.{self.p.COLUMNA_PERIODO_OUTPUT}
                    FROM `{self.p.PATH_PROYECCION_DEMANDA}` AS t1
                    WHERE 
                        DATE_TRUNC(PARSE_DATE('%Y-%m-%d', t1.{self.p.COLUMNA_FECHA_CONSUMO_DEMANDA}), MONTH) >= DATE_SUB(DATE('{simulated_first_day_of_prev_month_str}'), INTERVAL 24 MONTH)
                    GROUP BY t1.{self.p.COLUMNA_PERIODO_OUTPUT}
                    HAVING
                        MAX(DATE_TRUNC(PARSE_DATE('%Y-%m-%d', t1.{self.p.COLUMNA_FECHA_CONSUMO_DEMANDA}), MONTH)) = DATE('{simulated_first_day_of_prev_month_str}')
                    LIMIT 1
                )
        """

        self.p.sql_input_monitoreable = f"""
            SELECT {self.p.COLUMNAS_CONSUMO_DEMANDA_QUERY}
            FROM `{self.p.PATH_CONSUMO_DEMANDA}`
            WHERE 
                DATE_TRUNC(FECHA, MONTH) BETWEEN 
                    DATE_SUB(DATE_TRUNC(DATE('{simulated_date_str}'), MONTH), INTERVAL {self.p.num_meses_proyeccion} MONTH)
                    AND DATE_SUB(DATE_TRUNC(DATE('{simulated_date_str}'), MONTH), INTERVAL 1 MONTH)
                AND CLASIFICACION = '{self.p.clase_producto}'
        """

    ####################################################################
    # LÓGICA PRINCIPAL POR FECHA
    ####################################################################
    def ejecutar_simulacion(self, fecha_simulacion):
        identificadores = ['CLASIFICACION', 'CODSOCIEDAD', 'CODCENTRO', 'CODMATERIAL', 'CODUNIDADMEDIDABASE']
        identificadores_almacen = ['CLASIFICACION', 'CODSOCIEDAD', 'CODCENTRO']
        identificadores_entrenamiento = ['CODSOCIEDAD', 'CODCENTRO', 'CODMATERIAL', 'CODUNIDADMEDIDABASE']
        logging.info(f"Iniciando Trabajo de Proyección de Demanda - {self.p.clase_producto_log} para la fecha {fecha_simulacion.strftime('%Y-%m-%d')}")
        
        ####################################################################
        # 1 - AJUSTANDO QUERIES A FECHA DONDE SE DESEA HALLAR LA PLANIFICACIÓN
        ####################################################################
        logging.info(f"Ajustando Fechas de Queries para la Proyección de Demanda - {self.p.clase_producto_log}")
        self.ajustar_queries(fecha_simulacion)

        ####################################################################
        # 1 - VERIFICANDO EXISTENCIA DEL INPUT DE DEMANDA
        ####################################################################
        logging.info(f"Verificando existencia de input con los datos de Demanda Histórica - {self.p.clase_producto_log}")
        tabla_planificacion_demanda_existe = self.p.DataManager.verificar_tabla(self.p.sql_planificacion_demanda_validation)        

        if tabla_planificacion_demanda_existe:
            ####################################################################
            # 1.A1 - VERIFICANDO EXISTENCIA DE TABLA DE SKU ANALIZADOS DURANTE EL DESARROLLO DEL PROYECTO
            ####################################################################
            logging.info(f"SE ENCONTRÓ INPUT con los datos de Demanda Histórica - {self.p.clase_producto_log}")

            logging.info(f"Verificando existencia de Sheet de Excel con los datos Analizados en el Desarrollo respecto a Demanda Histórica - {self.p.clase_producto_log}")
            tabla_sku_analizados_existe = self.p.DataManager.verificar_archivo_gcs(self.p.BUCKET_SKU_ANALIZADOS, self.p.FILE_PATH_SKU_ANALIZADOS)

            if tabla_sku_analizados_existe:
                logging.info(f"SE ENCONTRÓ Sheet de Excel con los datos Analizados en el Desarrollo respecto a Demanda Histórica - {self.clase_producto_log}")
            
                ####################################################################
                # 1.A2.A1 - OBTENIENDO LOS SKU ANALIZADOS DURANTE EL DESARROLLO DEL PROYECTO
                ####################################################################
                logging.info(f"Obteniendo en Dataframe el Sheet de Excel con los datos Analizados en el Desarrollo respecto a Demanda Histórica - {self.clase_producto_log}")
                df_sku = self.p.DataManager.obtener_sku_analizados(self.p.BUCKET_SKU_ANALIZADOS, self.p.FILE_PATH_SKU_ANALIZADOS, self.p.SHEET_SKU_ANALIZADOS)

                ####################################################################
                # 1.A2.A2 - OBTENIENDO MUESTRAS CON LOS ESQUEMAS DE LOS OUTPUTS
                ####################################################################
                logging.info(f"Obteniendo dataframes de prueba para obtener esquemas de las tablas OUTPUT de Proyección y Monitoreo")
                df_muestra_input = pandas_gbq.read_gbq(self.p.sql_planificacion_demanda_validation, project_id=self.p.PROJECT_EXE)
                df_muestra_output = self.p.PreManager.transformar_a_output(df_muestra_input)
                df_muestra_monitoreo = self.p.PreManager.transformar_a_monitoreo(df_muestra_input)

                ####################################################################
                # 1.A2.A3 - VERIFICANDO EXISTENCIA DE LA TABLA OUTPUT DE PROYECCIÓN
                ####################################################################
                logging.info(f"Verificando existencia de tabla output de Proyección de la Demanda - {self.p.clase_producto_log}")
                tabla_proyeccion_demanda_existe = self.p.verificar_tabla(self.p.sql_proyeccion_demanda_validation)

                if tabla_proyeccion_demanda_existe:
                    logging.info(f"SE ENCONTRÓ TABLA OUTPUT de Proyección de Demanda Histórica - {self.p.clase_producto_log}")

                    ####################################################################
                    # 1.A2.A4.A1 - VERIFICANDO EXISTENCIA DE LA PROYECCIÓN DEL MES EN LA TABLA OUTPUT
                    ####################################################################
                    logging.info(f"Verificando existencia de la Proyección del Mes en Output de Demanda - {self.p.clase_producto_log}")
                    proyeccion_mes_existe = self.p.DataManager.verificar_resultados_tabla(self.p.sql_proyeccion_demanda_mes_validation)

                else:
                    ####################################################################
                    # 1.A2.A4.B1 - INFORMANDO QUE NO EXISTE TABLA OUTPUT DE PROYECCIÓN
                    ####################################################################
                    logging.info(f"NO EXISTE AÚN TABLA OUTPUT de Proyección de Demanda - {self.p.clase_producto_log}")
                    
                    ####################################################################
                    # 1.A2.A4.B2 - CREANDO TABLA PARTICIONADA PARA PROYECCIÓN
                    ####################################################################
                    logging.info(f"CREANDO Tabla Particionada de Proyección de Demanda - {self.p.clase_producto_log}")
                    self.p.DataManager.crear_tabla_particionada(
                        nueva_tabla = self.p.PATH_PROYECCION_DEMANDA,
                        df_schema = df_muestra_output,
                        columna_particionada = self.p.COLUMNA_PERIODO_OUTPUT
                    )
                    proyeccion_mes_existe = False
                    logging.info(f"Tabla Particionada CREADA de Proyección de Demanda - {self.p.clase_producto_log}")
                
                if proyeccion_mes_existe:
                    logging.info(f"YA EXISTE Proyección del Mes de Demanda - {self.p.clase_producto_log}")
                    logging.info(f"No se ejecutará Proyección del Mes de Demanda - {self.p.clase_producto_log}, porque ya existe")

                    ####################################################################
                    # 1.A2.A5.A1 - OBTENIENDO PROYECCIÓN EXISTENTE
                    ####################################################################
                    logging.info(f"Obteniendo Proyección del Mes de Demanda - {self.p.clase_producto_log}, directamente de la tabla")
                    df_final = pandas_gbq.read_gbq(self.p.sql_proyeccion_demanda, project_id=self.p.PROJECT_EXE)

                else:
                    ####################################################################
                    # 1.A2.A5.B1 - INFORMANDO QUE NO EXISTE LA PROYECCIÓN DEL MES RESPECTO A LA DEMANDA
                    ####################################################################
                    logging.info(f"NO EXISTE AÚN Proyección del Mes de Demanda - {self.p.clase_producto_log}")
                    
                    ####################################################################
                    # 1.A2.A5.B2 - OBTENER LOS DATOS HISTÓRICOS DEL INPUT PARA PROYECTAR
                    ####################################################################
                    logging.info(f"Obteniendo Data Input de Demanda - {self.p.clase_producto_log}, para proyección")
                    
                    df_36_meses_demanda = pandas_gbq.read_gbq(self.p.sql_planificacion_demanda, project_id=self.p.PROJECT_EXE)
                    df_procesado = self.p.PreManager.procesar_datos_para_planificacion(
                        df_36_meses_demanda,
                        date_col = self.p.COLUMNA_FECHA_CONSUMO_DEMANDA,
                        val_cols = [self.p.COLUMNA_CONSUMO_DEMANDA],
                        group_cols = identificadores,
                        filters = {
                            "CLASIFICACION": ["CEMENTO"],
                            "CODSOCIEDAD": ["6012", "6052"]
                        },
                        group_by_month = True,
                        complete_months = True,
                        num_meses = self.p.ventana_historico
                    )
                    
                    # OBTENER SKUS CONOCIDOS A PROYECTAR, DESCONOCIDOS A PROYECTAR, CONOCIDOS QUE NO SE PROYECTARÁN
                    df_ca, df_da, df_ci = self.p.PreManager.obtener_skus_conocidos_desconocidos(df_procesado, df_sku, identificadores)
                    
                    # OBTENER SKUS QUE SON PROYECTABLES CON MODELO, NO PROYECTABLES CON MODELO
                    df_sp, df_np = self.p.PreManager.obtener_skus_proyectables(
                        df_procesado,
                        self.p.ventana_segmentacion,
                        mes_col = self.p.mes_col,
                        val_col = self.p.COLUMNA_CONSUMO_DEMANDA,
                        group_cols_mes = identificadores + [self.p.mes_col],
                        group_cols_product = identificadores,
                        group_cols_warehouse = identificadores_almacen,
                        prefix_str = "",
                        abc_umb = (self.p.abc_umb_inf, self.p.abc_umb_sup),
                        xyz_umb = (self.p.xyz_umb_inf, self.p.xyz_umb_sup),
                        fsn_umb = (self.p.fsn_umb_inf, self.p.fsn_umb_sup),
                        condiciones_proyectables = {
                            "CODUNIDADMEDIDABASE": ["BLS"]
                        },
                        segmentos_proyectables = {
                            "ABC": ["A", "B", "C"],
                            "XYZ": ["X", "Y", "Z"],
                            "FSN": ["F", "S"]
                        }
                    )
                    
                    ####################################################################
                    # 1.A2.A5.B3 - VERIFICAR SI EXISTE EL MODELO ACTUALIZADO DE PROYECCIÓN
                    ####################################################################
                    logging.info(f"Verificando si ya existe el Modelo Actualizado del Mes de Proyección de Demanda - {self.p.clase_producto_log}")
                    modelo_mes_existe, versiones = self.p.ModelManager.verificar_modelo(self.p.MODEL_NAME, self.p.MODEL_VERSION_LABEL, self.p.MODEL_VERSION, self.p.MODEL_PROJECT, self.p.MODEL_REGION)

                    if modelo_mes_existe:
                        logging.info(f"YA EXISTE Modelo Actualizado del Mes para Proyección de Demanda - {self.p.clase_producto_log}")
                        
                        ####################################################################
                        # 1.A2.A5.B4.A1 - SI EXISTE, OBTENER EL MODELO ACTUALIZADO
                        ####################################################################
                        logging.info(f"Obteniendo Modelo Actualizado del Mes para Proyección de Demanda - {self.p.clase_producto_log}")
                        modelo_actual = self.p.ModelManager.obtener_modelo(self.p.MODEL_NAME, self.p.MODEL_VERSION_LABEL, self.p.MODEL_VERSION, self.p.MODEL_PROJECT, self.p.MODEL_REGION)
                    else:
                        logging.info(f"NO EXISTE Modelo Actualizado del Mes para Proyección de Demanda - {self.p.clase_producto_log}")

                        ####################################################################
                        # 1.A2.A5.B4.B1 - SI NO EXISTE, ACTUALIZAR MODELO CON LA DATA ACTUAL
                        ####################################################################
                        if not df_sp.empty:
                            logging.info(f"Actualizando Modelo con Histórico del Mes para Proyección de Demanda - {self.p.clase_producto_log}")
                            modelo_actual = self.p.ModelManager.actualizar_modelo(
                                df_sp,
                                id_skus = identificadores_entrenamiento,
                                mes_col = self.p.mes_col, 
                                val_col = self.p.COLUMNA_CONSUMO_DEMANDA,
                                prefix_str = "",
                                versiones = versiones, 
                                model_name = self.p.MODEL_NAME, 
                                version_label = self.p.MODEL_VERSION_LABEL, 
                                version_model = self.p.MODEL_VERSION, 
                                project_id = self.p.MODEL_PROJECT, 
                                region = self.p.MODEL_REGION, 
                                uri_gcs = self.p.MODEL_URI_GCS_GENERAL, 
                                prefix_gcs = self.p.MODEL_PREFIX_GCS
                            )

                    ####################################################################
                    # 1.A2.A5.B5 - EJECUTAR PROYECCIÓN DE LA DEMANDA CON EL MODELO ACTUALIZADO
                    ####################################################################
                    logging.info(f"Ejecutando Proyección del Mes de la Demanda - {self.p.clase_producto_log}, por los 18 meses siguientes")
                    df_proyeccion_demanda_18_meses = self.p.ForecastManager.proyectar_demanda(
                        df_proyectable = df_sp, 
                        df_no_proyectable = df_np, 
                        df_conocidos_activos = df_ca,
                        df_conocidos_inactivos = df_ci,
                        df_desconocidos = df_da,
                        id_skus = identificadores_entrenamiento,
                        mes_col = self.p.mes_col,
                        val_col = self.p.COLUMNA_CONSUMO_DEMANDA,
                        modelo = modelo_actual
                    )

                    logging.info(f"Agregando Información de Periodo al Dataframe de Proyección - {self.p.clase_producto_log}")
                    df_final = self.p.PreManager.agregar_columnas_periodo(df_proyeccion_demanda_18_meses)

                    ####################################################################
                    # 1.A2.A5.B6 - CARGAR PROYECCIÓN DE LA DEMANDA EN BIGQUERY
                    ####################################################################
                    logging.info(f"Cargando a Bigquery Proyección de Demanda - {self.p.clase_producto_log}")
                    self.p.DataManager.cargar_datos_bigquery(df_final, self.p.PATH_PROYECCION_DEMANDA, self.p.PROJECT_EXE, if_exists = 'append')

                ####################################################################
                # 1.A2.A6 - VERIFICAR EXISTENCIA DE TABLA DE MONITOREO
                ####################################################################
                logging.info(f"Verificando existencia de tabla de monitoreo de Modelo de Proyección de la Demanda - {self.p.clase_producto_log}")
                tabla_monitoreo_existe = self.p.verificar_tabla(self.p.sql_monitoreo_validation)

                if tabla_monitoreo_existe:
                    logging.info(f"SE ENCONTRÓ TABLA DE MONITOREO del Modelo de Proyección de Demanda - {self.p.clase_producto_log}")

                    ####################################################################
                    # 1.A2.A7.A1 - VERIFICAR EXISTENCIA DE DATOS DEL MES DE MONITOREO
                    ####################################################################
                    logging.info(f"Verificando existencia de Datos de Monitoreo del Mes respecto a Demanda - {self.p.clase_producto_log}")
                    monitoreo_mes_existe = self.p.verificar_resultados_tabla(self.p.sql_monitoreo_mes_validation)

                else:
                    ####################################################################
                    # 1.A2.A7.B1 - INFORMANDO QUE NO EXISTE TABLA DE MONITOREO
                    ####################################################################
                    logging.info(f"NO EXISTE AÚN TABLA DE MONITOREO del Modelo de Proyección de Demanda - {self.p.clase_producto_log}")
                    
                    ####################################################################
                    # 1.A2.A7.B2 - CREANDO TABLA PARTICIONADA DE MONITOREO
                    ####################################################################
                    logging.info(f"CREANDO Tabla Particionada de Monitoreo del Modelo de Proyección de Demanda - {self.p.clase_producto_log}")
                    self.p.DataManager.crear_tabla_particionada(
                        nueva_tabla = self.p.PATH_MONITOREO,
                        df_schema = df_muestra_monitoreo,
                        columna_particionada = self.p.COLUMNA_PERIODO_MONITOREO
                    )
                    monitoreo_mes_existe = False
                    logging.info(f"Tabla Particionada CREADA de Monitoreo del Modelo de Proyección de Demanda - {self.p.clase_producto_log}")

                if monitoreo_mes_existe:
                    ####################################################################
                    # 1.A2.A8.A1 - INFORMAR QUE YA EXISTE DATOS DE MONITOREO DEL MES Y NO SE CARGARÁ NADA
                    ####################################################################
                    logging.info(f"YA EXISTE Datos del Mes de Monitoreo del Modelo de Proyección de Demanda - {self.p.clase_producto_log}")
                    logging.info(f"No se cargarán Datos del Mes de Monitoreo del Modelo de Proyección de Demanda - {self.p.clase_producto_log}, porque ya existen")

                else:
                    ####################################################################
                    # 1.A2.A8.B1 - INFORMAR QUE NO EXISTEN DATOS DE MONITOREO DEL MES
                    ####################################################################
                    logging.info(f"NO EXISTEN AÚN Datos del Mes de Monitoreo del Modelo de Proyección de Demanda - {self.p.clase_producto_log}")
                    
                    ####################################################################
                    # 1.A2.A8.B2 - OBTENER DATOS DE MONITOREO DEL MES
                    ####################################################################
                    logging.info(f"Obteniendo Datos del Mes de Monitoreo del Modelo de Proyección de Demanda - {self.p.clase_producto_log}")
                    df_input_monitoreable = pandas_gbq.read_gbq(self.p.sql_input_monitoreable, project_id=self.p.PROJECT_EXE)
                    df_proyeccion_monitoreable = pandas_gbq.read_gbq(self.p.sql_proyeccion_monitoreable, project_id=self.p.PROJECT_EXE)
                    
                    df_input_procesado = self.p.PreManager.procesar_datos_para_planificacion(
                        df_input_monitoreable,
                        date_col = self.p.COLUMNA_FECHA_CONSUMO_DEMANDA,
                        val_cols = [self.p.COLUMNA_CONSUMO_DEMANDA],
                        group_cols = identificadores,
                        filters = {
                            "CLASIFICACION": ["CEMENTO"],
                            "CODSOCIEDAD": ["6012", "6052"]
                        },
                        group_by_month = True,
                        complete_months = True,
                        num_meses = self.p.ventana_historico
                    )
                    df_monitoreo = self.p.monitorear_modelo(df_proyeccion_monitoreable, df_input_procesado, identificadores, self.p.mes_col)

                    ####################################################################
                    # 1.A2.A8.B3 - CARGAR DATOS DE MONITOREO DEL MES EN BIGQUERY
                    ####################################################################
                    if not df_monitoreo.empty:
                        logging.info(f"Agregando Información de Periodo al Dataframe de Monitoreo - {self.p.clase_producto_log}")
                        df_monitoreo_final = self.p.PreManager.agregar_columnas_periodo(df_monitoreo)
                        
                        logging.info(f"Cargando a Bigquery Monitoreo del Modelo de Proyección de Demanda - {self.p.clase_producto_log}")
                        self.p.DataManager.cargar_datos_bigquery(df_monitoreo_final, self.p.PATH_MONITOREO, self.p.PROJECT_EXE, if_exists = 'append')
                    else:
                        logging.info(f"No se pudo calcular data de monitoreo del Modelo de Proyección de Demanda - {self.p.clase_producto_log}")
            else:
                ####################################################################
                # 1.A2.B1 - INFORMANDO QUE NO SE ENCONTRÓ TABLA DE SKU ANALIZADOS
                ####################################################################
                logging.info(f"NO SE ENCONTRÓ Sheet de Excel con los datos Analizados en el Desarrollo respecto a Demanda Histórica - {self.p.clase_producto_log}")
                logging.info(f"NO SE PROCEDERÁ con la Proyección de Demanda - {self.p.clase_producto_log}, por falta de datos analizados")
        else:
            ####################################################################
            # 1.B1 - INFORMANDO QUE NO SE ENCONTRÓ INPUT DE DEMANDA
            ####################################################################
            logging.info(f"NO SE ENCONTRÓ INPUT con los datos de Demanda Histórica - {self.p.clase_producto_log}")
            logging.info(f"NO SE PROCEDERÁ con la Proyección de Demanda - {self.p.clase_producto_log}, por falta de tabla input")

        ####################################################################
        # 2 - TERMINANDO TRABAJO / JOB DE PLANIFICACIÓN
        ####################################################################
        logging.info(f"Trabajo de Planificación de la Demanda - {self.p.clase_producto_log}")