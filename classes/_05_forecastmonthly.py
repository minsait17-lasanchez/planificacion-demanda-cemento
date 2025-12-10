"""_summary_.

PROYECTO           : [PRJ-25-002] CDS - PLANIFICACIÓN DE LA DEMANDA
NOMBRE             : 05_forecastmonthly
ARCHIVOS  DESTINO  : ---
ARCHIVOS  FUENTES  : ---
OBJETIVO           : Definir procesos para hacer la proyección mensual
OBSERVACION        : -
SCHEDULER          : CLOUD RUN
VERSION            : 1.0
DESARROLLADOR      : SÁNCHEZ AGUILAR LUIS ÁNGEL
PROVEEDOR          : MINSAIT
FECHA              : 10/12/2025
DESCRIPCION        : Paquete que contiene procesos para hacer la proyección mensual
"""

# Librerías Básicas
import logging

# Librerías para Datos
import pandas as pd
import numpy as np

# Librerías para Auto Selección de Modelos
from autogluon.timeseries import TimeSeriesDataFrame

class GestorProyeccion:
    """Clase para gestionar la proyección mensual."""

    def __init__(self, mes_col, months, sales, conf_ag, ml_type, sm_type, m0_type, gestor_modelo):
        """
        Inicializa la clase.
        """
        logging.info("Inicializando la clase de Gestor de la Proyección...")

        self.COLUMNA_FECHA_CONSUMO_DEMANDA = mes_col
        self.num_meses_proyeccion = months
        self.ventana_ventas = sales
        self.CONFIGURACION_AUTOGLUON_PREDICTOR = conf_ag
        self.TIPO_MODELO_ML = ml_type
        self.TIPO_MODELO_SIMPLE = sm_type
        self.TIPO_MODELO_0 = m0_type
        self.ModelManager = gestor_modelo

    ###################################################################################
    # FUNCIÓN PARA CONSTRUIR DATAFRAME CON DATOS CONOCIDOS EN EL FUTURO
    ###################################################################################
    def construir_dataframe_futuro(self, df_ag, modelo, known_covariates = [], need_known_time_features = False):
        """
        Función para construir el dataframe con datos conocidos en el futuro.
        """
        df = df_ag.copy()
        df = df.reset_index()
        fecha_min = df['timestamp'].min()

        # OBTENER LOS IDENTIFICADORES DE LOS SKUS PARA DATAFRAME DE VARIABLES CONOCIDAS A FUTURO
        future_df = modelo.make_future_data_frame(
            data = df_ag
        ).reset_index()

        if need_known_time_features:
            # VARIABLES DE CONOCIMIENTO FUTURO
            future_df["mes"] = future_df["timestamp"].dt.month
            future_df["mes_sin"] = np.sin(2*np.pi*future_df["mes"]/12)
            future_df["mes_cos"] = np.cos(2*np.pi*future_df["mes"]/12)
            future_df["trimestre"] = future_df["timestamp"].dt.quarter
            future_df["trim_sin"] = np.sin(2*np.pi*future_df["trimestre"]/4)
            future_df["trim_cos"] = np.cos(2*np.pi*future_df["trimestre"]/4)
            future_df["semestre"] = np.where(future_df["mes"] <= 6, 1, 2)
            future_df["sem_sin"] = np.sin(2*np.pi*future_df["semestre"]/2)
            future_df["sem_cos"] = np.cos(2*np.pi*future_df["semestre"]/2)
            future_df["contador_lineal"] = ((future_df["timestamp"].dt.year - fecha_min.year)*12 + (future_df["timestamp"].dt.month - fecha_min.month))
            future_df["contador_cuadratico"] = future_df["contador_lineal"]**2
            future_df["contador_log"] = np.log1p(future_df["contador_lineal"])

        # CREAR DATAFRAME DE LAS VARIABLES CONOCIDAS EN EL FUTURO
        df_future = TimeSeriesDataFrame.from_data_frame(
            df = future_df[["item_id","timestamp"] + known_covariates],
            id_column = "item_id",
            timestamp_column = "timestamp",
        )

        return df_future

    ###################################################################################
    # FUNCIÓN PARA CONSTRUIR EL DATAFRAME DE PROYECCIÓN
    ###################################################################################
    def construir_dataframe_proyeccion(self, predicciones, df_train, df_proyectables, df_conocidos_activos, df_conocidos_inactivos, df_desconocidos, id_skus, mes_col, val_col, tipo_modelo, tiene_ventas = None, es_proyectable = None, es_conocido = None, es_activo = None):
        """
        Función para proyectar la demanda los próximos meses, aplicando modelo y sin aplicarlo, según cada sku.
        """
        if hasattr(predicciones, 'to_pandas'):
            predicciones = predicciones.to_pandas()

        df_o = df_train.copy()
        df_p = df_proyectables.copy()
        df_con_act = df_conocidos_activos.copy()
        df_con_ina = df_conocidos_inactivos.copy()
        df_des = df_desconocidos.copy()

        predicciones["CLASIFICACION"] = self.clase_producto
        split_data = predicciones['item_id'].str.split('_', expand = True)
        split_data.columns = id_skus
        predicciones = pd.concat([predicciones, split_data], axis = 1)
        predicciones[self.COLUMNA_FECHA_CONSUMO_DEMANDA] = pd.to_datetime(predicciones["timestamp"])
        nuevas_columnas_riesgos = ["CTDCONSUMORIESGO05", "CTDCONSUMORIESGO25", "CTDCONSUMORIESGO50", "CTDCONSUMORIESGO75", "CTDCONSUMORIESGO95"]
        nuevas_columnas_riesgos_dict = {
            "CTDCONSUMORIESGO05": '0.05',
            "CTDCONSUMORIESGO25": '0.25',
            "CTDCONSUMORIESGO50": '0.5',
            "CTDCONSUMORIESGO75": '0.75',
            "CTDCONSUMORIESGO95": '0.95'
        }
        for nueva_columna_riesgo in nuevas_columnas_riesgos:
            predicciones[nueva_columna_riesgo] = np.round(predicciones[nuevas_columnas_riesgos_dict[nueva_columna_riesgo]].apply(lambda x: np.maximum(0, x))).astype(np.int64)

        df_p_sp = pd.DataFrame(predicciones[["CLASIFICACION"] + id_skus + [self.COLUMNA_FECHA_CONSUMO_DEMANDA] + nuevas_columnas_riesgos].copy())
        df_p_sp["TIPOMODELOPROYECCION"] = tipo_modelo
        
        df_p_sp['item_id'] = df_p_sp[id_skus].astype(str).agg('_'.join, axis=1)
        if tiene_ventas is not None:
            df_p_sp["SKUCONVENTAS"] = tiene_ventas
        else:
            df_o = df_o.reset_index()
            if 'timestamp' not in df_o.columns:
                df_o['timestamp'] = df_o[mes_col]
                df_o['target'] = df_o[val_col]
            fecha_actual = df_o['timestamp'].max()
            fecha_anterior = fecha_actual - pd.DateOffset(months=(self.ventana_ventas-1))

            tiene_ventas_recientes = df_o.groupby('item_id').apply(lambda x:
                ((x['timestamp'] > fecha_anterior) & (x['target'] > 0)).any()
            )
            df_p_sp['SKUCONVENTAS'] = df_p_sp['item_id'].map(tiene_ventas_recientes)
            df_p_sp['SKUCONVENTAS'] = df_p_sp['SKUCONVENTAS'].fillna(False)

        if es_proyectable is not None:
            df_p_sp["SKUPROYECTABLE"] = es_proyectable
        else:
            skus_proyectables = df_p['item_id'].unique()
            df_p_sp['SKUPROYECTABLE'] = df_p_sp['item_id'].isin(skus_proyectables)

        if es_conocido is not None:
            df_p_sp["SKUCONOCIDO"] = es_conocido
        else:
            df_con_act['item_id'] = df_con_act[id_skus].astype(str).agg('_'.join, axis=1)
            df_con_ina['item_id'] = df_con_ina[id_skus].astype(str).agg('_'.join, axis=1)
            skus_conocidos_activos = df_con_act['item_id'].unique()
            skus_conocidos_inactivos = df_con_ina['item_id'].unique()
            skus_conocidos = set(skus_conocidos_activos) | set(skus_conocidos_inactivos)
            df_p_sp['SKUCONOCIDO'] = df_p_sp['item_id'].isin(skus_conocidos)
        
        if es_activo is not None:
            df_p_sp["SKUACTIVO"] = es_activo
        else:
            df_con_act['item_id'] = df_con_act[id_skus].astype(str).agg('_'.join, axis=1)
            df_des['item_id'] = df_des[id_skus].astype(str).agg('_'.join, axis=1)
            skus_conocidos_activos = df_con_act['item_id'].unique()
            skus_desconocidos = df_des['item_id'].unique()
            skus_activos = set(skus_conocidos_activos) | set(skus_desconocidos)
            df_p_sp['SKUACTIVO'] = df_p_sp['item_id'].isin(skus_activos)

        df_p_sp = df_p_sp.drop(columns=['item_id'])
        return df_p_sp

    ###################################################################################
    # FUNCIÓN PARA OBTENER LOS SKUS CON VENTAS
    ###################################################################################
    def obtener_skus_con_ventas(self, df_pre, id_skus, mes_col, val_col):
        """
        Función para obtener dataframes con skus con ventas y sin ventas
        """
        df = df_pre.copy()
        fecha_actual = df[mes_col].max()
        fecha_anterior = fecha_actual - pd.DateOffset(months=(self.ventana_ventas-1))

        df = df.reset_index()
        if 'timestamp' not in df.columns:
            df['timestamp'] = df[mes_col]
            df['target'] = df[val_col]
        df['item_id'] = df[id_skus].astype(str).agg('_'.join, axis=1)
        tiene_ventas_recientes = df.groupby('item_id').apply(lambda x:
            ((x['timestamp'] > fecha_anterior) & (x['target'] > 0)).any()
        )
        df['SKUCONVENTASTEMPORAL'] = df['item_id'].map(tiene_ventas_recientes)
        df['SKUCONVENTASTEMPORAL'] = df['SKUCONVENTASTEMPORAL'].fillna(False)

        df_con_ventas = df[df["SKUCONVENTASTEMPORAL"] == True]
        df_sin_ventas = df[df["SKUCONVENTASTEMPORAL"] == False]
        df_con_ventas = df_con_ventas.drop(columns=['item_id', 'SKUCONVENTASTEMPORAL'])
        df_sin_ventas = df_sin_ventas.drop(columns=['item_id', 'SKUCONVENTASTEMPORAL'])

        return df_con_ventas, df_sin_ventas

    ###################################################################################
    # FUNCIÓN PARA HACER PROYECCIONES SIMPLES DE DEMANDA
    ###################################################################################
    def proyectar_demanda_simple(self, df_simple, id_skus, mes_col, val_col, algoritmo = "Media_Movil"):
        """
        Función para hacer proyecciones simples de demanda
        """
        df = df_simple.copy()
        df['item_id'] = df[id_skus].astype(str).agg('_'.join, axis=1)

        fecha_final = df[mes_col].max()
        unique_item_ids = df['item_id'].unique()

        # GENERAR LAS 18 FECHAS FUTURAS
        future_dates = pd.date_range(
            start = fecha_final + pd.DateOffset(months = 1),
            periods = self.num_meses_proyeccion,
            freq = 'MS'
        )

        if algoritmo == "Media_Movil":
            projection_movil_recursive_data = []

            for item_id_val in unique_item_ids:
                # DATOS HISTÓRICOS PARA EL PRESENTE SKU
                sku_historical_data = df[df['item_id'] == item_id_val].set_index(mes_col).sort_index()
                current_rolling_values = list(sku_historical_data[val_col].tail(self.ventana_ventas).values)

                for pred_date in future_dates:
                    # PROMEDIAR LA VENTANA
                    if current_rolling_values:
                        projected_value = np.mean(current_rolling_values).round().astype(int)
                    else:
                        projected_value = 0

                    # ASEGURAR QUE LAS PROYECCIONES NO SEAN NEGATIVAS
                    projected_value = max(0, projected_value)

                    projection_movil_recursive_data.append({
                        'item_id': item_id_val,
                        mes_col: pred_date,
                        'mean': projected_value
                    })

                    # ACTUALIZAR LA VENTANA CON EL NUEVO VALOR PROYECTADO
                    current_rolling_values.pop(0)
                    current_rolling_values.append(projected_value)

            df_proyeccion_movil_recursiva = pd.DataFrame(projection_movil_recursive_data)
            df_proyeccion_movil_recursiva['0.05'] = df_proyeccion_movil_recursiva['mean']
            df_proyeccion_movil_recursiva['0.25'] = df_proyeccion_movil_recursiva['mean']
            df_proyeccion_movil_recursiva['0.5'] = df_proyeccion_movil_recursiva['mean']
            df_proyeccion_movil_recursiva['0.75'] = df_proyeccion_movil_recursiva['mean']
            df_proyeccion_movil_recursiva['0.95'] = df_proyeccion_movil_recursiva['mean']
            df_proyeccion_movil_recursiva['timestamp'] = df_proyeccion_movil_recursiva[mes_col]
            df_proyeccion_simple = df_proyeccion_movil_recursiva.copy()

        elif algoritmo == "Zero":
            projection_cero_data = []

            for item_id_val in unique_item_ids:
                for pred_date in future_dates:
                    projection_cero_data.append({
                        'item_id': item_id_val,
                        mes_col: pred_date,
                        'mean': 0
                    })

            df_proyeccion_cero = pd.DataFrame(projection_cero_data)
            df_proyeccion_cero['0.05'] = df_proyeccion_cero['mean']
            df_proyeccion_cero['0.25'] = df_proyeccion_cero['mean']
            df_proyeccion_cero['0.5'] = df_proyeccion_cero['mean']
            df_proyeccion_cero['0.75'] = df_proyeccion_cero['mean']
            df_proyeccion_cero['0.95'] = df_proyeccion_cero['mean']
            df_proyeccion_cero['timestamp'] = df_proyeccion_cero[mes_col]
            df_proyeccion_simple = df_proyeccion_cero.copy()

        return df_proyeccion_simple
        
    ###################################################################################
    # FUNCIÓN PARA PROYECTAR LOS PRÓXIMOS MESES DE DEMANDA
    ###################################################################################
    def proyectar_demanda(self, df_proyectable, df_no_proyectable, df_conocidos_activos, df_conocidos_inactivos, df_desconocidos, id_skus, mes_col, val_col, modelo):
        """
        Función para proyectar la demanda los próximos meses, aplicando modelo y sin aplicarlo, según cada sku.
        """
        if not df_proyectable.empty:
            df_proyectable_time = self.ModelManager.crear_time_features(
                data = df_proyectable,
                id_cols = id_skus,
                date_col = mes_col,
                value_col = val_col,
                num_month = self.num_meses_proyeccion
            )
            
            # UTILIZAR MODELO PARA HALLAR PROYECCIÓN DEL DATAFRAME DE SKUS PROYECTABLES
            df_ts_ag_p, known_covariates = self.ModelManager.preparar_para_autogluon(
                df_proyectable_time,
                id_skus = id_skus,
                mes_col = mes_col,
                val_col = val_col,
                need_dynamic_time_features = True,
                need_known_time_features = True,
                need_static_segment_features = True
            )

            df_known_cov_future = self.construir_dataframe_futuro(
                df_ts_ag_p, 
                modelo = modelo,
                known_covariates = known_covariates,
                need_known_time_features = True
            )

            try:
                predicciones = modelo.predict(
                    data = df_ts_ag_p,
                    model = self.CONFIGURACION_AUTOGLUON_PREDICTOR,
                    known_covariates = df_known_cov_future
                ).reset_index()
            except:
                try:
                    predicciones = modelo.predict(
                        data = df_ts_ag_p,
                        known_covariates = df_known_cov_future
                    ).reset_index()
                except Exception as E:
                    raise("NO SE PUEDE REALIZAR PREDICCIÓN", E)

            df_p_sp = self.construir_dataframe_proyeccion(
                predicciones, 
                df_ts_ag_p,
                df_proyectable,
                df_conocidos_activos,
                df_conocidos_inactivos, 
                df_desconocidos,
                id_skus = id_skus,
                mes_col = mes_col,
                val_col = val_col,
                tipo_modelo = self.TIPO_MODELO_ML,
                tiene_ventas = None,
                es_proyectable = True,
                es_conocido = None,
                es_activo = True
            )

        if not df_no_proyectable.empty:
            # SEPARAR LOS NO PROYECTABLES EN 'CON VENTAS' Y 'SIN VENTAS' EN EL ÚLTIMO AÑO
            df_cv, df_sv = self.obtener_skus_con_ventas(df_no_proyectable, id_skus, mes_col, val_col)

            # HACER UNA PROYECCIÓN SIMPLE PARA SKUS NO ROTATIVOS CON VENTAS
            pred_cv = self.proyectar_demanda_simple(df_cv, id_skus, mes_col, val_col, algoritmo = "Media_Movil")
            df_p_np_cv = self.construir_dataframe_proyeccion(
                pred_cv, 
                df_cv,
                df_proyectable,
                df_conocidos_activos,
                df_conocidos_inactivos, 
                df_desconocidos,
                id_skus = id_skus,
                mes_col = mes_col,
                val_col = val_col,
                tipo_modelo = self.TIPO_MODELO_SIMPLE,
                tiene_ventas = True,
                es_proyectable = False,
                es_conocido = None,
                es_activo = True
            )

            # HACER UNA PROYECCIÓN A 0 PARA SKUS NO ROTATIVOS SIN VENTAS
            pred_sv = self.proyectar_demanda_simple(df_cv, id_skus, mes_col, val_col, algoritmo = "Zero")
            df_p_np_sv = self.construir_dataframe_proyeccion(
                pred_sv, 
                df_sv,
                df_proyectable,
                df_conocidos_activos,
                df_conocidos_inactivos, 
                df_desconocidos,
                id_skus = id_skus,
                mes_col = mes_col,
                val_col = val_col,
                tipo_modelo = self.TIPO_MODELO_0,
                tiene_ventas = False,
                es_proyectable = False,
                es_conocido = None,
                es_activo = True
            )

        if not df_proyectable.empty and not df_no_proyectable.empty:
            df_proyeccion_concatenado = pd.concat([df_p_sp, df_p_np_cv, df_p_np_sv])
        elif not df_proyectable.empty:
            df_proyeccion_concatenado = df_p_sp
        elif not df_no_proyectable.empty:
            df_proyeccion_concatenado = pd.concat([df_p_np_cv, df_p_np_sv])
        return df_proyeccion_concatenado