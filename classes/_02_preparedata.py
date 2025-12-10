"""_summary_.

PROYECTO           : [PRJ-25-002] CDS - PLANIFICACIÓN DE LA DEMANDA
NOMBRE             : 02_preparedata
ARCHIVOS  DESTINO  : ---
ARCHIVOS  FUENTES  : ---
OBJETIVO           : Definir procesos para preprocesar los datos
TIPO               : PY
OBSERVACION        : -
SCHEDULER          : CLOUD RUN
VERSION            : 1.0
DESARROLLADOR      : SÁNCHEZ AGUILAR LUIS ÁNGEL
PROVEEDOR          : MINSAIT
FECHA              : 10/12/2025
DESCRIPCION        : Paquete que contiene procesos para preprocesar los datos
"""

# Librerías Propias
from classes._03_segmentdata import SegmentadorDatos

# Librerías Básicas
import logging

# Librerías para Datos
import pandas as pd

class GestorPreparacionDatos:
    """Clase para la gestión de los procesos de preparación de los datos para distintos objetivos."""

    def __init__(self, val_col):
        """
        Inicializa la clase.
        """
        logging.info("Inicializando la clase de Gestor de Preparación de Datos...")

        self.Segmentador = SegmentadorDatos()
        self.COLUMNA_CONSUMO_DEMANDA = val_col


    ###################################################################################
    # AGREGAR COLUMNAS DE FECHA AL DATAFRAME
    ###################################################################################
    def agregar_columnas_periodo(self, df):
        # AGREGAR VARIABLES DE FECHA DE CARGA Y DE ELIMINACION
        ahora = pd.to_datetime(self.fecha_base).tz_localize('America/Lima', ambiguous='NaT', nonexistent='NaT')
        df_carga = df.copy()
        df_carga["FECHASEGMENTACION"] = ahora.strftime("%d-%m-%Y")
        df_carga["PERIODOSEGMENTACION"] = int(ahora.strftime("%Y%m"))
        df_carga["FECCARGA"] = ahora
        df_carga["FLGELIMINADO"] = False
        df_carga["FECELIMINACION"] = df_carga["FECCARGA"]
    
        return df_carga

    ###################################################################################
    # FUNCIÓN PARA TRANSFORMAR UN DATAFRAME A LA FORMA DEL OUTPUT DE PROYECCIÓN
    ###################################################################################
    def transformar_a_output(self, df_demanda):
        """
        Función para transformar un dataframe a la forma del Output de Proyección.
        """
        # ELIMINAMOS LAS COLUMNAS QUE NO INTERESAN EN EL OUTPUT
        df_m_o = df_demanda.copy()
        columnas_a_eliminar = ["FECULTIMOCONSUMO", self.COLUMNA_CONSUMO_DEMANDA]
        df_m_o.drop(columns=columnas_a_eliminar, axis=1, inplace=True)

        # AGREGAR COLUMNAS DE PROYECCION
        nuevas_columnas_entero = [
            "CTDCONSUMORIESGO05",
            "CTDCONSUMORIESGO25",
            "CTDCONSUMORIESGO50",
            "CTDCONSUMORIESGO75",
            "CTDCONSUMORIESGO95"
        ]

        for col in nuevas_columnas_entero:
            df_m_o[col] = 0
            df_m_o[col] = df_m_o[col].astype(int)

        # AGREGAR COLUMNAS DE DESCRIPCION
        nuevas_columnas_string = [
            "TIPOMODELOPROYECCION"
        ]
        
        for col in nuevas_columnas_string:
            df_m_o[col] = ""
            df_m_o[col] = df_m_o[col].astype(str)

        # AGREGAR COLUMNAS BANDERAS DE TIPO
        nuevas_columnas_bool = [
            "SKUCONVENTAS",
            "SKUPROYECTABLE",
            "SKUCONOCIDO",
            "SKUACTIVO"
        ]
        
        for col in nuevas_columnas_bool:
            df_m_o[col] = False
            df_m_o[col] = df_m_o[col].astype(bool)

        # AGREGAR COLUMNAS DE PERIODO
        df_m_o_p = self.agregar_columnas_periodo(df_m_o)
        return df_m_o_p

    ###################################################################################
    # FUNCIÓN PARA TRANSFORMAR UN DATAFRAME A LA FORMA DEL OUTPUT DE PROYECCIÓN
    ###################################################################################
    def transformar_a_monitoreo(self, df_demanda):
        """
        Función para transformar un dataframe a la forma del Output de Proyección.
        """
        # ELIMINAMOS LAS COLUMNAS QUE NO INTERESAN EN EL OUTPUT
        df_m_m = df_demanda.copy()
        columnas_a_eliminar = ["FECULTIMOCONSUMO"]
        df_m_m.drop(columns=columnas_a_eliminar, axis=1, inplace=True)

        # ASEGURAR COLUMNA DE CANTIDAD DE VENTAS COMO ENTERO
        df_m_m[self.COLUMNA_CONSUMO_DEMANDA] = df_m_m[self.COLUMNA_CONSUMO_DEMANDA].astype(int)

        # AGREGAR COLUMNAS DE PROYECCION
        nuevas_columnas_entero = [
            "CTDCONSUMORIESGO05",
            "CTDCONSUMORIESGO25",
            "CTDCONSUMORIESGO50",
            "CTDCONSUMORIESGO75",
            "CTDCONSUMORIESGO95",
            "ERRORCONSUMORIESGO05",
            "ERRORCONSUMORIESGO25",
            "ERRORCONSUMORIESGO50",
            "ERRORCONSUMORIESGO75",
            "ERRORCONSUMORIESGO95"
        ]

        for col in nuevas_columnas_entero:
            df_m_m[col] = 0
            df_m_m[col] = df_m_m[col].astype(int)

        # AGREGAR COLUMNAS DE DESCRIPCION
        nuevas_columnas_string = [
            "TIPOMODELOPROYECCION"
        ]
        
        for col in nuevas_columnas_string:
            df_m_m[col] = ""
            df_m_m[col] = df_m_m[col].astype(str)

        # AGREGAR COLUMNAS BANDERAS DE TIPO
        nuevas_columnas_bool = [
            "SKUCONVENTAS",
            "SKUPROYECTABLE",
            "SKUCONOCIDO",
            "SKUACTIVO"
        ]
        
        for col in nuevas_columnas_bool:
            df_m_m[col] = False
            df_m_m[col] = df_m_m[col].astype(bool)

        # AGREGAR COLUMNAS DE PERIODO
        df_m_m_p = self.agregar_columnas_periodo(df_m_m)
        return df_m_m_p
    
    ###################################################################################
    # FUNCIÓN PARA COMPLETAR LOS MESES FALTANTES DE VENTAS DE TODOS LOS PRODUCTOS
    ###################################################################################
    def completar_meses(self, df, fecha_inicio, fecha_final, date_col = "FECHA", val_cols = ["MTOVENTAS"], group_cols = ["CLASIFICACION", "CODSOCIEDAD", "CODCENTRO", "CODMATERIAL"]):
        """
        Función para completar los meses faltantes de ventas de todos los productos.
        """
        # ASEGURAR COLUMNA DE PERIODO
        df[date_col] = pd.to_datetime(df[date_col])

        # OBTENER TODAS LAS COMBINACIONES ÚNICAS DE ALMACEN COMERCIAL - PRODUCTO
        combinaciones = df[group_cols].drop_duplicates()

        # OBTENER TODAS LAS FECHAS DE LA VENTANA
        rango_fechas = pd.date_range(start=fecha_inicio, end=fecha_final, freq='MS')

        # OBTENER TODAS LAS FECHAS PARA TODAS LAS COMBINACIONES DE ALMACÉN COMERCIAL - PRODUCTO
        esqueleto = combinaciones.merge(pd.DataFrame({date_col: rango_fechas}), how='cross')
        df_agregado = df.groupby(group_cols + [date_col]).agg(
            ventas_existentes = (date_col, 'count')
        ).reset_index()

        # HACER UN CRUCE CON LA DATA ORIGINAL PARA MANTENER LOS DATOS EXISTENTES Y AGREGAR LOS MESES FALTANTES
        esqueleto_completo = esqueleto.merge(
            df_agregado,
            on = group_cols + [date_col],
            how = 'left'
        )

        # IMPUTACIÓN CON 0 AQUELLOS MESES DONDE NO HAY VENTAS
        df_faltantes = esqueleto_completo[esqueleto_completo['ventas_existentes'].isna()].copy()
        df_faltantes[val_cols] = 0.0

        # ELIMINAR COLUMNAS AUXILIARES
        df_faltantes = df_faltantes.drop(columns=['ventas_existentes'])
        #df = df.drop(columns=['FECHA_MES'])

        # ORDENAR POR FECHA LA SALIDA
        df_final = pd.concat([df, df_faltantes], ignore_index=True)
        return df_final.sort_values(by=group_cols + [date_col]).reset_index(drop = True)

    ###################################################################################
    # FUNCIÓN PARA PROCESAR LOS DATOS HISTÓRICOS PARA QUE SEAN ÚTILES EN PLANIFICACION
    ###################################################################################
    def procesar_datos_para_planificacion(self, df_input, date_col = "FECHA", val_cols = ["MTOVENTAS"], group_cols = ["CLASIFICACION", "CODSOCIEDAD", "CODCENTRO", "CODMATERIAL"], filters = {}, group_by_month = False, complete_months = False, num_meses = 0):
        """
        Función para procesar los datos históricos para que sean útiles en planificación.
        """
        # ASEGURAND COLUMNA DE FECHA
        df_cliente = df_input.copy()
        df_cliente[date_col] = pd.to_datetime(df_cliente[date_col])

        # DEFINIR PERIODO
        df_cliente["PERIODO"] = df_cliente[date_col].dt.strftime('%Y%m')
        df_cliente["MES"] = pd.to_datetime(df_cliente["PERIODO"] + "01", format="%Y%m%d")

        for val_col in val_cols:
            # ASEGURAR TIPO DE DATOS DE VARIABLES NUMÉRICAS
            df_cliente[val_col] = df_cliente[val_col].astype(float)

            # IMPUTACIÓN DE 0 CUANDO NO HAY VENTAS
            df_cliente[val_col] = df_cliente[val_col].fillna(0)

        # FILTROS (POR DEFECTO ASEGURAR SOLO PRODUCTO TERMINADO Y ALMACENES COMERCIALES)
        for columna, lista_valores in filters.items():
            df_cliente = df_cliente[df_cliente[columna].isin(lista_valores)].reset_index(drop = True)

        # SUMAR VENTAS POR MES
        if group_by_month:
            df_mes = df_cliente.groupby(group_cols + ["MES"])[val_cols].sum().reset_index()

            if complete_months:
                ultimo_mes = df_mes["MES"].max()
                ini_ventana = ultimo_mes - pd.DateOffset(months=(num_meses-1))
                df_completo = self.completar_meses(df_mes, ini_ventana, ultimo_mes, date_col = "MES", val_cols = val_cols, group_cols = group_cols)
                return df_completo
            else:
                return df_mes
        else:
            return df_cliente
        
    ###################################################################################
    # FUNCIÓN PARA SEPARAR SKU'S CONOCIDOS Y DESCONOCIDOS
    ###################################################################################
    def obtener_skus_conocidos_desconocidos(self, df, df_sku, identificadores):
        """
        Función para separar sku's conocidos y desconocidos.
        """
        df_general = df.copy()
        df_skus = df_sku.copy()

        # CREAR UNA COLUMNA DE IDENTIFICADOR ÚNICO
        df_general['_sku_id_combinado'] = df_general[identificadores].astype(str).agg('_'.join, axis=1)
        df_skus['_sku_id_combinado'] = df_skus[identificadores].astype(str).agg('_'.join, axis=1)

        # OBTENER LOS SKUS ÚNICOS PARA CADA DATAFRAME
        skus_en_general = set(df_general['_sku_id_combinado'].unique())
        skus_en_skus = set(df_skus['_sku_id_combinado'].unique())

        # OBTENER LOS SKUS CONOCIDOS POR EL ANÁLISIS QUE NECESITAN PROYECCIÓN ACTUAL
        df_conocidos_activos = df_general[df_general['_sku_id_combinado'].isin(skus_en_skus)].copy()

        # OBTENER LOS SKUS DESCONOCIDOS POR EL ANÁLISIS QUE NECESITAN PROYECCIÓN ACTUAL
        df_desconocidos = df_general[~df_general['_sku_id_combinado'].isin(skus_en_skus)].copy()

        # OBTENER LOS SKUS QUE YA NO NECESITAN PROYECCIÓN ACTUAL
        df_conocidos_inactivos = df_skus[~df_skus['_sku_id_combinado'].isin(skus_en_general)].copy()

        # ELIMINAR COLUMNA DE IDENTIFICADORES UNIFICADOS
        df_conocidos_activos = df_conocidos_activos.drop(columns=['_sku_id_combinado'])
        df_desconocidos = df_desconocidos.drop(columns=['_sku_id_combinado'])
        df_conocidos_inactivos = df_conocidos_inactivos.drop(columns=['_sku_id_combinado'])
        
        return df_conocidos_activos, df_desconocidos, df_conocidos_inactivos
    
    ###################################################################################
    # FUNCIÓN PARA SEPARAR SKU'S QUE SON PROYECTABLES Y LOS QUE NO SON PROYECTABLES
    ###################################################################################
    def obtener_skus_proyectables(self, df, ventana_segmentacion, mes_col, val_col, group_cols_mes, group_cols_product, group_cols_warehouse, prefix_str = None, abc_umb = (0.80, 0.95), xyz_umb = (0.35, 0.80), fsn_umb = (2, 6), condiciones_proyectables = {}, segmentos_proyectables = {"ABC": ["A", "B", "C"], "XYZ": ["X", "Y", "Z"], "FSN": ["F", "S", "N"]}):
        """
        Función para separar sku's que son proyectables y no proyectables
        """
        df_general = df.copy()

        # REALIZAR SEGMENTACIÓN DE DEMANDA
        df_segmentado = self.Segmentador.segmentar_abc_xyz_fsn(
            df_general,
            ventana_segmentacion,
            mes_col = mes_col,
            val_col = val_col,
            group_cols_mes = group_cols_mes,
            group_cols_product = group_cols_product,
            group_cols_warehouse = group_cols_warehouse,
            prefix_str = prefix_str,
            abc_umb = abc_umb,
            xyz_umb = xyz_umb,
            fsn_umb = fsn_umb
        )

        # AGREGAR LOS SEGMENTOS AL DATAFRAME ORIGINAL
        df_general = pd.merge(
            df_general,
            df_segmentado,
            on = group_cols_product,
            how = 'left'
        )

        # AGREGAR FILTROS PARA DEFINIR PROYECTABLES SEGÚN SEGMENTOS
        ventana_segmentacion_str = str(ventana_segmentacion).zfill(2)
        if not prefix_str:
            prefix_str = val_col
        abc_col_demanda = f"ABC{prefix_str}{ventana_segmentacion_str}M"
        xyz_col_demanda = f"XYZ{prefix_str}{ventana_segmentacion_str}M"
        fsn_col_demanda = f"FSN{prefix_str}{ventana_segmentacion_str}M"
        condiciones_segmentos_proyectables = {
            abc_col_demanda: segmentos_proyectables["ABC"],
            xyz_col_demanda: segmentos_proyectables["XYZ"],
            fsn_col_demanda: segmentos_proyectables["FSN"]
        }

        condiciones_proyectables.update(condiciones_segmentos_proyectables)

        # CREANDO UNA MÁSCARA DE PROYECTABLES Y NO PROYECTABLES EN BASE A CONDICIONES DE USUARIO Y DE SEGMENTOS
        mascara_total_proyectable = pd.Series(True, index=df_general.index)
        for columna, lista_valores in condiciones_proyectables.items():
            mascara_filtro_actual = df_general[columna].isin(lista_valores)
            mascara_total_proyectable = mascara_total_proyectable & mascara_filtro_actual

        # FILTRAR DATAFRAME EN PROYECTABLES Y NO PROYECTABLES
        df_no_proyectables = df_general[~mascara_total_proyectable].reset_index(drop=True)
        df_proyectables = df_general[mascara_total_proyectable].reset_index(drop=True)

        return df_proyectables, df_no_proyectables