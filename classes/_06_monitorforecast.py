"""_summary_.

PROYECTO           : [PRJ-25-002] CDS - PLANIFICACIÓN DE LA DEMANDA
NOMBRE             : 06_monitorforecast
ARCHIVOS  DESTINO  : ---
ARCHIVOS  FUENTES  : ---
OBJETIVO           : Definir procesos para monitorear la proyección
OBSERVACION        : -
SCHEDULER          : CLOUD RUN
VERSION            : 1.0
DESARROLLADOR      : SÁNCHEZ AGUILAR LUIS ÁNGEL
PROVEEDOR          : MINSAIT
FECHA              : 10/12/2025
DESCRIPCION        : Paquete que contiene procesos para monitorear la proyección
"""

# Librerías Básicas
import logging

# Librerías para Datos
import pandas as pd

class GestorMonitoreo:
    """Clase para gestionar la proyección mensual."""

    def __init__(self, val_col, mes_col):
        """
        Inicializa la clase.
        """
        logging.info("Inicializando la clase de Gestor del Monitoreo de la Proyección...")

        self.COLUMNA_CONSUMO_DEMANDA = val_col
        self.COLUMNA_FECHA_CONSUMO_DEMANDA = mes_col

    ###################################################################################
    # FUNCIÓN PARA OBTENER EL DATAFRAME DE MONITOREO DE LA PROYECCIÓN
    ###################################################################################
    def monitorear_modelo(self, df_proyeccion, df_input, identificadores, mes_col):
        """
        Función para obtener el DF de monitoreo de la proyección de demanda.
        """
        if not df_proyeccion.empty and not df_input.empty:
            df_proyeccion[mes_col] = pd.to_datetime(df_proyeccion[self.COLUMNA_FECHA_CONSUMO_DEMANDA])
            df_merged = pd.merge(df_proyeccion, df_input, on=identificadores+[mes_col], how='left')
            df_merged["ERRORCONSUMORIESGO05"] = df_merged["CTDCONSUMORIESGO05"] - df_merged[self.COLUMNA_CONSUMO_DEMANDA]
            df_merged["ERRORCONSUMORIESGO25"] = df_merged["CTDCONSUMORIESGO25"] - df_merged[self.COLUMNA_CONSUMO_DEMANDA]
            df_merged["ERRORCONSUMORIESGO50"] = df_merged["CTDCONSUMORIESGO50"] - df_merged[self.COLUMNA_CONSUMO_DEMANDA]
            df_merged["ERRORCONSUMORIESGO75"] = df_merged["CTDCONSUMORIESGO75"] - df_merged[self.COLUMNA_CONSUMO_DEMANDA]
            df_merged["ERRORCONSUMORIESGO95"] = df_merged["CTDCONSUMORIESGO95"] - df_merged[self.COLUMNA_CONSUMO_DEMANDA]

            df_merged[self.COLUMNA_FECHA_CONSUMO_DEMANDA] = df_merged[mes_col]

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

            nuevas_columnas_string = [
                "TIPOMODELOPROYECCION"
            ]

            nuevas_columnas_bool = [
                "SKUCONVENTAS",
                "SKUPROYECTABLE",
                "SKUCONOCIDO",
                "SKUACTIVO"
            ]
            columnas_finales = identificadores + [self.COLUMNA_FECHA_CONSUMO_DEMANDA, self.COLUMNA_CONSUMO_DEMANDA] + nuevas_columnas_entero + nuevas_columnas_string + nuevas_columnas_bool
            df_final_monitoreo = df_merged[columnas_finales].copy()
        else:
            df_final_monitoreo = pd.DataFrame()
        
        return df_final_monitoreo