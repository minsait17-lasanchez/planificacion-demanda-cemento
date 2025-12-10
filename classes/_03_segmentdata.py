"""_summary_.

PROYECTO           : [PRJ-25-002] CDS - PLANIFICACIÓN DE LA DEMANDA
NOMBRE             : 03_segmentdata
ARCHIVOS  DESTINO  : ---
ARCHIVOS  FUENTES  : ---
OBJETIVO           : Definir procesos para segmentar los datos según ABC-XYZ-FSN
TIPO               : PY
OBSERVACION        : -
SCHEDULER          : CLOUD RUN
VERSION            : 1.0
DESARROLLADOR      : SÁNCHEZ AGUILAR LUIS ÁNGEL
PROVEEDOR          : MINSAIT
FECHA              : 10/12/2025
DESCRIPCION        : Paquete que contiene procesos para segmentar los datos según ABC-XYZ-FSN
"""

# Librerías Básicas
import logging

# Librerías para Datos
import pandas as pd
import numpy as np

class SegmentadorDatos:
    """Clase para la segmentación de la data según ABC-XYZ-FSN."""

    def __init__(self):
        """
        Inicializa la clase.
        """
        logging.info("Inicializando la clase de Segmentador de Datos...")

    ###################################################################################
    # FUNCIÓN PARA CALCULAR EL COEFICIENTE DE VARIACIÓN / DESVIACIÓN RELATIVA A LA MEDIA
    ###################################################################################
    def cv(self, s):
        """
        Función para calcular el coeficiente de variación / desviación relativa a la media.
        """
        # ASEGURAR TOMAR EN CUENTA MESES DONDE HAY VENTAS
        x = s[s > 0]
        
        # EN CASO SOLO HAYA UNA VENTA O NINGUNA, SIMPLEMENTE NO SE PUEDE CALCULAR LA VARIACIÓN
        if len(x) <= 1 or x.mean() == 0:
            return 0
        
        # CALCULAR FINALMENTE EL COEFICIENTE DE VARIACIÓN
        return round(x.std()/ x.mean(), 4)

    ###################################################################################
    # FUNCIÓN PARA CALCULAR LA ROTACIÓN O NÚMERO DE MESES DONDE HAY UN VALOR DIFERENTE A 0
    ###################################################################################
    def rotacion(self, s):
        """
        Función para calcular la rotación o número de meses donde hay un valor diferente a 0 en ventas.
        """
        # MANTENER SOLO MESES CON VENTAS
        x = s[s > 0]

        # CALCULAR CUÁNTOS MESES TIENEN VENTAS RESPECTO AL TOTAL DE MESES
        return round(len(x)*12/len(s), 4)

    ###################################################################################
    # FUNCIÓN PARA SEGMENTACIÓN ABC
    ###################################################################################
    def segmentar_abc_por_acumulado(self, df, value_col, new_col, breaks=(0.80, 0.95)):
        """
        Función para la segmentación ABC.
        """
        a, b = breaks
        # ASEGURAMOS DATOS ORDENADOS POR VALORIZADO
        g = df.sort_values(value_col, ascending=False).copy()
        
        # CALCULAMOS EL TOTAL DEL VALORIZADO DEL GRUPO
        tot = g[value_col].sum()
        
        # SI NO HAY VALORIZADO, DEFINIMOS TODOS LOS PRODUCTOS COMO "C"
        if tot <= 0:
            g[new_col] = "C"

        # SI HAY VALORIZADO, EVALUAMOS EL ACUMULADO
        else:
            g["ACUMULADO"] = g[value_col].cumsum() / tot
            
            # DEFINIMOS EL SEGMENTO DEL PRODUCTO SEGÚN EL ACUMULADO
            g[new_col] = np.where(g["ACUMULADO"] <= a, "A",
                        np.where(g["ACUMULADO"] <= b, "B", "C"))

            # ASEGURAMOS SEGMENTO B EN CASOS DONDE SOLO HAYA SEGMENTOS A Y C (CUANDO LOS UMBRALES SON CERCANOS)
            if (g[new_col] == "B").sum() == 0 and (g[new_col] == "A").any() and (g[new_col] == "C").any():
                idx = g.index[g["ACUMULADO"] > a][0]
                g.loc[idx, new_col] = "B"

            # ELIMINAMOS COLUMNAS AUXILIARES
            g = g.drop(columns=["ACUMULADO"])
        return g

    ###################################################################################
    # FUNCIÓN PARA SEGMENTACIÓN XYZ
    ###################################################################################
    def segmentar_xyz(self, cv, breaks=(0.35, 0.80)):
        """
        Función para la segmentación XYZ.
        """
        a, b = breaks
        # SI EL COEFICIENTE DE VARIACIÓN ES NAN, SIGNIFICA QUE VENTAS EN 0 (LO TOMAMOS COMO ESTABLE)
        if pd.isna(cv): return "X"
        # SI EL COEFICIENTE DE VARIACION ES CERCANO A 0 SE TOMA COMO ESTABLE
        if cv <= a: return "X"
        # SI EL COEFICIENTE DE VARIACION SE VA ALEJANDO DE 0 SE TOMA COMO VARIABLE
        if cv <= b: return "Y"
        # SI EL COEFICIENTE SE ALEJA MUCHO DEL 0 SE TOMA COMO ERRÁTICO
        return "Z"

    ###################################################################################
    # FUNCIÓN PARA SEGMENTACIÓN FSN
    ###################################################################################
    def segmentar_fsn(self, rot, breaks=(2, 6)):
        """
        Función para la segmentación FSN.
        """
        a, b = breaks
        # SI LA ROTACION OCURRE MÁS DE LA MITAD DE LA VENTANA SE CONSIDERA RÁPIDO
        if rot >= b: return "F"
        # SI LA ROTACIÓN OCURRE MÁS DE LA SEXTA PARTE DE LA VENTANA SE CONSIDERA LENTO
        if rot >= a: return "S"
        # SI LA ROTACIÓN OCURRE SOLO UNA VEZ O NINGUNA, SE CONSIDERA SIN ROTACIÓN
        return "N"

    ###################################################################################
    # FUNCIÓN PARA CONSTRUIR LA SEGMENTACIÓN ABC-XYZ-FSN
    ###################################################################################
    def segmentar_abc_xyz_fsn(self, df_mes, ventana_segmentacion, mes_col, val_col, group_cols_mes, group_cols_product, group_cols_warehouse, prefix_str = None, abc_umb = (0.80, 0.95), xyz_umb = (0.35, 0.80), fsn_umb = (2, 6)):
        """
        Función para construir la segmentación ABC-XYZ-FSN.
        """
        ventana_segmentacion_str = str(ventana_segmentacion).zfill(2)

        if not prefix_str:
            prefix_str = val_col

        # VARIABLES - NOMBRES DE NUEVAS VARIABLES DE SEGMENTACIÓN
        val_col_demanda = f"VALORIZADO{prefix_str}{ventana_segmentacion_str}M"
        cv_col_demanda = f"CV{prefix_str}{ventana_segmentacion_str}M"
        rot_col_demanda = f"ROTACION{prefix_str}{ventana_segmentacion_str}M"

        abc_col_demanda = f"ABC{prefix_str}{ventana_segmentacion_str}M"
        xyz_col_demanda = f"XYZ{prefix_str}{ventana_segmentacion_str}M"
        fsn_col_demanda = f"FSN{prefix_str}{ventana_segmentacion_str}M"

        # OBTENER MES FINAL Y MES INICIAL DE LA VENTANA DE SEGMENTACIÓN
        df = df_mes.copy()
        ultimo_mes = df[mes_col].max()
        ini_ventana = ultimo_mes - pd.DateOffset(months=(ventana_segmentacion-1))

        # OBTENER VENTANA DE TIEMPO DE LA DATA
        df_ventana = df[(df[mes_col] >= ini_ventana) & (df[mes_col] <= ultimo_mes)].copy()

        # ASEGURAR ORDENAMIENTO POR PERIODO PARA CADA ALMACÉN COMERCIAL - PRODUCTO
        df_tipo = df_ventana.sort_values(group_cols_mes).reset_index(drop = True).copy()

        # VALORIZADO DE LA DEMANDA
        valorizado_demanda = df_tipo.groupby(group_cols_product)[val_col].sum().rename(val_col_demanda)

        # COEFICIENTE DE VARIACIÓN (CV) DE LA DEMANDA
        variabilidad_demanda = df_tipo.groupby(group_cols_product)[val_col].apply(self.cv).rename(cv_col_demanda)

        # ROTACIÓN SEGÚN CAMBIOS EN LA DEMANDA
        rotacion_demanda = df_tipo.groupby(group_cols_product)[val_col].apply(self.rotacion).rename(rot_col_demanda)

        metricas = pd.concat([valorizado_demanda, variabilidad_demanda, rotacion_demanda], axis=1)
        metricas[val_col_demanda] = metricas[val_col_demanda].fillna(0)

        # SEGMENTACIÓN ABC
        metricas = metricas.groupby(group_cols_warehouse, group_keys=False).apply(lambda g: self.segmentar_abc_por_acumulado(g, value_col=val_col_demanda, new_col=abc_col_demanda, breaks = (abc_umb[0], abc_umb[1])))

        # SEGMENTACIÓN XYZ
        metricas[xyz_col_demanda] = metricas.groupby(group_cols_warehouse)[cv_col_demanda].transform(lambda s: s.apply(self.segmentar_xyz, breaks = (xyz_umb[0], xyz_umb[1])))

        # SEGMENTACIÓN FSN
        metricas[fsn_col_demanda] = metricas.groupby(group_cols_warehouse)[rot_col_demanda].transform(lambda s: s.apply(self.segmentar_fsn, breaks = (fsn_umb[0], fsn_umb[1])))

        df_segmentado = metricas.reset_index().copy()
        return df_segmentado