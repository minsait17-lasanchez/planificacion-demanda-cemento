"""_summary_.
PROYECTO           : [PRJ-25-002] CDS - PLANIFICACIÓN DE LA DEMANDA
NOMBRE             : main
ARCHIVOS  DESTINO  : Tablas en BigQuery
ARCHIVOS  FUENTES  : Tablas en BigQuery
OBJETIVO           : Proyección de Demanda a 18 Meses de Producto Terminado Cemento en Almacenes Comerciales
TIPO               : PY
OBSERVACION        : ---
SCHEDULER          : CLOUD RUN
VERSION            : 1.0
DESARROLLADOR      : SÁNCHEZ AGUILAR LUIS ÁNGEL
PROVEEDOR          : MINSAIT
FECHA              : 10/12/2025
DESCRIPCION        : Cloud Run para Automatizar la Proyección de Demanda a 18 Meses de Producto Terminado Cemento en almacenes comerciales.
"""

# Sección Imports
import logging
import sys

from classes.classes import PlanificadorDemandaPTCemento

# Configurar logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

def main(request):
    """Descripción: Cloud Run para Automatizar la Proyección de Demanda a 18 Meses de Producto Terminado Cemento en almacenes comerciales.
    :param request: Solicitud de entrada, no utilizada en esta implementación
    :type request: any
    """

    ### SCRIPT
    try:
        logging.info("Iniciando Trabajo de Segmentación")
        segmentador = PlanificadorDemandaPTCemento()
        segmentador.ejecutar()
        print(f"OK")
        logging.info("Proceso completado.")
    
    except Exception as e:
        log_message = f"Error en el proceso: {e}"
        logging.error(log_message)
        raise ValueError(f"ERROR: {e}")
    
    sys.exit(0)  # Finaliza el proceso y cierra Cloud Run

if __name__ == "__main__":
    main(None)

