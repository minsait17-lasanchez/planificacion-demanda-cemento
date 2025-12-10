# Proyecto: [PRJ-25-002] CDS - PLANIFICACIÓN DE LA DEMANDA

## Introducción
Este proyecto tiene como objetivo hacer proyecciones de cantidad de venta de múltiples productos de cemento de los almacenes comerciales con el objetivo de que sean utilizados en la Planificación de Producción de PT (Producto Terminado).

La presente Cloud Run es específicamente del modelo de proyección de productos de Cemento en almacenes comerciales.

## Comenzando
Siga los siguientes pasos para poner en marcha el código en su propio sistema.

### Instalación
1. Clonar el repositorio.
2. Instalar las dependencias necesarias utilizando el archivo `requirements.txt`:
    ```sh
    pip install -r requirements.txt
    ```

### Dependencias de Software
- `pandas==2.2.2`
- `numpy==1.26.4`
- `pandas-gbq==0.24.0`
- `autogluon.timeseries==1.4.0`
- `autogluon.core==1.4.0`
- `autogluon.common==1.4.0`
- `google-cloud-bigquery==3.38.0`
- `google-cloud-storage==2.19.0`
- `google-cloud-aiplatform==1.122.0`
- `google-api-core==2.28.0`
- `db-dtypes==1.2.0`
- `pyarrow==16.1.0`
- `python-dateutil==2.9.0.post0`

### Ejecución
El archivo principal es `main.py`, que ejecuta la clase `PlanificadorDemandaPTCemento` para realizar la segmentación de producto terminado en almacenes comerciales.

```python
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
```

## Construcción y Pruebas
Para construir y probar el código, asegúrese de que todas las dependencias estén instaladas y ejecute el archivo `main.py`.


Para más detalles sobre cómo crear buenos archivos README, consulte las siguientes [directrices](https://docs.microsoft.com/en-us/azure/devops/repos/git/create-a-readme?view=azure-devops).