# Streaming Cleaning & Data Mining Pipeline

Proyecto de limpieza, transformación y análisis de datos en streaming  
para la materia de **Data Mining**.  
Implementa un flujo completo con **Kafka + MongoDB + Python (Pandas)**  
para limpiar, transformar, enriquecer y analizar datos en tiempo real.

---

##  Estructura del Proyecto

streaming-cleaning-project/
│
├── producer/
│ └── dirty_producer.py
│
├── consumer/
│ ├── cleaning_consumer.py
│ ├── transform_pipeline.py
│ └── feature_engineering.py
│
├── notebooks/
│ └── exploration.py
│
├── reports/
│ ├── features_dictionary.md
│ ├── eda_summary.csv
│ ├── eda_missing_summary.csv
│ └── visuals/
│ ├── dist_value_raw_vs_cleaned.png
│ ├── box_value_raw_vs_cleaned.png
│ ├── timeseries_value_vs_rolling.png
│ └── outliers_bar.png
│
├── docker-compose.yml
├── requirements.txt
└── README.md


---

## Requisitos Previos

 tener instalado:
-  **Python 3.10+**
-  **Docker y Docker Compose**
-  **Git Bash** o una terminal compatible
-  Librerías del proyecto:
  ```bash
  pip install -r requirements.txt

  ---

## levantar sercicios con docker 
docker compose up -d
Esto levanta los contenedores de: Kafka, Zookeeper, MongoDB
## verificar que esten corriendo
docker compose ps


## ejecucion paso a paso

1- python producer/dirty_producer.py --bootstrap localhost:9092 --rate 5
2- python consumer/cleaning_consumer.py
-  **colecciones generadas**
raw_dirty.events
cleaned.events
cleaned.metadata
cleaned.reference

3-python consumer/transform_pipeline.py
4-python consumer/feature_engineering.py
5-python notebooks/exploration.py

## resultados en mongoDB
docker exec -it mongodb mongosh --quiet --port 27017

## dentro de mongosh
use streaming_demo
show collections
db["curated.events"].findOne()
db["curated.features"].findOne()

## indicadores de la calidad de los datos 
Métrica	             Antes (RAW)	           Después (CLEANED)	            Mejoría
Registros totales	  ~30,000	                     ~28,000	     -6.7% (duplicados eliminados)
Valores nulos	       ~15%	                            <2%	   
Formatos inválidos	  múltiples	                      ninguno	
Outliers detectados	    N/A	                            ~3%
Features nuevas	         —	                             10        (inter_arrival, rolling_z, etc.)	


## conceptos principales
Kafka: Plataforma de mensajería para streaming de datos.
Consumer/Producer: Arquitectura de lectura/escritura asíncrona.
MongoDB: Base NoSQL usada para persistencia.
z-score: Mide desviación respecto a la media.
Rolling features: Calcula tendencias en ventanas móviles.
Feature Engineering: Nuevas variables para enriquecer el modelo.
EDA: Análisis exploratorio para validar limpieza y consistencia.

## Este proyecto demuestra la capacidad para construir un flujo de procesamiento de datos en tiempo real, con almacenamiento persistente, limpieza, transformación, ingeniería de características y análisis exploratorio visual.