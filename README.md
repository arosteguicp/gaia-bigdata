
# Análisis de datos espaciales de GAIA para la predicción de ubicación de constelaciones y estrellas con Big Data
Gaia es una misión espacial europea que proporciona astrometría y muchos más datos y métricas de 1.000 millones de estrellas de la Vía Láctea. En este proyecto utilizaremos la mayor cantidad de herramientas aprendidas en Big Data para poder sacar información provechosa de los datos proporcionados por Gaia, con el fin de predecir la ubicación de algunas estrellas en las constelaciones.  

## Ingesta de datos con Kafka  
Nuestro objetivo es recolectar un gran volúmen de datos progresivamente mediante el consumer.py 








## Procesamiento con Spark

Este módulo se encarga de recibir los datos emitidos desde Kafka y aplicar un pipeline de limpieza, imputación de valores nulos y transformación, utilizando Apache Spark.

### Funcionalidades principales:
- Conexión con Kafka para lectura en modo streaming.
- Validación física de rangos para columnas como `ra`, `dec`, `parallax`, etc.
- Imputación de nulos:
  - `bp_rp`: media agrupada por cuartiles de parallax.
  - `teff_gspphot` y `distance_gspphot`: media global.
- Agrega una columna `processed_at` con el timestamp actual.
- Guarda los datos en formato Parquet como resultado limpio.

El archivo principal para el procesamiento es:
```bash
processing/gaia_streaming_cleaner.py
```

---

### 2. Detalles del Pipeline de Limpieza

### Limpieza y Validaciones

| Variable              | Unidad         | Rango físico permitido            | Acción correctiva                  |
|-----------------------|----------------|-----------------------------------|------------------------------------|
| `ra`                 | grados         | 0 a 360                           | Limitar al rango                   |
| `dec`                | grados         | -90 a 90                          | Limitar al rango                   |
| `parallax`           | mas            | > 0                               | Convertir a valor absoluto         |
| `pmra`, `pmdec`      | mas/año        | -200 a 200                        | Limitar al rango                   |
| `phot_g_mean_mag`    | magnitudes     | 3 a 21                            | Limitar al rango                   |
| `bp_rp`              | magnitudes     | 0 a 5                             | Valor absoluto si < 0, límite sup |
| `teff_gspphot`       | Kelvin (K)     | 2500 a 15000                      | Valor absoluto si < 0, límite sup |
| `distance_gspphot`   | parsecs (pc)   | > 0, máximo razonable: 50000      | Valor absoluto, límite superior    |


### Ejecución

Asegúrate de tener el contenedor corriendo con Kafka. 
```bash
docker run -it --rm `
  --network ingestion_default `
  -v ${PWD}:/app `
  -w /app/ingestion `
  python:3.9 `
  bash -c "pip install -r requirements.txt && python gaia_kafka_producer_jamona.py"
```

Luego, ejecuta el siguiente comando desde la raíz del proyecto para iniciar el procesamiento:

```bash
docker run -it --rm `
  --network ingestion_default `
  -v ${PWD}:/app `
  -w /app/processing `
  bitnami/spark:3.1.3 `
  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.3 gaia_streaming_cleaner.py 
```

## Carga y Almacenamiento en MongoDB

Una vez que los datos han sido procesados y limpiados por Spark, el sistema permite cargarlos a MongoDB para facilitar consultas complejas, visualizaciones y análisis espaciales.

### Scripts disponibles:

- `processing/store_to_mongo.py`: carga los archivos **Parquet** a `gaia_clean_parquet`.
- `processing/store_csv_to_mongo.py`: carga los archivos **CSV** a `gaia_clean_csv`.

Ambos scripts crean el índice geoespacial (`2dsphere`) sobre el campo `location`.


### Ejecución

Asegurarse de tener MongoDB corriendo localmente o en Docker:

```bash
docker run -d --name mongo-gaia -p 27017:27017 mongo
```

## Consultas frecuentes con MongoDB

El archivo `processing/mongo_queries.py` permite realizar búsquedas rápidas y reutilizables sobre los datos astronómicos almacenados.

### Consultas disponibles:

| Consulta                            | Descripción |
|-------------------------------------|-------------|
| `estrellas_por_magnitud()`         | Rango de magnitud `phot_g_mean_mag` |
| `estrellas_temperatura()`          | Rango de temperatura efectiva |
| `estrellas_por_distancia()`        | Rango de distancia en parsecs |
| `estrellas_alta_velocidad()`       | Movimiento propio (`pmra`, `pmdec`) elevado |
| `estrellas_bril_cerca()`           | Estrellas brillantes y próximas |

### Ejecución desde terminal

```bash
python processing/mongo_queries.py
```

## Analytics: Predicción de Ubicación de Estrellas y Análisis de Movimiento

Una vez que los datos han sido limpiados y almacenados, aplicamos modelos de Machine Learning para predecir las posiciones futuras de las estrellas (en las coordenadas `ra` y `dec`) y detectar patrones de movimiento. Utilizamos **Regresión Lineal**  y **MLPRegressor** para esta tarea. Además, implementamos **DBSCAN** para la identificación de cúmulos de estrellas en función de su proximidad espacial.

### DBSCAN: Identificación de Cúmulos de Estrellas

Primero, aplicamos DBSCAN para identificar cúmulos de estrellas en las coordenadas `ra` y `dec`. DBSCAN es un algoritmo de agrupamiento que identifica densidades de puntos y puede detectar "ruido" o estrellas que no forman parte de ningún cúmulo.

```python
from sklearn.cluster import DBSCAN

X = df[['ra', 'dec']].values
db = DBSCAN(eps=0.1, min_samples=5, metric='euclidean')
df['cluster'] = db.fit_predict(X)  
```

### Regresión Lineal 

A continuación, utilizamos Regresión Ridge para predecir las posiciones futuras de las estrellas. Ridge es una forma de regresión lineal con regularización L2, lo que ayuda a prevenir el sobreajuste y mejora la generalización del modelo.

```python
from sklearn.linear_model import Ridge

linear_model = Ridge(alpha=1.0)  
linear_model.fit(X_train, y_train)
y_pred_linear = linear_model.predict(X_test)
df['pred_ra_linear'] = linear_model.predict(df[['ra', 'dec', 'pmra', 'pmdec']])[:, 0] 
df['pred_dec_linear'] = linear_model.predict(df[['ra', 'dec', 'pmra', 'pmdec']])[:, 1]  
```

### MLPRegressor

Usamos MLPRegressor, un modelo de red neuronal, para capturar patrones no lineales en los datos y mejorar la precisión de las predicciones. Este modelo utiliza múltiples capas ocultas para aprender relaciones más complejas entre las características de las estrellas.

```python
from sklearn.neural_network import MLPRegressor

mlp_model = MLPRegressor(hidden_layer_sizes=(100,), max_iter=1000, random_state=42, 
                         early_stopping=True, validation_fraction=0.1, n_iter_no_change=10)
mlp_model.fit(X_train_scaled, y_train)
y_pred_mlp = mlp_model.predict(X_test_scaled)
df['pred_ra_mlp'] = mlp_model.predict(df[['ra', 'dec', 'pmra', 'pmdec']])[:, 0]  
df['pred_dec_mlp'] = mlp_model.predict(df[['ra', 'dec', 'pmra', 'pmdec']])[:, 1] 
```

### Detección de Outliers

Para detectar valores atípicos en las variables de entrada (por ejemplo, ra, dec, pmra, pmdec), utilizamos el Z-Score. Los valores cuyo Z-Score es mayor a 3 se marcan como outliers.

```python
from scipy import stats

z_scores = np.abs(stats.zscore(df[['ra', 'dec', 'pmra', 'pmdec']]))
df['outlier'] = (z_scores > 3).all(axis=1)
```

## Visualización
