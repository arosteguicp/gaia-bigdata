
# Análisis de datos espaciales de GAIA para la predicción de ubicación de constelaciones y estrellas con Big Data
Gaia es una misión espacial europea que proporciona astrometría y muchos más datos y métricas de 1.000 millones de estrellas de la Vía Láctea. En este proyecto utilizaremos la mayor cantidad de herramientas aprendidas en Big Data para poder sacar información provechosa de los datos proporcionados por Gaia, con el fin de predecir la ubicación de algunas estrellas en las constelaciones.  

## Ingesta de datos con Kafka  
Nuestro objetivo es recolectar un gran volúmen de datos progresivamente mediante el producer en nuestro caso gaia_kafka_producer2.py.  
Nos apoyamos de la librería **astroquery.gaia** que nos permite acceder a la base de datos de la misión sin necesidad de realizar webscraping en su página principal, en su base de datos nosotros trabajamos con la tabla: **gaiadr3.gaia_source** que es el tercer reporte de la misión.

``````
from astroquery.gaia import Gaia
``````

En nuestro producer,  extraemos los registros de gaia en lotes de 10 mil cada minuto para publicarlos en el topic: gaia_topic, asegurando un flujo constante. Nos conectamos a un broker de Kafka que se ejecuta en localhost:9092 y envía datos al topic, con cada registro serializado en formato JSON.

``````
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_TOPIC = 'gaia_topic'
RECORDS_PER_BATCH = 10000 #10k
INTERVAL_SECONDS = 60 
``````
Aquí es importante resaltar que si se clona el repositorio para correr el proyecto, si el entorno donde se instalaron los requerimientos es de WSL entonces será necesario definir el bootstrap como localhost para evitar errores.  
Ahora, en la consulta a la base dde datos y la tabla que especificamos, nos centramos en atributos numéricos que tengan variabilidad e información sobre las estrellas pertenecientes a las constelaciones.

``````
  query = f"""
    SELECT TOP {RECORDS_PER_BATCH}
        source_id, ra, dec, parallax, pmra, pmdec,
        phot_g_mean_mag, bp_rp, teff_gspphot, distance_gspphot,
        ruwe, phot_variable_flag
    FROM gaiadr3.gaia_source
    WHERE phot_g_mean_mag IS NOT NULL
      AND parallax IS NOT NULL
      AND teff_gspphot IS NOT NULL
      AND source_id > {last_processed_source_id}
    ORDER BY source_id ASC
    """
``````
Para evitar el envío de datos duplicados (con los cuales tuvimos problemas al principio), nos ayudamos de **source_id**, agregándole un **order by** para garantizar un único llamado al registro. Es por eso que en cada iteración por lote se tiene un source_id mayor que el último procesado exitosamente.  

Todo este proceso de extracción y publicación está orquestado para ejecutarse en intervalos de un minuto (en el producer lo especificamos con 60 segundos).
``````
elapsed_time = time.time() - start_time
    time_to_wait = INTERVAL_SECONDS - elapsed_time
    #aqui garatnizamos el tiempo en recolectar mandar y esperamos al minuto para evitar errores de transmisión.
    if time_to_wait > 0:
        print(f"Tiempo transcurrido: {elapsed_time:.2f} segundos. Esperando {time_to_wait:.2f} segundos para la siguiente ejecución ")
        time.sleep(time_to_wait)
    else:
        print(f"La ejecución tardó más de {INTERVAL_SECONDS} segundos ({elapsed_time:.2f}s). Ejecutando inmediatamente el siguiente ciclo")
``````
Después de obtener y enviar un lote de registros, el script calcula el tiempo restante en el minuto y pausa, asegurando una tasa de envío de datos consistente, esto lo agregamos para cubrir casos donde un lote demore más de un minuto en recolectarse y se acumule en el buffer con el siguiente, además también llamamos a ``````producer.flush()`````` para enviar lo que se recolecte hasta el momento.
Resaltamos de igual manera que el tiempo promedio de recolección de registros es de **5 segundos** y que el producer en su tiempo más largo de ejecución (**28 minutos**) recolectó **280 k registros** obteniendo un gran volúmen de datos con los cuales podemos trabajar.









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
Este apartado incluye los gráficos creados con Tableau para analizar las observaciones astronómicas obtenidas de la misión **Gaia**. A través de estos gráficos, se busca visualizar patrones, relaciones y distribuciones clave entre diferentes variables astronómicas.[Enlace a Dashboard]
(https://public.tableau.com/app/profile/paolo5730/viz/GAIA_17516868128430/Dashboard1)

## 1. Mapa de Clústeres
**Descripción**: Este gráfico muestra la ubicación geográfica de los objetos astronómicos clasificados en diferentes **clústeres**. Utilizando las coordenadas de **Longitud** y **Latitud**, cada punto está representado por un color que corresponde a un **cluster** específico.

**Objetivo**: Ver cómo se distribuyen los objetos en el espacio según su **cluster**, proporcionando una visualización espacial de los objetos observados.

## 2. Temperatura Efectiva vs Magnitud
**Descripción**: Este gráfico de dispersión muestra la relación entre la **Temperatura Efectiva** de las estrellas (**Teff Gspphot**) y su **Brillo Aparente** (**Phot G Mean Mag**).

**Objetivo**: Explorar cómo se correlacionan la temperatura y el brillo de las estrellas. Las estrellas más calientes suelen ser más brillantes, lo que se refleja en la tendencia de este gráfico.

## 3. Velocidad de las Estrellas vs Patrón de Movimiento
**Descripción**: Gráfico de barras que muestra la **velocidad media** de las estrellas agrupadas por su **patrón de movimiento**. Los patrones incluyen categorías como "Lento", "Rápido", y "Muy Rápido".

**Objetivo**: Visualizar cómo se distribuyen las velocidades entre los diferentes patrones de movimiento de las estrellas. Esto puede indicar si ciertos patrones de movimiento están asociados con mayores o menores velocidades.


## 4. Relación entre Distancia y Parallax
**Descripción**: Este gráfico de dispersión muestra la relación entre la **distancia estimada de las estrellas** (**Distance Gspphot**) y el **parallax** observado de las mismas estrellas.

**Objetivo**: Verificar la relación inversa entre **parallax** y **distancia**. Según la teoría, una mayor **parallax** debe correlacionarse con una menor **distancia**.

## 5. Gráfico de Errores (RA y DEC)
**Descripción**: Este gráfico de dispersión muestra los **errores** en las coordenadas **RA** (Ascensión Recta) y **DEC** (Declinación) de las estrellas.

**Objetivo**: Analizar los errores en la medición de las coordenadas de las estrellas y evaluar la precisión de las observaciones. Un análisis de los errores es clave para entender la calidad de los datos obtenidos por Gaia.


