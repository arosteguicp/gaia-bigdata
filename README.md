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
