from kafka import KafkaProducer
import json
import time
from astroquery.gaia import Gaia
import pandas as pd 


KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_TOPIC = 'gaia_topic'
RECORDS_PER_BATCH = 10000 #10k
INTERVAL_SECONDS = 60 



try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        
    )
    print("Productor Kafka configurado.")
except Exception as e:
    print(f"ERROR al configurar el productor Kafka: {e}")
    exit(1)

#mapear ids para correcto manejo de registross no repetidos en envío de bloks
last_processed_source_id = 0



print(f"Iniciando el proceso de extracción y envío a Kafka cada {INTERVAL_SECONDS} segundos")

while True:
    start_time = time.time()
    
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

    print(f"\n--- {time.strftime('%Y-%m-%d %H:%M:%S')} ---")
    print(f"Consultando {RECORDS_PER_BATCH} registros de GAIA a partir de source_id > {last_processed_source_id} ")

    try:
        job = Gaia.launch_job(query)
        results = job.get_results()
        df = results.to_pandas()
        
        num_extracted = len(df)
        print(f"Se extrajeron {num_extracted} registros de GAIA.")

        if num_extracted == 0:
            #si esq supera el limite
            print("No se encontraron nuevos registros. Posiblemente se ha alcanzado el final del dataset o no hay datos nuevos.")
            

        new_max_source_id_in_batch = last_processed_source_id
        for _, row in df.iterrows():
            msg = row.to_dict()
            current_source_id = msg.get('source_id') 

            producer.send(KAFKA_TOPIC, value=msg)

            if current_source_id is not None and current_source_id > new_max_source_id_in_batch:
                new_max_source_id_in_batch = current_source_id
            
        #importante actualizar id
        if new_max_source_id_in_batch > last_processed_source_id:
            last_processed_source_id = new_max_source_id_in_batch
            print(f"Último source_id procesado actualizado a: {last_processed_source_id}")
        

        producer.flush() #q no se queden en buffer
        print(f"Se enviaron {num_extracted} registros al topic '{KAFKA_TOPIC}'.")

    except Exception as e:
        print(f"ERROR durante la consulta o envío: {e}")

    
    elapsed_time = time.time() - start_time
    time_to_wait = INTERVAL_SECONDS - elapsed_time
    #aqui garatnizamos el tiempo en recolectar mandar y esperamos al minuto para evitar errores de transmisión.
    if time_to_wait > 0:
        print(f"Tiempo transcurrido: {elapsed_time:.2f} segundos. Esperando {time_to_wait:.2f} segundos para la siguiente ejecución ")
        time.sleep(time_to_wait)
    else:
        print(f"La ejecución tardó más de {INTERVAL_SECONDS} segundos ({elapsed_time:.2f}s). Ejecutando inmediatamente el siguiente ciclo")
