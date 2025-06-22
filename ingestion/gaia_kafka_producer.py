from kafka import KafkaProducer
import json
import time
from astroquery.gaia import Gaia

# Configurar productor Kafka
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Consulta a GAIA usando ADQL
query = """
SELECT TOP 100000
    source_id, ra, dec, parallax, pmra, pmdec, 
    phot_g_mean_mag, bp_rp, teff_val, radius_val, lum_val
FROM gaiadr3.gaia_source
WHERE phot_g_mean_mag IS NOT NULL AND parallax IS NOT NULL AND teff_val IS NOT NULL

"""

print("Consultando datos de GAIA...")
job = Gaia.launch_job(query)
results = job.get_results()
df = results.to_pandas()

# Enviar a Kafka
for _, row in df.iterrows():
    msg = row.to_dict()
    producer.send('gaia_topic', value=msg)
    print("Enviado:", msg)
    time.sleep(0.2)
