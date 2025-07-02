from pymongo import MongoClient, GEOSPHERE
import pandas as pd
import os
import glob

client = MongoClient("mongodb://localhost:27017/")
db = client["gaia_db"]
collection = db["gaia_clean_csv"]
collection.create_index([("location", GEOSPHERE)])

csv_dir = "./output/cleaned_stream/csv"
files = glob.glob(os.path.join(csv_dir, "*.csv"))
print(f"Archivos encontrados: {len(files)}")

for file in files:
    try:
        print(f"Procesando: {file}")
        df = pd.read_csv(file)

        if df.empty:
            print("Archivo vacío. Omitido.")
            continue

        df["ra_long"] = df["ra"].apply(lambda ra: ra - 360 if ra > 180 else ra)
        df["location"] = df.apply(lambda row: {
            "type": "Point",
            "coordinates": [row["ra_long"], row["dec"]]
        }, axis=1)
        df.drop(columns=["ra_long"], inplace=True)

        collection.insert_many(df.to_dict(orient="records"))
        print(f"{len(df)} registros insertados.")
    except Exception as e:
        print(f"Error en {file}: {e}")
