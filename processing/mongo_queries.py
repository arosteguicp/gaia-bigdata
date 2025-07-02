from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017/")
db = client["gaia_db"]
collection = db["gaia_clean_parquet"]

def estrellas_por_magnitud(min_mag=5, max_mag=15):
    return list(collection.find({
        "phot_g_mean_mag": {"$gte": min_mag, "$lte": max_mag}
    }).limit(10))

def estrellas_temperatura(min_temp=5000, max_temp=7000):
    return list(collection.find({
        "teff_gspphot": {"$gte": min_temp, "$lte": max_temp}
    }).limit(10))

def estrellas_por_distancia(min_dist=0, max_dist=1000):
    return list(collection.find({
        "distance_gspphot": {"$gte": min_dist, "$lte": max_dist}
    }).limit(10))

def estrellas_alta_velocidad(min_pm=100):
    return list(collection.find({
        "$or": [
            {"pmra": {"$gte": min_pm}},
            {"pmdec": {"$gte": min_pm}}
        ]
    }).limit(10))

def estrellas_bril_cerca(max_mag=10, max_dist=100):
    return list(collection.find({
        "phot_g_mean_mag": {"$lt": max_mag},
        "distance_gspphot": {"$lt": max_dist}
    }).limit(10))

if __name__ == "__main__":
    print("Estrellas magnitud 8 a 12:")
    for star in estrellas_por_magnitud(8, 12):
        print(star)

    print("\nEstrellas temperatura 5000K a 6000K:")
    for star in estrellas_temperatura(5000, 6000):
        print(star)


    print("\nEstrellas entre 100 y 300 parsecs:")
    for star in estrellas_por_distancia(100, 300):
        print(star)

    print("\nEstrellas con movimiento propio alto:")
    for star in estrellas_alta_velocidad(150):
        print(star)

    print("\nEstrellas brillantes y cercanas:")
    for star in estrellas_bril_cerca(8, 50):
        print(star)
