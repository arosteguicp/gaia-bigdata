import dask.dataframe as dd
from dask.distributed import Client
from dask_ml.cluster import DBSCAN as DaskDBSCAN
from dask_ml.linear_model import Ridge as DaskRidge
from dask_ml.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
import numpy as np
import pandas as pd
from pymongo import MongoClient
from scipy import stats

client = MongoClient("mongodb://localhost:27017/")
db = client["gaia_db"]
collection = db["gaia_clean_parquet"]

# Transforming data to DataFrame
data = list(collection.find())
df = dd.from_pandas(pd.DataFrame(data), npartitions=4)

df = df[[
    'ra', 'dec', 'parallax', 'pmra', 'pmdec', 
    'phot_g_mean_mag', 'bp_rp', 'teff_gspphot', 
    'distance_gspphot', 'location', 'processed_at'
]]

df['longitude'] = df['location'].apply(lambda x: x['coordinates'][0] if isinstance(x, dict) else None, meta=('x', 'f8'))
df['latitude'] = df['location'].apply(lambda x: x['coordinates'][1] if isinstance(x, dict) else None, meta=('x', 'f8'))
df = df.drop(columns=['location'])
df['processed_at'] = dd.to_datetime(df['processed_at'])
df['processed_at'] = (df['processed_at'].astype('int64') // 10**9).astype('int32')

# DBSCAN
X = df[['ra', 'dec']]  
db = DaskDBSCAN(eps=0.1, min_samples=5, metric='euclidean')
df['cluster'] = db.fit_predict(X)

# Future trajectory prediction
df['ra_future'] = df['ra'] + df['pmra']  
df['dec_future'] = df['dec'] + df['pmdec']  

X = df[['ra', 'dec', 'pmra', 'pmdec']]
y = df[['ra_future', 'dec_future']]  

X_train, X_test, y_train, y_test = train_test_split(X.compute(), y.compute(), test_size=0.2, random_state=42)

# Scaling
scaler = StandardScaler()
X_train_scaled = scaler.fit_transform(X_train)
X_test_scaled = scaler.transform(X_test)

# Linear Regression
linear_model = DaskRidge(alpha=1.0)
linear_model.fit(X_train, y_train)

y_pred_linear = linear_model.predict(X_test)

df['pred_ra_linear'] = linear_model.predict(df[['ra', 'dec', 'pmra', 'pmdec']])[:, 0] 
df['pred_dec_linear'] = linear_model.predict(df[['ra', 'dec', 'pmra', 'pmdec']])[:, 1]

# 2. MLPRegressor (Neural Network)
mlp_model = MLPRegressor(hidden_layer_sizes=(100,), max_iter=1000, random_state=42, early_stopping=True, validation_fraction=0.1, n_iter_no_change=10)
mlp_model.fit(X_train_scaled, y_train)

y_pred_mlp = mlp_model.predict(X_test_scaled)
df['pred_ra_mlp'] = mlp_model.predict(df[['ra', 'dec', 'pmra', 'pmdec']])[:, 0]  
df['pred_dec_mlp'] = mlp_model.predict(df[['ra', 'dec', 'pmra', 'pmdec']])[:, 1]  

# 3. Outlier Detection using Z-Score
z_scores = np.abs(stats.zscore(df[['ra', 'dec', 'pmra', 'pmdec']].compute()))  
df['outlier'] = (z_scores > 3).all(axis=1)

df.to_csv('data_gaia_predictions.csv', index=False, single_file=True)
