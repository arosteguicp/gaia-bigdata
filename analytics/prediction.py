import pandas as pd
from pymongo import MongoClient
from sklearn.cluster import DBSCAN
from sklearn.linear_model import LinearRegression, Ridge
from sklearn.neural_network import MLPRegressor
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_squared_error, r2_score
import numpy as np
from scipy import stats

client = MongoClient("mongodb://localhost:27017/")
db = client["gaia_db"]
collection = db["gaia_clean_parquet"]

# Transforming data to DataFrame
data = list(collection.find())
df = pd.DataFrame(data)

df = df[[
    'ra', 'dec', 'parallax', 'pmra', 'pmdec', 
    'phot_g_mean_mag', 'bp_rp', 'teff_gspphot', 
    'distance_gspphot', 'location', 'processed_at'
]]

df['longitude'] = df['location'].apply(lambda x: x['coordinates'][0] if isinstance(x, dict) else None)
df['latitude'] = df['location'].apply(lambda x: x['coordinates'][1] if isinstance(x, dict) else None)
df = df.drop(columns=['location'])
df['processed_at'] = pd.to_datetime(df['processed_at'])
df['processed_at'] = df['processed_at'].astype('int64') // 10**9
df['processed_at'] = df['processed_at'].astype('int32')

# DBSCAN
X = df[['ra', 'dec']].values
db = DBSCAN(eps=0.1, min_samples=5, metric='euclidean')
df['cluster'] = db.fit_predict(X)

# Future trajectory prediction
df['ra_future'] = df['ra'] + df['pmra']  
df['dec_future'] = df['dec'] + df['pmdec']  

X = df[['ra', 'dec', 'pmra', 'pmdec']] 
y = df[['ra_future', 'dec_future']]  
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

scaler = StandardScaler()
X_train_scaled = scaler.fit_transform(X_train)
X_test_scaled = scaler.transform(X_test)

# 1. Linear Regression 
linear_model = Ridge(alpha=1.0) 
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
z_scores = np.abs(stats.zscore(df[['ra', 'dec', 'pmra', 'pmdec']]))
df['outlier'] = (z_scores > 3).all(axis=1) 

df.to_csv('data_gaia_predictions.csv', index=False)
