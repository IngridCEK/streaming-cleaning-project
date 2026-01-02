
"""
Feature Engineering (sobre datos curated.events)
Genera ≥5 features y persiste en MongoDB: curated.features
"""

from pymongo import MongoClient
import pandas as pd
import numpy as np

MONGO_URI = "mongodb://localhost:27018"
DB_NAME = "streaming_demo"

client = MongoClient(MONGO_URI)
db = client[DB_NAME]

print(" Cargando curated.events...")
events = pd.DataFrame(list(db["curated.events"].find({}, {"_id":0})))
if events.empty:
    raise SystemExit("curated.events está vacío. Corre transform_pipeline primero.")

# Asegura el orden temporal por entidad
events = events.sort_values(["entity_id", "timestamp_iso"])

# --- Inter-arrival time (segundos entre eventos por entidad)
events["timestamp_dt"] = pd.to_datetime(events["timestamp_iso"], errors="coerce", format="mixed")
events["inter_arrival_s"] = events.groupby("entity_id")["timestamp_dt"].diff().dt.total_seconds()
events["inter_arrival_s"] = events["inter_arrival_s"].fillna(0)

# --- Event frequency (eventos / 10 min por entidad)
events["ts_10m"] = events["timestamp_dt"].dt.floor("10min")
freq = (events
        .groupby(["entity_id","ts_10m"])
        .size()
        .reset_index(name="events_10m"))
events = events.merge(freq, on=["entity_id","ts_10m"], how="left")

# --- Rate of change (derivada simple value)
events["roc_1"] = events.groupby("entity_id")["value"].diff()
events["roc_1"] = events["roc_1"].fillna(0)

# --- Rolling z-score (zscore móvil con ventana 5)
def rolling_z(x, w=5):
    m = x.rolling(w, min_periods=2).mean()
    s = x.rolling(w, min_periods=2).std()
    return (x - m) / s
events["rolling_z_5"] = events.groupby("entity_id")["value"].apply(rolling_z).reset_index(level=0, drop=True)

# --- Binary flags
events["is_high_activity"] = (events["events_10m"] >= 8)  # umbral ejemplo
events["is_anomaly_rolling"] = events["rolling_z_5"].abs() >= 3

# Limpieza de infinitos/NaN
events.replace([np.inf, -np.inf], np.nan, inplace=True)
for col in ["rolling_z_5"]:
    events[col] = events[col].fillna(0)

# Selección de columnas para features
feature_cols = [
    "event_id","entity_id","timestamp_iso","value",
    "inter_arrival_s","events_10m","roc_1","rolling_z_5",
    "is_high_activity","is_anomaly_rolling"
]

features = events[feature_cols].copy()

print(f" Features calculadas: {len(features)} filas.")

# Persistir
db["curated.features"].drop()
db["curated.features"].insert_many(features.to_dict(orient="records"))
print(" Guardado en Mongo: curated.features")
