"""
Transform & Feature Engineering Pipeline

Builds curated event-level features by safely merging
cleaned events with metadata and reference baselines.
"""

from pymongo import MongoClient
import pandas as pd
import numpy as np

MONGO_URI = "mongodb://localhost:27018"
DB_NAME = "streaming_demo"

client = MongoClient(MONGO_URI)
db = client[DB_NAME]

print(" Extrayendo datos limpios de MongoDB...")

# Cargar solo campos relevantes
events = pd.DataFrame(list(db["cleaned.events"].find({}, {"_id":0,"event_id":1,"entity_id":1,"timestamp_iso":1,"value":1})))
meta   = pd.DataFrame(list(db["cleaned.metadata"].find({}, {"_id":0,"entity_id":1,"category":1,"geo":1,"platform":1})))
ref    = pd.DataFrame(list(db["cleaned.reference"].find({}, {"_id":0,"entity_id":1,"baseline_mean":1,"baseline_std":1,"label":1})))

print(f"Eventos: {len(events)}, Metadata: {len(meta)}, Reference: {len(ref)}")

# Asegurar unicidad
meta = meta.drop_duplicates(subset="entity_id", keep="last")
ref  = ref.drop_duplicates(subset="entity_id", keep="last")

# Merge más seguro
merged = (
    events.merge(meta, on="entity_id", how="left", validate="many_to_one")
          .merge(ref, on="entity_id", how="left", validate="many_to_one")
)

print(" Merge completado.")


merged["timestamp"] = pd.to_datetime(
    merged["timestamp_iso"], errors="coerce"
)
# ---------------------------------

# Calcular métricas
merged["value_delta"] = merged["value"] - merged["baseline_mean"]
merged["z_score"] = (merged["value"] - merged["baseline_mean"]) / merged["baseline_std"]
merged["is_outlier"] = merged["z_score"].abs() >= 3

# Rolling features
merged = merged.sort_values(["entity_id", "timestamp"])
merged["rolling_mean_5"] = merged.groupby("entity_id")["value"].transform(lambda x: x.rolling(5, min_periods=1).mean())
merged["rolling_std_5"]  = merged.groupby("entity_id")["value"].transform(lambda x: x.rolling(5, min_periods=1).std())
merged["value_min_5"]    = merged.groupby("entity_id")["value"].transform(lambda x: x.rolling(5, min_periods=1).min())
merged["value_max_5"]    = merged.groupby("entity_id")["value"].transform(lambda x: x.rolling(5, min_periods=1).max())
merged["trend_5"]        = merged.groupby("entity_id")["value"].transform(lambda x: x - x.shift(4))

merged.replace([np.inf, -np.inf], np.nan, inplace=True)
merged.fillna({"z_score": 0, "trend_5": 0}, inplace=True)

print("Features generadas correctamente.")

# Guardar resultados (sobrescribir)
db["curated.events"].drop()
db["curated.events"].insert_many(merged.to_dict(orient="records"))

db["curated.metadata"].drop()
db["curated.metadata"].insert_many(meta.to_dict(orient="records"))

db["curated.reference"].drop()
db["curated.reference"].insert_many(ref.to_dict(orient="records"))

print(" Datos guardados en MongoDB (colecciones curated.*)")
print("Pipeline completed successfully.")
