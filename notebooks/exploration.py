

"""

- Distribuciones: RAW vs CLEANED (histograma y boxplot)
- Serie de tiempo: valor vs suavizado
- Conteos: raw, cleaned, curated
- Tabla de faltantes antes vs después (conteo y %)
- Outliers (curated)
-  gráficos en PNG y SVG (reports/visuals)
- resúmenes en CSV
"""

from pymongo import MongoClient
import pandas as pd
import numpy as np
import os
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
from datetime import datetime, timezone

# =========================
# Configuración
# =========================
MONGO_URI = "mongodb://localhost:27018"
DB_NAME = "streaming_demo"

OUT_DIR = "reports/visuals"
os.makedirs(OUT_DIR, exist_ok=True)
os.makedirs("reports", exist_ok=True)

plt.rcParams.update({
    "figure.figsize": (10, 6),
    "figure.dpi": 120,
    "axes.grid": True,
    "savefig.bbox": "tight",
})

def savefig_multi(path_no_ext: str):
    """Guarda figura en PNG y SVG."""
    plt.tight_layout()
    plt.savefig(f"{path_no_ext}.png")
    plt.savefig(f"{path_no_ext}.svg")
    plt.close()

def fmt_thousands(x, _):
    return f"{x:,.0f}".replace(",", " ")

thousand_formatter = FuncFormatter(fmt_thousands)

def to_numeric_series(s):
    """Convierte a numérico; no convertibles = NaN."""
    if s is None:
        return pd.Series(dtype="float64")
    return pd.to_numeric(s, errors="coerce")

def coerce_datetime(s):
    """Convierte a datetime timezone-aware."""
    if s is None:
        return pd.Series(dtype="datetime64[ns]")
    return pd.to_datetime(s, errors="coerce", utc=True)

# =========================
# Conexión Mongo
# =========================
client = MongoClient(MONGO_URI)
db = client[DB_NAME]

def safe_load(collection, projection):
    try:
        data = list(db[collection].find({}, projection))
        return pd.DataFrame(data)
    except Exception as e:
        print(f"[WARN] No se pudo leer {collection}: {e}")
        return pd.DataFrame()

raw_events = safe_load(
    "raw_dirty.events",
    {"_id": 0, "entity_id": 1, "timestamp": 1, "value": 1}
)

clean_events = safe_load(
    "cleaned.events",
    {"_id": 0, "entity_id": 1, "timestamp_iso": 1, "value": 1,
     "is_null_ts": 1, "is_null_value": 1}
)

curated_events = safe_load(
    "curated.events",
    {"_id": 0, "entity_id": 1, "timestamp_iso": 1, "value": 1,
     "rolling_mean_5": 1, "z_score": 1, "is_outlier": 1}
)

print(
    f"raw_events={len(raw_events)}, "
    f"cleaned_events={len(clean_events)}, "
    f"curated_events={len(curated_events)}"
)

# =========================
# Preparación
# =========================
raw_val = to_numeric_series(raw_events.get("value"))
clean_val = to_numeric_series(clean_events.get("value"))

# =========================
#  Distribuciones: RAW vs CLEANED
# =========================
if raw_val.dropna().any() or clean_val.dropna().any():

    # Histograma
    plt.figure()
    bins = 40
    plt.hist(raw_val.dropna(), bins=bins, alpha=0.5, label="RAW")
    plt.hist(clean_val.dropna(), bins=bins, alpha=0.5, label="CLEANED")
    plt.title("Distribución de valores — RAW vs CLEANED")
    plt.xlabel("Valor")
    plt.ylabel("Frecuencia")
    plt.legend()
    plt.gca().yaxis.set_major_formatter(thousand_formatter)
    savefig_multi(os.path.join(OUT_DIR, "01_dist_value_raw_vs_cleaned"))

    # Boxplot 
    plt.figure()
    plt.boxplot(
        [raw_val.dropna(), clean_val.dropna()],
        labels=["RAW", "CLEANED"]
    )
    plt.title("Distribución robusta — RAW vs CLEANED (Boxplot)")
    plt.ylabel("Valor")
    plt.gca().yaxis.set_major_formatter(thousand_formatter)
    savefig_multi(os.path.join(OUT_DIR, "02_box_value_raw_vs_cleaned"))

# =========================
# Serie de tiempo (entidad top)
# =========================
if not curated_events.empty and "timestamp_iso" in curated_events.columns:

    top_entity = curated_events["entity_id"].value_counts().idxmax()
    ce = curated_events[curated_events["entity_id"] == top_entity].copy()

    ce["ts"] = coerce_datetime(ce["timestamp_iso"])
    ce = ce.dropna(subset=["ts"]).sort_values("ts")

    if not ce.empty:
        plt.figure()
        plt.plot(ce["ts"], ce["value"], label="Valor", linewidth=1.5)
        if "rolling_mean_5" in ce.columns:
            plt.plot(ce["ts"], ce["rolling_mean_5"],
                     label="Media móvil (5)", linewidth=2.0)
        plt.title(f"Serie temporal — Entidad {top_entity}")
        plt.xlabel("Tiempo")
        plt.ylabel("Valor")
        plt.legend()
        plt.gca().yaxis.set_major_formatter(thousand_formatter)
        plt.xticks(rotation=20)
        savefig_multi(os.path.join(OUT_DIR, "03_timeseries_value_vs_rolling"))

# =========================
#  Conteos por capa
# =========================
raw_count = len(raw_events)
clean_count = len(clean_events)
curated_count = len(curated_events)

summary_counts = pd.DataFrame({
    "metric": ["raw_count", "clean_count", "curated_count", "run_ts"],
    "value": [
        raw_count,
        clean_count,
        curated_count,
        datetime.now(timezone.utc).isoformat(timespec="seconds")
    ]
})

summary_counts.to_csv("reports/eda_summary.csv", index=False)

plt.figure()
plt.bar(["RAW", "CLEANED", "CURATED"],
        [raw_count, clean_count, curated_count])
plt.title("Volumen de registros por capa")
plt.ylabel("Número de registros")
plt.gca().yaxis.set_major_formatter(thousand_formatter)
savefig_multi(os.path.join(OUT_DIR, "04_counts_by_layer"))

# =========================
#  Faltantes antes vs después
# =========================
raw_ts_missing = raw_events["timestamp"].isna().sum() if "timestamp" in raw_events else np.nan
raw_val_missing = raw_val.isna().sum()

clean_ts_missing = clean_events["is_null_ts"].sum() if "is_null_ts" in clean_events else np.nan
clean_val_missing = clean_events["is_null_value"].sum() if "is_null_value" in clean_events else np.nan

missing_summary = pd.DataFrame({
    "field": ["timestamp", "value"],
    "raw_missing": [raw_ts_missing, raw_val_missing],
    "clean_missing": [clean_ts_missing, clean_val_missing],
})

missing_summary.to_csv("reports/eda_missing_summary.csv", index=False)

plt.figure()
x = np.arange(len(missing_summary))
width = 0.35

plt.bar(x - width/2, missing_summary["raw_missing"], width, label="RAW")
plt.bar(x + width/2, missing_summary["clean_missing"], width, label="CLEANED")
plt.title("Valores faltantes — Antes vs Después")
plt.xticks(x, missing_summary["field"])
plt.ylabel("Cantidad de faltantes")
plt.legend()
plt.gca().yaxis.set_major_formatter(thousand_formatter)
savefig_multi(os.path.join(OUT_DIR, "05_missing_before_vs_after"))

# =========================
#  Outliers
# =========================
if not curated_events.empty and "is_outlier" in curated_events.columns:

    counts = curated_events["is_outlier"].value_counts()
    n_out = int(counts.get(True, 0))
    n_ok = int(counts.get(False, 0))

    plt.figure()
    plt.bar(["No outlier", "Outlier"], [n_ok, n_out])
    plt.title("Curated — Conteo de outliers")
    plt.gca().yaxis.set_major_formatter(thousand_formatter)
    savefig_multi(os.path.join(OUT_DIR, "06_outliers_bar"))

print("EDA listo.")
