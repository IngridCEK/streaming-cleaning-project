
"""
EDA Corporativo (versión mejorada)
- Distribuciones: RAW vs CLEANED (hist/boxplot) con formato corporativo
- Serie de tiempo: valor vs suavizado (entidad con más eventos)
- Conteos: raw, cleaned, curated + descartados/fijos/imputados
- Tabla de faltantes antes vs después (conteo y %)
- Outliers (curated)
- Guarda gráficos en PNG y SVG -> reports/visuals/
- Guarda resúmenes en CSV -> reports/eda_summary.csv y reports/eda_missing_summary.csv
"""

from pymongo import MongoClient
import pandas as pd
import numpy as np
import os
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
from datetime import datetime

# =========================
# --- Configuración ---
# =========================
MONGO_URI = "mongodb://localhost:27018"   # tu mapeo 27018->27017
DB_NAME = "streaming_demo"

OUT_DIR = "reports/visuals"
os.makedirs(OUT_DIR, exist_ok=True)
os.makedirs("reports", exist_ok=True)

# Paleta y tipografía “corporativa”
PALETTE = {
    "primary": "#1F6FEB",     # azul
    "secondary": "#6E7781",   # gris
    "accent": "#2DA44E",      # verde
    "warning": "#BF8700",     # dorado
    "danger": "#D1242F",      # rojo
}
plt.rcParams.update({
    "figure.figsize": (10, 6),
    "figure.dpi": 120,
    "axes.facecolor": "#FFFFFF",
    "axes.edgecolor": "#E5E7EB",
    "axes.labelcolor": "#111827",
    "axes.titleweight": "bold",
    "axes.titlecolor": "#111827",
    "axes.grid": True,
    "grid.color": "#E5E7EB",
    "grid.linestyle": "-",
    "grid.linewidth": 0.8,
    "xtick.color": "#374151",
    "ytick.color": "#374151",
    "font.size": 11,
    "savefig.bbox": "tight",
})

def savefig_multi(path_no_ext: str):
    """Guarda figura en PNG y SVG con nombres consistentes."""
    plt.tight_layout()
    plt.savefig(f"{path_no_ext}.png")
    plt.savefig(f"{path_no_ext}.svg")
    plt.close()

def fmt_thousands(x, _):
    return f"{x:,.0f}".replace(",", " ")

thousand_formatter = FuncFormatter(fmt_thousands)

def to_numeric_series(s):
    """Convierte a numérico; no convertibles->NaN."""
    if s is None:
        return pd.Series(dtype="float64")
    try:
        return pd.to_numeric(s, errors="coerce")
    except Exception:
        return pd.Series([np.nan]*len(s))

def coerce_datetime(s, colname="timestamp"):
    """Convierte a datetime de forma robusta."""
    if s is None:
        return pd.Series(dtype="datetime64[ns]")
    return pd.to_datetime(s, errors="coerce", utc=False)

# =========================
# --- Conexión Mongo ---
# =========================
client = MongoClient(MONGO_URI)
db = client[DB_NAME]

def safe_load(coll_name, proj):
    try:
        data = list(db[coll_name].find({}, proj))
        return pd.DataFrame(data)
    except Exception as e:
        print(f"[WARN] No se pudo leer {coll_name}: {e}")
        return pd.DataFrame()

raw_events = safe_load("raw_dirty.events", {"_id":0,"entity_id":1,"timestamp":1,"value":1})
clean_events = safe_load("cleaned.events", {"_id":0,"entity_id":1,"timestamp_iso":1,"value":1,"is_null_ts":1,"is_null_value":1})
curated_events = safe_load("curated.events", {"_id":0,"entity_id":1,"timestamp_iso":1,"value":1,
                                              "rolling_mean_5":1,"z_score":1,"is_outlier":1})

print(f"raw_events={len(raw_events)}, cleaned_events={len(clean_events)}, curated_events={len(curated_events)}")

# =========================
# --- Preparación ---
# =========================
raw_val = to_numeric_series(raw_events.get("value"))
clean_val = to_numeric_series(clean_events.get("value"))

# =========================
# 1) Distribuciones: RAW vs CLEANED
# =========================
if raw_val.dropna().shape[0] > 0 or clean_val.dropna().shape[0] > 0:
    # Histograma comparativo
    plt.figure()
    bins = 40
    plt.hist(raw_val.dropna(), bins=bins, alpha=0.65, label="RAW",
             color=PALETTE["secondary"], edgecolor="#FFFFFF")
    plt.hist(clean_val.dropna(), bins=bins, alpha=0.65, label="CLEANED",
             color=PALETTE["primary"], edgecolor="#FFFFFF")
    plt.title("Distribución de valores — RAW vs CLEANED")
    plt.xlabel("Valor")
    plt.ylabel("Frecuencia")
    plt.legend(frameon=False)
    ax = plt.gca()
    ax.yaxis.set_major_formatter(thousand_formatter)
    savefig_multi(os.path.join(OUT_DIR, "01_dist_value_raw_vs_cleaned"))

    # Boxplot comparativo
    plt.figure()
    bp = plt.boxplot([raw_val.dropna(), clean_val.dropna()],
                     labels=["RAW", "CLEANED"],
                     patch_artist=True)
    # Colores caja
    colors = [PALETTE["secondary"], PALETTE["primary"]]
    for patch, c in zip(bp['boxes'], colors):
        patch.set_facecolor(c)
        patch.set_alpha(0.6)
        patch.set_edgecolor("#9CA3AF")
    for whisker in bp['whiskers']:
        whisker.set_color("#9CA3AF")
    for cap in bp['caps']:
        cap.set_color("#9CA3AF")
    for median in bp['medians']:
        median.set_color("#111827")
        median.set_linewidth(2)
    plt.title("Distribución robusta — RAW vs CLEANED (Boxplot)")
    plt.ylabel("Valor")
    ax = plt.gca()
    ax.yaxis.set_major_formatter(thousand_formatter)
    savefig_multi(os.path.join(OUT_DIR, "02_box_value_raw_vs_cleaned"))

# =========================
# 2) Serie de tiempo: valor vs suavizado (entidad top)
# =========================
if not curated_events.empty and "timestamp_iso" in curated_events:
    top_entity = curated_events["entity_id"].value_counts().idxmax()
    ce = curated_events[curated_events["entity_id"] == top_entity].copy()

    ce["ts"] = coerce_datetime(ce["timestamp_iso"], "timestamp_iso")
    ce = ce.dropna(subset=["ts"]).sort_values("ts")

    if not ce.empty:
        plt.figure()
        plt.plot(ce["ts"], ce["value"], label="Valor (curated)", linewidth=1.6, color=PALETTE["primary"])
        if "rolling_mean_5" in ce.columns:
            plt.plot(ce["ts"], ce["rolling_mean_5"], label="Media móvil (5)", linewidth=2.0, color=PALETTE["accent"])
        plt.title(f"Serie temporal — Entidad {top_entity}")
        plt.xlabel("Tiempo")
        plt.ylabel("Valor")
        plt.legend(frameon=False)
        ax = plt.gca()
        ax.yaxis.set_major_formatter(thousand_formatter)
        plt.xticks(rotation=20)
        savefig_multi(os.path.join(OUT_DIR, "03_timeseries_value_vs_rolling"))

        # Agregado diario (promedio) para una vista “ejecutiva”
        daily = ce.set_index("ts")["value"].resample("D").mean().dropna()
        if not daily.empty:
            plt.figure()
            plt.plot(daily.index, daily.values, linewidth=2.0, color=PALETTE["primary"])
            plt.title(f"Promedio diario — Entidad {top_entity}")
            plt.xlabel("Fecha")
            plt.ylabel("Valor promedio (D)")
            ax = plt.gca()
            ax.yaxis.set_major_formatter(thousand_formatter)
            plt.xticks(rotation=20)
            savefig_multi(os.path.join(OUT_DIR, "04_timeseries_daily_mean"))

# =========================
# 3) Conteos + heurísticas de limpieza
# =========================
raw_count = len(raw_events)
clean_count = len(clean_events)
curated_count = len(curated_events)

dropped_noninserted = max(0, raw_count - clean_count)

raw_non_numeric = raw_val.isna().sum()
clean_non_null = clean_events["value"].notna().sum() if "value" in clean_events else np.nan
fixed_or_valid = (clean_non_null - (raw_count - raw_non_numeric)
                  if pd.notna(clean_non_null) else np.nan)

imputed_ts = clean_events["is_null_ts"].sum() if "is_null_ts" in clean_events else (
    clean_events["timestamp_iso"].isna().sum() if "timestamp_iso" in clean_events else np.nan
)
imputed_val = clean_events["is_null_value"].sum() if "is_null_value" in clean_events else (
    clean_events["value"].isna().sum() if "value" in clean_events else np.nan
)

summary_counts = pd.DataFrame({
    "metric": [
        "raw_count","clean_count","curated_count",
        "dropped_noninserted","raw_non_numeric","clean_non_null",
        "imputed_ts","imputed_val","fixed_or_valid_est",
        "run_ts"
    ],
    "value": [
        raw_count, clean_count, curated_count,
        dropped_noninserted, raw_non_numeric, clean_non_null,
        imputed_ts, imputed_val, fixed_or_valid,
        datetime.now().isoformat(timespec="seconds")
    ]
})
summary_counts.to_csv("reports/eda_summary.csv", index=False)

# Barra de conteos con anotaciones
plt.figure()
labels = ["RAW","CLEANED","CURATED"]
vals = [raw_count, clean_count, curated_count]
bars = plt.bar(labels, vals, color=[PALETTE["secondary"], PALETTE["primary"], PALETTE["accent"]])
plt.title("Volumen de registros por capa")
plt.ylabel("Número de registros")
ax = plt.gca()
ax.yaxis.set_major_formatter(thousand_formatter)

for bar in bars:
    height = bar.get_height()
    plt.text(bar.get_x() + bar.get_width()/2, height,
             f"{int(height):,}".replace(",", " "),
             ha="center", va="bottom", fontsize=10, color="#111827")

savefig_multi(os.path.join(OUT_DIR, "05_counts_by_layer"))

# =========================
# 4) Faltantes antes vs después
# =========================
raw_ts_missing = raw_events["timestamp"].isna().sum() if "timestamp" in raw_events else np.nan
raw_val_missing = raw_val.isna().sum()
clean_ts_missing = imputed_ts if pd.notna(imputed_ts) else np.nan
clean_val_missing = imputed_val if pd.notna(imputed_val) else np.nan

missing_summary = pd.DataFrame({
    "field": ["timestamp","value"],
    "raw_missing": [raw_ts_missing, raw_val_missing],
    "clean_missing": [clean_ts_missing, clean_val_missing],
})

# Porcentajes (cuando aplique)
def pct(missing, total):
    if pd.isna(missing) or total in [0, np.nan]:
        return np.nan
    return (missing / total) * 100

missing_summary["raw_missing_pct"] = [
    pct(raw_ts_missing, raw_count),
    pct(raw_val_missing, raw_count)
]
missing_summary["clean_missing_pct"] = [
    pct(clean_ts_missing, clean_count if clean_count else np.nan),
    pct(clean_val_missing, clean_count if clean_count else np.nan)
]

missing_summary.to_csv("reports/eda_missing_summary.csv", index=False)

# Visual simple de faltantes
plt.figure()
fields = missing_summary["field"].tolist()
raw_m = missing_summary["raw_missing"].fillna(0).tolist()
clean_m = missing_summary["clean_missing"].fillna(0).tolist()

x = np.arange(len(fields))
width = 0.36

plt.bar(x - width/2, raw_m, width=width, label="RAW", color=PALETTE["secondary"])
plt.bar(x + width/2, clean_m, width=width, label="CLEANED", color=PALETTE["primary"])
plt.title("Valores faltantes — Antes vs Después")
plt.ylabel("Cantidad de faltantes")
plt.xticks(x, fields)
plt.legend(frameon=False)
ax = plt.gca()
ax.yaxis.set_major_formatter(thousand_formatter)
savefig_multi(os.path.join(OUT_DIR, "06_missing_before_vs_after"))

# =========================
# 5) Outliers (curated)
# =========================
if not curated_events.empty and "is_outlier" in curated_events.columns:
    counts = curated_events["is_outlier"].value_counts(dropna=False)
    n_out = int(counts.get(True, 0))
    n_ok = int(counts.get(False, 0))
    total_c = n_out + n_ok
    outlier_rate = (n_out / total_c * 100) if total_c > 0 else 0.0

    # Barra con tasa en título
    plt.figure()
    plt.bar(["No outlier","Outlier"], [n_ok, n_out],
            color=[PALETTE["accent"], PALETTE["danger"]])
    plt.title(f"Curated: Conteo de outliers (tasa={outlier_rate:.2f}%)")
    ax = plt.gca()
    ax.yaxis.set_major_formatter(thousand_formatter)
    for i, v in enumerate([n_ok, n_out]):
        plt.text(i, v, f"{v:,}".replace(",", " "), ha="center", va="bottom", fontsize=10)
    savefig_multi(os.path.join(OUT_DIR, "07_outliers_bar"))

print("✅ EDA corporativo listo. Archivos en 'reports/visuals' y CSVs en 'reports/'.")
