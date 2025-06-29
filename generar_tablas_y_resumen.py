import pandas as pd
from pymongo import MongoClient

# ============================
# 1️⃣ Cargar datos desde MongoDB o JSON local
# ============================
MONGO_URI = "mongodb+srv://benja15mz:123@database.5iimvyd.mongodb.net/"
client = MongoClient(MONGO_URI)
db = client["GPS"]
collection = db["Gps"]

cursor = collection.find()
df = pd.DataFrame(list(cursor))

print(f"✅ Registros cargados: {len(df)}")

# ============================
# 2️⃣ Preparar rutas y tramos
# ============================
rutas_unicas = df['ruta'].unique()

ruta_tramos_list = []
for ruta in rutas_unicas:
    tramos_unicos = df[df['ruta'] == ruta]['tramo'].unique()
    ruta_tramos_list.append({
        "ruta": ruta,
        "cantidad_tramos": len(tramos_unicos),
        "tramos": ", ".join(tramos_unicos)
    })

df_rutas = pd.DataFrame(ruta_tramos_list)
print(f"\n✅ Total de rutas: {len(df_rutas)}")
print(df_rutas)

# ============================
# 3️⃣ Preparar buses
# ============================
buses_unicos = sorted(df['n_vehiculo'].unique())
df_buses = pd.DataFrame(buses_unicos, columns=["n_vehiculo"])
print(f"\n✅ Total de buses distintos: {len(df_buses)}")
print(df_buses.head())

# ============================
# 4️⃣ Crear tablas por ruta
# ============================
columns_needed = ["ruta", "tramo", "km_por_tramo_minimo", "km_por_tramo_maximo", "temperatura_motor_c"]
df_filtered = df[columns_needed + ['n_vehiculo']]

tabla_ruta_a = df_filtered[df_filtered["ruta"] == "Ruta A"].drop('ruta', axis=1)
tabla_ruta_b = df_filtered[df_filtered["ruta"] == "Ruta B"].drop('ruta', axis=1)
tabla_ruta_c = df_filtered[df_filtered["ruta"] == "Ruta C"].drop('ruta', axis=1)

# ============================
# 5️⃣ Cuadro resumen global
# ============================
resumen_global = (
    df_filtered
    .groupby(['ruta', 'tramo'])
    .agg(
        km_min_promedio=('km_por_tramo_minimo', 'mean'),
        km_max_promedio=('km_por_tramo_maximo', 'mean'),
        temperatura_promedio=('temperatura_motor_c', 'mean'),
        cantidad_registros=('tramo', 'count'),
        buses_distintos=('n_vehiculo', lambda x: x.nunique())
    )
    .reset_index()
    .sort_values(by=['ruta', 'tramo'])
)

print("\n✅ Cuadro resumen global generado:")
print(resumen_global.head())

# ============================
# 6️⃣ Guardar en Excel
# ============================
import openpyxl

with pd.ExcelWriter("tablas_por_ruta.xlsx", engine="openpyxl") as writer:
    tabla_ruta_a.to_excel(writer, sheet_name="Ruta A", index=False)
    tabla_ruta_b.to_excel(writer, sheet_name="Ruta B", index=False)
    tabla_ruta_c.to_excel(writer, sheet_name="Ruta C", index=False)
    resumen_global.to_excel(writer, sheet_name="Resumen Global", index=False)
    df_rutas.to_excel(writer, sheet_name="Rutas", index=False)
    df_buses.to_excel(writer, sheet_name="Buses", index=False)

print("\n✅ Archivo 'tablas_por_ruta.xlsx' generado con:")
print("- Hoja Ruta A")
print("- Hoja Ruta B")
print("- Hoja Ruta C")
print("- Hoja Resumen Global")
print("- Hoja Rutas")
print("- Hoja Buses")
