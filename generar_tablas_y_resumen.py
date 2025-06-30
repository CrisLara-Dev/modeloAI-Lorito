import pandas as pd
from pymongo import MongoClient

# ============================
# 1️⃣ Cargar datos desde MongoDB
# ============================
MONGO_URI = "mongodb+srv://benja15mz:123@bigdata.nmf477i.mongodb.net/"
client = MongoClient(MONGO_URI)
db = client["GPS"]
collection = db["GPS"]

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
# 4️⃣ Preparar tablas por ruta
# ============================
columns_needed = ["ruta", "tramo", "km_por_tramo_minimo", "km_por_tramo_maximo", "temperatura_motor_c"]
df_filtered = df[columns_needed + ['n_vehiculo']]

tabla_ruta_a = df_filtered[df_filtered["ruta"] == "Ruta A"].drop('ruta', axis=1)
tabla_ruta_b = df_filtered[df_filtered["ruta"] == "Ruta B"].drop('ruta', axis=1)
tabla_ruta_c = df_filtered[df_filtered["ruta"] == "Ruta C"].drop('ruta', axis=1)

# ============================
# 5️⃣ Generar resumen global
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

print("\n✅ Resumen global generado:")
print(resumen_global.head())

# ============================
# 6️⃣ Contar climas, estados de velocidad y días de semana
# ============================
conteo_clima = df['clima'].value_counts().reset_index()
conteo_clima.columns = ['clima', 'cantidad']

conteo_estado_velocidad = df['estado_velocidad'].value_counts().sort_index().reset_index()
conteo_estado_velocidad.columns = ['estado_velocidad', 'cantidad']

conteo_dias = df['dia_semana'].value_counts().reset_index()
conteo_dias.columns = ['dia_semana', 'cantidad']

print("\n✅ Conteo de climas:")
print(conteo_clima)

print("\n✅ Conteo de estados de velocidad:")
print(conteo_estado_velocidad)

print("\n✅ Conteo de días de semana:")
print(conteo_dias)

# ============================
# 7️⃣ Guardar archivo tablas_por_ruta.xlsx
# ============================
with pd.ExcelWriter("tablas_por_ruta.xlsx", engine="openpyxl") as writer:
    tabla_ruta_a.to_excel(writer, sheet_name="Ruta A", index=False)
    tabla_ruta_b.to_excel(writer, sheet_name="Ruta B", index=False)
    tabla_ruta_c.to_excel(writer, sheet_name="Ruta C", index=False)
    resumen_global.to_excel(writer, sheet_name="Resumen Global", index=False)
    df_rutas.to_excel(writer, sheet_name="Rutas", index=False)
    df_buses.to_excel(writer, sheet_name="Buses", index=False)
    conteo_clima.to_excel(writer, sheet_name="Conteo Climas", index=False)
    conteo_estado_velocidad.to_excel(writer, sheet_name="Conteo Estados", index=False)
    conteo_dias.to_excel(writer, sheet_name="Conteo Dias", index=False)

print("\n✅ Archivo 'tablas_por_ruta.xlsx' generado con todas las hojas organizadas (incluyendo conteo de días).")

# ============================
# 8️⃣ Guardar archivo resumen_global.xlsx
# ============================
with pd.ExcelWriter("resumen_global.xlsx", engine="openpyxl") as writer:
    resumen_global.to_excel(writer, sheet_name="Resumen Global", index=False)

print("\n✅ Archivo 'resumen_global.xlsx' generado con el resumen global limpio.")
