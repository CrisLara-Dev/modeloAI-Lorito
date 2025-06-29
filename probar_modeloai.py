import pandas as pd
import joblib
from pymongo import MongoClient
from datetime import datetime

# ============================
# 1️⃣ Cargar el modelo
# ============================
model = joblib.load("modelo_estado_velocidad.pkl")
print("✅ Modelo cargado correctamente.\n")

# ============================
# 2️⃣ Crear input de prueba
# ============================
input_data = {
    "n_vehiculo": ["BUSLORITO-12"],
    "ruta": ["Ruta B"],
    "tramo": ["Tramo B1"],
    "velocidad_kmh": [26],
    "km_por_tramo_maximo": [24],
    "temperatura_motor_c": [79],
    "dia_semana": ["Martes"],
    "clima": ["Nublado"],
    "hora": [15],
}


df_input = pd.DataFrame(input_data)

# ============================
# 3️⃣ Obtener columnas de referencia desde MongoDB
# ============================
MONGO_URI = "mongodb+srv://benja15mz:123@database.5iimvyd.mongodb.net/"
client = MongoClient(MONGO_URI)
db = client["GPS"]
collection = db["Gps"]

cursor = collection.find().limit(1000)
df_reference = pd.DataFrame(list(cursor))

features = ['n_vehiculo', 'ruta', 'tramo', 'velocidad_kmh',
            'km_por_tramo_maximo',
            'temperatura_motor_c', 'dia_semana', 'clima', 'hora']

df_reference = pd.get_dummies(df_reference[features])

# ============================
# 4️⃣ Procesar input igual que en entrenamiento
# ============================
df_processed = pd.get_dummies(df_input)
df_processed = df_processed.reindex(columns=df_reference.columns, fill_value=0)

# ============================
# 5️⃣ Realizar predicción
# ============================
pred = model.predict(df_processed)[0]

estado_map = {
    0: "Normal (0)",
    1: "Advertencia (1)",
    2: "Exceso de velocidad (2)"
}

# Obtener fecha y hora actual en formato legible
fecha_actual = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# Mostrar resultados detallados
print(f"✅ Resultado de predicción:")
print(f"- Vehículo: {input_data['n_vehiculo'][0]}")
print(f"- Fecha de solicitud: {fecha_actual}")
print(f"- Estado de velocidad: {estado_map[pred]}")
