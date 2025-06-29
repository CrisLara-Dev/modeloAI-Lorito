from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import pandas as pd
import joblib
from datetime import datetime
from pymongo import MongoClient
import json

app = FastAPI(
    title="API Monitoreo Flota IA",
    description="Predice el estado de velocidad de buses Lorito, consultando km_por_tramo_maximo automáticamente.",
    version="1.0.1"
)

class InputData(BaseModel):
    n_vehiculo: str
    ruta: str
    tramo: str
    velocidad_kmh: int
    temperatura_motor_c: int
    dia_semana: str
    clima: str
    hora: int

# =======================
# 1️⃣ Cargar modelo y columnas al iniciar
# =======================
model = None
columns_reference = None

@app.on_event("startup")
def load_model_and_columns():
    global model, columns_reference
    model = joblib.load("modelo_estado_velocidad.pkl")
    print("✅ Modelo cargado correctamente.")

    # Leer columnas de entrenamiento
    with open("columnas_entrenamiento.json", "r") as f:
        columns_reference = json.load(f)
    print("✅ Columnas de referencia cargadas.")

    # Configurar conexión Mongo para uso en consultas
    global mongo_client, collection
    MONGO_URI = "mongodb+srv://benja15mz:123@database.5iimvyd.mongodb.net/"
    mongo_client = MongoClient(MONGO_URI)
    db = mongo_client["GPS"]
    collection = db["Gps"]
    print("✅ Conexión Mongo configurada para consultas de km_por_tramo_maximo.")

@app.get("/")
def health_check():
    return {"status": "API funcionando correctamente 🚀"}

# =======================
# 2️⃣ Endpoint de predicción
# =======================
@app.post("/predict")
def predict_estado(data: InputData):
    # Consultar km_por_tramo_maximo automáticamente de la BD
    tramo_info = collection.find_one(
        {"ruta": data.ruta, "tramo": data.tramo},
        {"km_por_tramo_maximo": 1}
    )

    if not tramo_info or "km_por_tramo_maximo" not in tramo_info:
        raise HTTPException(
            status_code=404,
            detail=f"No se encontró km_por_tramo_maximo para {data.ruta} - {data.tramo}."
        )
    km_por_tramo_maximo = tramo_info["km_por_tramo_maximo"]

    # Crear DataFrame de entrada
    input_dict = data.dict()
    input_dict["km_por_tramo_maximo"] = km_por_tramo_maximo  # Inyectar el valor consultado

    df_input = pd.DataFrame([input_dict])

    # Procesar columnas
    df_processed = pd.get_dummies(df_input)
    df_processed = df_processed.reindex(columns=columns_reference, fill_value=0)

    pred = model.predict(df_processed)[0]
    estado_map = {
        0: "Normal (0)",
        1: "Advertencia (1)",
        2: "Exceso de velocidad (2)"
    }
    fecha_actual = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    return {
        "vehiculo": data.n_vehiculo,
        "fecha_prediccion": fecha_actual,
        "estado_velocidad": estado_map[pred]
    }
