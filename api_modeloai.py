import pandas as pd
import numpy as np
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import joblib
import os
import gdown
import json
from datetime import datetime
import time
from typing import Optional

# Variables globales
model: Optional[object] = None
columns_reference = None
df_resumen = None
initialized = False

# Primero definimos la app
app = FastAPI(
    title="API Monitoreo Flota IA",
    description="Predice el estado de velocidad de buses Lorito consultando datos desde resumen_local.xlsx sin Mongo.",
    version="1.0.2"
)

# Luego agregamos el middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Ahora definimos el modelo de datos
class InputData(BaseModel):
    n_vehiculo: str
    ruta: str
    tramo: str
    velocidad_kmh: int
    temperatura_motor_c: int
    dia_semana: str
    clima: str
    hora: int

# Configuración del modelo y archivos
MODEL_PATH = "estado_velocidad.pkl"
GOOGLE_DRIVE_ID = "1I0rWEKvQ7Xg-NsrThvUw5MBojt2HWFen"

# Evento de inicio
@app.on_event("startup")
async def startup_event():
    global model, columns_reference, df_resumen, initialized
    if initialized:
        return
    
    print("Iniciando proceso de carga...")
    
    # Esperar un poco para asegurar que el sistema esté listo
    time.sleep(2)
    
    # Verificar y descargar modelo si no existe
    if not os.path.exists(MODEL_PATH):
        print("Descargando modelo desde Google Drive...")
        try:
            gdown.download(f"https://drive.google.com/uc?id={GOOGLE_DRIVE_ID}", MODEL_PATH, quiet=False)
            print("✅ Modelo descargado exitosamente.")
        except Exception as e:
            print(f"Error al descargar el modelo: {str(e)}")
            raise HTTPException(status_code=500, detail="Error al descargar el modelo")
    
    # Cargar modelo
    try:
        model = joblib.load(MODEL_PATH)
        print("✅ Modelo cargado correctamente.")
    except Exception as e:
        print(f"Error al cargar el modelo: {str(e)}")
        raise HTTPException(status_code=500, detail="Error al cargar el modelo")

    # Cargar columnas de entrenamiento
    try:
        with open("columnas_entrenamiento.json", "r") as f:
            columns_reference = json.load(f)
        print("✅ Columnas de referencia cargadas.")
    except Exception as e:
        print(f"Error al cargar columnas de referencia: {str(e)}")
        raise HTTPException(status_code=500, detail="Error al cargar columnas de referencia")

    # Cargar resumen_global.xlsx
    try:
        df_resumen = pd.read_excel("resumen_global.xlsx")
        print("✅ Resumen Global cargado correctamente.")
    except Exception as e:
        print(f"Error al cargar resumen global: {str(e)}")
        raise HTTPException(status_code=500, detail="Error al cargar resumen global")

    initialized = True

# Endpoint principal
@app.get("/")
def health_check():
    return {"status": "API funcionando correctamente 🚀"}

# Endpoint de predicción
@app.post("/predict")
def predict_estado(data: InputData):
    global df_resumen

    # Consultar km_por_tramo_maximo desde df_resumen
    tramo_data = df_resumen[
        (df_resumen["ruta"] == data.ruta) & (df_resumen["tramo"] == data.tramo)
    ]

    if tramo_data.empty:
        raise HTTPException(
            status_code=404,
            detail=f"No se encontró información para la ruta {data.ruta} y el tramo {data.tramo}."
        )

    km_por_tramo_maximo = tramo_data.iloc[0]["km_max_promedio"]

    # Crear DataFrame de entrada
    input_dict = data.dict()
    input_dict["km_por_tramo_maximo"] = km_por_tramo_maximo

    df_input = pd.DataFrame([input_dict])

    # Procesar columnas como en el entrenamiento
    df_processed = pd.get_dummies(df_input)
    df_processed = df_processed.reindex(columns=columns_reference, fill_value=0)

    # Predicción
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