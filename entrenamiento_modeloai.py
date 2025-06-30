import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, confusion_matrix
from pymongo import MongoClient
import joblib
import json

# ============================
# 1️⃣ Conexión a MongoDB Atlas
# ============================
MONGO_URI = "mongodb+srv://benja15mz:123@bigdata.nmf477i.mongodb.net/"
client = MongoClient(MONGO_URI)
db = client["GPS"]
collection = db["GPS"]

cursor = collection.find()
df = pd.DataFrame(list(cursor))

print(f"\n✅ Registros cargados: {len(df)}")
print(df.head())
print("\n✅ Columnas cargadas:")
print(df.columns)

# ============================
# 2️⃣ Verificar columnas necesarias
# ============================
required_columns = [
    'velocidad_kmh', 'km_por_tramo_maximo', 'hora', 'estado_velocidad'
]
missing_cols = [col for col in required_columns if col not in df.columns]
if missing_cols:
    print(f"\n🚩 Faltan columnas necesarias: {missing_cols}")
    exit()

# ============================
# 3️⃣ Preparar features y target
# ============================
features = [
    'n_vehiculo', 'ruta', 'tramo',
    'velocidad_kmh', 'km_por_tramo_maximo',
    'temperatura_motor_c', 'dia_semana', 'clima', 'hora'
]

missing_features = [col for col in features if col not in df.columns]
if missing_features:
    print(f"\n🚩 Faltan columnas para entrenamiento: {missing_features}")
    exit()

X = pd.get_dummies(df[features])
y = df['estado_velocidad']

print("\n✅ Conteo de clases en 'estado_velocidad':")
print(y.value_counts())

# ============================
# 4️⃣ Dividir en entrenamiento y prueba
# ============================
min_class_count = y.value_counts().min()
if min_class_count < 2:
    print("\n⚠️ Alguna clase tiene menos de 2 registros, removiendo stratify.")
    stratify_option = None
else:
    stratify_option = y

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=stratify_option
)

# ============================
# 5️⃣ Entrenar modelo Random Forest con 'class_weight=balanced'
# ============================
print("\n✅ Entrenando modelo Random Forest con class_weight='balanced'...")
model = RandomForestClassifier(
    n_estimators=150,
    random_state=42,
    class_weight='balanced',
    n_jobs=-1
)
model.fit(X_train, y_train)

# ============================
# 6️⃣ Evaluar modelo
# ============================
y_pred = model.predict(X_test)

print("\n✅ Matriz de Confusión:")
print(confusion_matrix(y_test, y_pred))

print("\n✅ Classification Report:")
print(classification_report(y_test, y_pred, digits=4))

# ============================
# 7️⃣ Guardar modelo
# ============================
joblib.dump(model, "modelo_estado_velocidad.pkl")
print("\n✅ Modelo entrenado y guardado como 'modelo_estado_velocidad.pkl'.")

# ============================
# 8️⃣ Guardar columnas de entrenamiento
# ============================
with open("columnas_entrenamiento.json", "w") as f:
    json.dump(list(X.columns), f)

print("\n✅ Columnas de entrenamiento guardadas como 'columnas_entrenamiento.json'. Úsalas en tu API para evitar errores de columnas al predecir.")
