from fastapi import FastAPI
import pickle
import numpy as np

model = pickle.load(open("model.pkl", "rb"))
app = FastAPI()

@app.post("/predict")
def predict(features: dict):
    X = np.array([features['distance'], features['hour']]).reshape(1, -1)
    eta = model.predict(X)[0]
    return {"predicted_eta": eta}