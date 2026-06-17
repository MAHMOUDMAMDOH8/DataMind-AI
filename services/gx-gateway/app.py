from fastapi import FastAPI
import great_expectations as gx

app = FastAPI(title="DataMind GX Gateway", version="0.1.0")


@app.get("/health")
def health():
    return {"status": "ok", "great_expectations": gx.__version__}
