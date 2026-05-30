from fastapi import FastAPI

app = FastAPI(title="analyzer-service")

@app.get("/health")
def health():
    return {"status": "ok", "service": "analyzer-service"}