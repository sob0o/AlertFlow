from fastapi import FastAPI

app = FastAPI(title="alert-service")

@app.get("/health")
def health():
    return {"status": "ok", "service": "alert-service"}