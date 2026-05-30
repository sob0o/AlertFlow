from fastapi import FastAPI

app = FastAPI(title="agent-service")

@app.get("/health")
def health():
    return {"status": "ok", "service": "agent-service"}