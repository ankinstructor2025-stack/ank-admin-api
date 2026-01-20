from fastapi import FastAPI

app = FastAPI()

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/v1/session")
def get_session():
    return {"state": "OK"}
