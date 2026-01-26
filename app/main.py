from fastapi import FastAPI, Request
from fastapi.responses import Response
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://ankinstructor2025-stack.github.io"],
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["Authorization", "Content-Type"],
    expose_headers=["*"],
    max_age=3600,
)

@app.options("/{path:path}")
def cors_preflight(path: str, request: Request):
    # CORSMiddleware が効いていれば、ここは呼ばれない（＝保険）
    return Response(status_code=204)

@app.get("/health")
def health():
    return {"ok": True}
