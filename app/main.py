from fastapi import FastAPI, Request
from fastapi.responses import Response

from core.cors import setup_cors

from routers.public import router as public_router
from routers.admin_core import router as admin_core_router
from routers.invites import router as invites_router
from routers.uploads import router as uploads_router
from routers.judge_method import router as judge_method_router


def create_app() -> FastAPI:
    app = FastAPI()

    # CORS（allow_origins などは core/cors.py に集約）
    setup_cors(app)

    # 念のための preflight（CORSMiddleware が効いていれば基本呼ばれない）
    @app.options("/{path:path}")
    def cors_preflight(path: str, request: Request):
        return Response(status_code=204)

    # health
    @app.get("/health")
    def health():
        return {"ok": True}

    # routers
    app.include_router(public_router)
    app.include_router(admin_core_router)
    app.include_router(invites_router)
    app.include_router(uploads_router)
    app.include_router(judge_method_router)

    return app


app = create_app()
