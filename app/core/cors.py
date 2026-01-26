from fastapi.middleware.cors import CORSMiddleware


def setup_cors(app):
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],  # 本番では絞る
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
