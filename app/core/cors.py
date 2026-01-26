from fastapi.middleware.cors import CORSMiddleware


def setup_cors(app):
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["https://ankinstructor2025-stack.github.io"],
        allow_credentials=True,
        allow_methods=["GET", "POST", "OPTIONS"],
        allow_headers=["Authorization", "Content-Type"],
        expose_headers=["*"],
        max_age=3600,
    )
)
