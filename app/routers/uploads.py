from fastapi import APIRouter

router = APIRouter()

@router.get("/v1/uploads/health")
def uploads_health():
    return {"ok": True}
