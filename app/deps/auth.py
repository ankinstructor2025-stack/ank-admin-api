import os
from fastapi import Depends, HTTPException
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import firebase_admin
from firebase_admin import auth as firebase_auth, credentials as firebase_credentials

auth_scheme = HTTPBearer(auto_error=False)

def _init_firebase():
    if not firebase_admin._apps:
        firebase_admin.initialize_app(
            firebase_credentials.ApplicationDefault(),
            {"projectId": os.environ.get("FIREBASE_PROJECT_ID") or os.environ.get("GOOGLE_CLOUD_PROJECT")},
        )

def require_user(cred: HTTPAuthorizationCredentials = Depends(auth_scheme)):
    if not cred or cred.scheme.lower() != "bearer":
        raise HTTPException(status_code=401, detail="missing Authorization: Bearer <idToken>")
    _init_firebase()
    token = cred.credentials
    try:
        decoded = firebase_auth.verify_id_token(token)
        return decoded  # decoded["uid"]
    except Exception as e:
        raise HTTPException(status_code=401, detail=f"invalid token: {str(e)}")
