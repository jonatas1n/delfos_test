import os
from fastapi import APIRouter


router = APIRouter()


@router.get("/")
def read_root():
    return {"message": "FastAPI is running"}


@router.get("/health")
def health_check():
    return {
        "status": "ok",
        "db": {
            "host": os.getenv("DB_HOST", "postgres"),
            "source": os.getenv("DB_SOURCE_NAME", os.getenv("DB_NAME", "source")),
            "target": os.getenv("DB_TARGET_NAME", "target"),
        },
    }
