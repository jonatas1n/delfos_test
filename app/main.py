from fastapi import FastAPI
from api.routes import router as api_router
from api.source import router as source_router

app = FastAPI(title="Delfos Technical Test API")

app.include_router(api_router)
app.include_router(source_router)
