import os

import uvicorn

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from database import create_tables
from models import etf as etf_models
from repositories.etf import EtfRepository
from router.etf import router as etf_router




@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize database tables and seed allowed bot username on startup."""
    _ = etf_models
    await create_tables()

    allowed_username = os.getenv("ALLOWED_USERNAME", "@vlados7529")
    await EtfRepository.add_allowed_user(allowed_username)

    yield




app = FastAPI(lifespan=lifespan)




app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)




app.include_router(etf_router)




if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        reload=True,
        port=3001,
        host="0.0.0.0",
    )