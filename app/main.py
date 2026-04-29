"""
Learning Behavior Tracker – FastAPI Entry Point
Serve: API + Static files (GUI)
"""
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

load_dotenv()

from app.database import DB
from app.api.endpoints import router


@asynccontextmanager
async def lifespan(app: FastAPI):
    DB.connect()
    yield
    DB.close()


app = FastAPI(
    title="Learning Behavior Tracker",
    description="Hệ thống tracking hành vi học tập – AstraDB / Apache Cassandra",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# API routes
app.include_router(router, prefix="/api")

# Serve static files (GUI)
static_dir = os.path.join(os.path.dirname(__file__), "static")
app.mount("/static", StaticFiles(directory=static_dir), name="static")


@app.get("/", include_in_schema=False)
async def root():
    return FileResponse(os.path.join(static_dir, "index.html"))


@app.get("/health")
async def health():
    try:
        DB.session().execute("SELECT now() FROM system.local")
        return {"status": "ok", "db": "AstraDB connected"}
    except Exception as e:
        return {"status": "error", "detail": str(e)}
