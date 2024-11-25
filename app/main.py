import logging
logging.basicConfig(level=logging.INFO)

from fastapi import FastAPI, Query
from app.neo4j_access.neo4j_access import Neo4jInstance
import os
from contextlib import asynccontextmanager
from collections.abc import AsyncIterator
from app.db import init_db, get_db
from app.apscheduler import start_scheduler
from app.cache import init_cache
from app.endpoints import community_router, centrality_router, health_router, update_router, search_router, ranking_router, history_router, equality_router

@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    init_db()
    init_cache()
    start_scheduler()
    yield
    db = get_db()
    db.close()

app = FastAPI(lifespan=lifespan)

app.include_router(community_router, prefix="/community")
app.include_router(centrality_router, prefix="/centrality")
app.include_router(health_router, prefix="/health")
app.include_router(update_router, prefix="/update")
app.include_router(search_router, prefix="/search")
app.include_router(ranking_router, prefix="/ranking")
app.include_router(history_router, prefix="/history")
app.include_router(equality_router, prefix="/equality")

