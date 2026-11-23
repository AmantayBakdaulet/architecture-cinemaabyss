import logging
import os
import random

import httpx
from fastapi import FastAPI, Request, Response
from fastapi.responses import PlainTextResponse

MONOLITH_URL = os.getenv("MONOLITH_URL", "http://monolith:8080")
MOVIES_SERVICE_URL = os.getenv("MOVIES_SERVICE_URL", "http://movies-service:8081")
EVENTS_SERVICE_URL = os.getenv("EVENTS_SERVICE_URL", "http://events-service:8082")
GRADUAL_MIGRATION = os.getenv("GRADUAL_MIGRATION", "true").lower() == "true"
MOVIES_MIGRATION_PERCENT = int(os.getenv("MOVIES_MIGRATION_PERCENT", "0"))

log = logging.getLogger("uvicorn.error")
app = FastAPI()
client = httpx.AsyncClient(timeout=10)


def target(path):
    if path.startswith("/api/movies"):
        if not GRADUAL_MIGRATION or random.randint(1, 100) <= MOVIES_MIGRATION_PERCENT:
            return MOVIES_SERVICE_URL
        return MONOLITH_URL
    if path.startswith("/api/events"):
        return EVENTS_SERVICE_URL
    return MONOLITH_URL


@app.get("/health")
def health():
    return PlainTextResponse("Strangler Fig Proxy is healthy")


@app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
async def proxy(request: Request):
    url = target(request.url.path)
    log.info("%s %s -> %s", request.method, request.url.path, url)

    resp = await client.request(
        request.method,
        url + request.url.path,
        params=request.query_params,
        content=await request.body(),
        headers={"Content-Type": request.headers.get("content-type", "application/json")},
    )
    return Response(resp.content, resp.status_code, media_type=resp.headers.get("content-type"))
