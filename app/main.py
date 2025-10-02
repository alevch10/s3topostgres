from fastapi import FastAPI
from app.config import settings
from app.api.endpoints import router
from app.logger import logger

app = FastAPI(
    title=settings.title,
    description=settings.description,
    version=settings.version,
    debug=settings.debug,
)

app.include_router(router)


@app.on_event("shutdown")
async def shutdown_event():
    logger.info("Application shutdown")


if __name__ == "__main__":
    logger.info("Starting uvicorn server")
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
