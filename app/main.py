from fastapi import FastAPI
from contextlib import asynccontextmanager
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.utils import get_openapi
from app.routers import ingestion_api,api_gateway
from app.config.database import Database
from dotenv import load_dotenv
from fastapi import HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi import status

load_dotenv()

# Create FastAPI app
@asynccontextmanager
async def lifespan(app: FastAPI):
    await Database.connect_db()
    yield
    await Database.close_db()


app = FastAPI(
    title="Product Ingestion Service",
    description="API for ingesting data from various sources into MongoDB",
    version="1.0.0",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content={
            "status": "error",
            "message": "Invalid request payload",
            "details": exc.errors()
        },
    )

@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "status": "error",
            "message": exc.detail
        },
    )
    
app.include_router(api_gateway.router, prefix="/api/v1")
app.include_router(ingestion_api.router, prefix="/api/v1")


def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema

    openapi_schema = get_openapi(
        title=app.title,
        version=app.version,
        description=app.description,
        routes=app.routes,
    )

    schemas = openapi_schema.get("components", {}).get("schemas", {})
    for unwanted in ["HTTPValidationError", "ValidationError"]:
        schemas.pop(unwanted, None)

    for path_item in openapi_schema.get("paths", {}).values():
        for operation in path_item.values():
            responses = operation.get("responses", {})
            if "422" in responses:
                content = responses["422"].get("content", {})
                if "application/json" in content:
                    schema = content["application/json"].get("schema", {})
                    if "$ref" in schema and "HTTPValidationError" in schema["$ref"]:
                        # Replace with custom schema
                        content["application/json"]["schema"] = {
                            "type": "object",
                            "properties": {
                                "status": {"type": "string", "example": "error"},
                                "message": {"type": "string", "example": "Invalid request payload"},
                                "details": {
                                    "type": "array",
                                    "items": {"type": "object"}
                                }
                            }
                        }

    app.openapi_schema = openapi_schema
    return app.openapi_schema

app.openapi = custom_openapi
