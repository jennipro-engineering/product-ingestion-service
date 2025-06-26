import logging
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from typing import AsyncGenerator
from .settings import get_settings

settings = get_settings()
logger = logging.getLogger("mongodb")

class Database:
    client: AsyncIOMotorClient = None
    db: AsyncIOMotorDatabase = None

    @classmethod
    async def connect_db(cls):
        """Create database connection and initialize indexes."""
        try:
            cls.client = AsyncIOMotorClient(settings.mongodb_url)
            cls.db = cls.client[settings.mongodb_name]
            logger.info(f"Connected to MongoDB database: {settings.mongodb_name}")
        except Exception as e:
            logger.error(f"Error connecting to MongoDB: {e}")
            raise

    @classmethod
    async def close_db(cls):
        """Close database connection."""
        try:
            if cls.client:
                cls.client.close()
                logger.info("Closed MongoDB connection.")
        except Exception as e:
            logger.error(f"Error closing MongoDB connection: {e}")

    @classmethod
    def get_db(cls) -> AsyncIOMotorDatabase:
        """Get database instance."""
        return cls.db

    @classmethod
    async def health_check(cls) -> bool:
        """Check MongoDB connection health."""
        try:
            await cls.client.admin.command('ping')
            logger.info("MongoDB health check successful.")
            return True
        except Exception as e:
            logger.error(f"MongoDB health check failed: {e}")
            return False

async def get_database() -> AsyncGenerator[AsyncIOMotorDatabase, None]:
    """Dependency for getting database instance."""
    yield Database.get_db()
