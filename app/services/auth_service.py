import time
import asyncio
import logging
from typing import Dict, Any
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from app.config.database import Database
from fastapi import Depends, HTTPException
from datetime import datetime

logger = logging.getLogger(__name__)
security = HTTPBearer()

token_cache: Dict[str, Dict[str, Any]] = {}

cache_lock = asyncio.Lock()

async def verify_token(
    authorization: HTTPAuthorizationCredentials = Depends(security),
) -> bool:
    if authorization is None:
        logger.warning("Missing Authorization header")
        return False

    provided_token = authorization.credentials

    try:
        db = Database.get_db()
        if db is None:
            logger.error("Database connection not available")
            return False

        token_doc = await db.auth_tokens.find_one({"token": provided_token})
        if not token_doc:
            logger.warning("Token not found in database")
            return False

        expiry_at_str = token_doc.get("expiry_at", None)
        if expiry_at_str:
            # Adjusted datetime parsing to handle microseconds and optional UTC suffix
            try:
                expiry_at = datetime.strptime(expiry_at_str, '%Y-%m-%d %H:%M:%S.%f UTC')
            except ValueError:
                try:
                    expiry_at = datetime.strptime(expiry_at_str, '%Y-%m-%d %H:%M:%S.%f')
                except ValueError:
                    expiry_at = datetime.strptime(expiry_at_str, '%Y-%m-%d %H:%M:%S')
            if datetime.utcnow() > expiry_at:
                logger.warning("Token expired based on expiry_at")
                return False
        else:
            logger.warning("expiry_at not found in token document")
            return False

        logger.info("Token verified from DB")
        return True

    except Exception as e:
        logger.exception(f"Token verification failed: {str(e)}")
        return False
