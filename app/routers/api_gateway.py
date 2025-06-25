import logging
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from jose import jwt
from datetime import datetime, timedelta
from app.config.database import Database
from app.config.settings import get_settings

router = APIRouter()
logger = logging.getLogger(__name__)

settings = get_settings()
SECRET_KEY = settings.jwt_secret_key
ALGORITHM = "HS256"
TOKEN_EXPIRY_SECONDS = 3600

class TokenRequest(BaseModel):
    client_id: str
    client_secret: str
    grant_type: str = "client_credentials"


@router.post("/ProductIngestionToken")
async def generate_oauth2_token(body: TokenRequest):
    try:
        db = Database.get_db()
        logger.info(f"Database object: {db}")
        if db is None:
            raise HTTPException(status_code=500, detail="Database not connected")

        # fint seller from database using client_id and client_secret
        seller = await db.auth_credentials.find_one({
            "client_id": body.client_id,
            "client_secret": body.client_secret
        })

        if not seller:
            raise HTTPException(status_code=403, detail="Invalid client_id or client_secret")

        # calculate expiry time with current time and add 1 hour to it
        expiry_time = datetime.utcnow() + timedelta(seconds=TOKEN_EXPIRY_SECONDS)

        payload = {
            "source": seller["seller_id"],
            "client_id": seller["client_id"],
            "client_secret": seller["client_secret"],
            "exp": int(expiry_time.timestamp())
        }
        # create token
        token = jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)

        # upsert to db
        await db.auth_tokens.update_one(
            {"_id": body.client_id},
            {"$set": {
                "token": token,
                "expiry": int(expiry_time.timestamp()),
                "expiry_at": expiry_time.strftime('%Y-%m-%d %H:%M:%S.%f'),
                "token_type": "Bearer",
                "client_id":body.client_id
            }},
            upsert=True
        )

        return {
            "access_token": token,
            "token_type": "Bearer",
            "expires_in": TOKEN_EXPIRY_SECONDS,
            "expires_at": expiry_time.strftime('%Y-%m-%d %H:%M:%S.%f') +' UTC'
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error generating token: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")
