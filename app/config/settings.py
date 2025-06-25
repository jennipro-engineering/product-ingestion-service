from pydantic_settings import BaseSettings
from functools import lru_cache
from typing import Dict
import json
import os

class Settings(BaseSettings):
    mongodb_url: str = "mongodb+srv://jennipro_dev_user:1EfA1u5Am3swHGiW@devcluster.ld0nukp.mongodb.net/?retryWrites=true&ssl=true&w=majority&maxIdleTimeMS=60000"
    mongodb_name: str = "auth_DB"
    rate_limit_requests: int = 5
    rate_limit_period: int = 60
    gcp_pub_sub_project_id: str = None
    pubsub_topic_id: str = None
    jwt_secret_key:str="77GGUoxBGBEOVAPhl+/YAaH+yqo1ZcNP78njQbWmWKILp6z3c2nBW9g9O+iysXI8K7H6HoZIm3Iskgo/ICf+6g=="
    validate_keys: list = []
    validate_keys_category:dict={}
    validate_values_grouping: dict = {}
    error_messages: dict = {}
    use_config_validation: bool = False
    validation_rules: dict = {}
    gcs_bucket_name: str = "invalid_ingestion_data"

    def __init__(self, **values):
        super().__init__(**values)
        config_path = os.path.join(os.path.dirname(__file__), '../../config.json')
        try:
            with open(config_path, 'r') as f:
                config_data = json.load(f)
                self.validate_keys = config_data.get("validate_keys", [])
                self.validate_keys_category=config_data.get('validate_keys_category',{})
                self.validate_values_grouping = config_data.get("validate_values_grouping", {})
                self.error_messages = config_data.get("error_messages", {})
                self.use_config_validation = config_data.get("use_config_validation", False)
                self.validation_rules = config_data.get("validation_rules", {})
        except Exception as e:
            # Log or handle error if needed
            pass
    
    class Config:
        env_file = ".env"

@lru_cache()
def get_settings():
    return Settings()
