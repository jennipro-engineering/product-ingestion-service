from pydantic_settings import BaseSettings
from functools import lru_cache
from typing import Optional
import json
import os

class Settings(BaseSettings):
    mongodb_url: Optional[str] = None
    mongodb_user: Optional[str] = None
    mongodb_password: Optional[str] = None
    mongodb_cluster: Optional[str] = None
    mongodb_name: str
    gcp_pub_sub_project_id: str = None
    pubsub_topic_id: str = None
    jwt_secret_key:str
    validate_keys: list = []
    validate_keys_category:dict={}
    validate_values_grouping: dict = {}
    error_messages: dict = {}
    use_config_validation: bool = False
    gcs_bucket_name: str

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
        except Exception as e:
            # Log or handle error if needed
            pass

        # Build mongodb_url
        if not self.mongodb_url:
            if self.mongodb_user and self.mongodb_password and self.mongodb_cluster:
                self.mongodb_url = (
                    f"mongodb+srv://{self.mongodb_user}:{self.mongodb_password}@{self.mongodb_cluster}/"
                    f"?retryWrites=true&w=majority&appName=DevCluster"
                )
            else:
                raise ValueError("MongoDB URL or user/password/cluster must be set in environment variables.")
    
    class Config:
        env_file = ".env"

@lru_cache()
def get_settings():
    return Settings()
