from pydantic_settings import BaseSettings


class DomusSettings(BaseSettings):
    mongo_uri: str
    mongo_db_name: str
    google_api_key: str
    reinfolib_api_key: str
