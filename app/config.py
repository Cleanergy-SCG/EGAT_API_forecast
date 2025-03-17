from pydantic import BaseSettings
from dotenv import load_dotenv
import os

class Settings(BaseSettings):
    ENVIRONMENT: str
    DATABASE_URL: str
    DEBUG: bool

    class Config:
        env_file = f".env.{os.getenv('ENVIRONMENT', 'dev')}"

# Load environment variables
load_dotenv(Settings.Config.env_file)

settings = Settings()
