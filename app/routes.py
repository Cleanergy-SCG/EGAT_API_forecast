from fastapi import APIRouter, Request, HTTPException,Response,Depends,BackgroundTasks
import hashlib, time, base64, os

import json
from datetime import datetime, timezone
import pyodbc
import httpx,requests
import logging
import secrets
import jwt
from typing import Optional
import shutil

logging.basicConfig(level=logging.INFO)


DEBUG_MODE = 0
TOKEN_EXP_TIME = 900

TPA_SERVER_URL = "https://tpa.pea.co.th/stg-matching"

# Null handle
null = None
Null = None

SECRET_KEY = "S48wVj91QvXQOgjUkDdRnYd9WQ4VbYsrU1zgit7BEdU"
ALGORITHM = "HS256"
TOKEN_FILE = "api_matching_key/token.txt"
# PEA_token_matching = ""

conn_str = (
    r'DRIVER={SQL Server};'
    r'SERVER=172.29.23.180;'
    r'DATABASE=PEA_meter_data;'
    r'UID=postsavp;'
    r'PWD=Clean@100923;'
)

## for PEA token saving
class TokenManager:
    def __init__(self):
        self.access_token = None  # Store token here
    
    def set_token(self, token: str):
        self.access_token = token

    def get_token(self):
        if not self.access_token:
            raise Exception("Token not available. Please authenticate first.")
        return self.access_token

token_manager = TokenManager()

class ProjectKeyManager:
    def __init__(self):
        self.tpa_api_secret_key = None  
        self.agg_ca = None 

    def set_key(self,agg_ca: str, tpa_api_secret_key: str):
        self.tpa_api_secret_key = tpa_api_secret_key
        self.agg_ca = agg_ca
        print(f"update key {self.tpa_api_secret_key} on project {self.agg_ca}")

    def get_key(self):

        data = {
            "agg_ca": self.agg_ca,
            "tpa_api_secret_key": self.tpa_api_secret_key
        }
        return data

projectKeyManager = ProjectKeyManager()    
    

## for AGG server generate token to PEA access and response to our sserver
def generate_token() -> str:
    """Generate a new JWT token and save it to a file."""
    payload = {"sub": "user", "iat": int(time.time())}
    token = jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)
    with open(TOKEN_FILE, "w") as file:
        file.write(token)
    return token

def get_saved_token() -> Optional[str]:
    """Retrieve the saved token from the file."""
    if os.path.exists(TOKEN_FILE):
        with open(TOKEN_FILE, "r") as file:
            return file.read().strip()
    return None

def verify_token(token: str):
    """Verify the given token against the saved one."""
    saved_token = get_saved_token()
    if not saved_token or token != saved_token:
        raise HTTPException(status_code=401, detail="Invalid or expired token")
    return True

def serialize_data(data):
    """Convert bytes to string if found in data."""
    if isinstance(data, bytes):
        return data.decode("utf-8", errors="replace")  # Decode bytes to string
    if isinstance(data, dict):
        return {k: serialize_data(v) for k, v in data.items()}  # Recursively process dicts
    if isinstance(data, list):
        return [serialize_data(v) for v in data]  # Recursively process lists
    return data

router = APIRouter()
DRIVER_ENV = os.environ.get('INTERNAL_DB_DRIVER')
SERVER_ENV = os.environ.get('INTERNAL_SERVER')
DATABASE_ENV = os.environ.get('INTERNAL_DATABASE')
DRIVER_ENV = os.environ.get('INTERNAL_UID')
DRIVER_ENV = os.environ.get('INTERNAL_PWDss')
conn_str = (
    r'DRIVER={SQL Server};'
    r'SERVER=172.29.23.180;'
    r'DATABASE=PEA_meter_data;'
    r'UID=postsavp;'
    r'PWD=Clean@100923;'
)