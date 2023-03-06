import json
from dataclasses import dataclass


@dataclass
class SnowflakeSettings:
    user: str
    account: str
    authenticator: str
    warehouse: str
    database: str
    schema: str
    role: str

def get_snowflake_settings() -> SnowflakeSettings:
    with open('config/credentials.json') as f:
        settings = json.load(f)
    return SnowflakeSettings(**settings)

settings = get_snowflake_settings()
