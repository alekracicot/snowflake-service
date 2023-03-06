import json
from dataclasses import dataclass


@dataclass
class SnowflakeSettings:
    user: str =  "<Enter info>"
    account: str = "<Enter info>"
    authenticator: str = "<Enter info>"
    warehouse: str = "<Enter info>"
    database: str = "<Enter info>"
    schema: str = "<Enter info>"
    role: str = "<Enter info>"

settings = SnowflakeSettings()
