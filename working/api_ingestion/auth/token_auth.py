import os

from typing import Dict

from config.api_config import AuthConfig
from .base_auth import BaseAuth


class TokenAuth(BaseAuth):

    def __init__(self, config: AuthConfig):
        self.config = config

    def get_headers(self) -> Dict[str, str]:

        credentials = self.config.credentials

        header_name = credentials.get(
            "header_name",
            "Authorization"
        )

        env_name = credentials.get(
            "env_name"
        )

        prefix = credentials.get(
            "prefix",
            ""
        )

        if not env_name:
            raise ValueError(
                "Authentication env_name is required"
            )

        token = os.getenv(env_name)

        if not token:
            raise ValueError(
                f"Environment variable "
                f"'{env_name}' is not defined"
            )

        if prefix:
            value = f"{prefix} {token}"
        else:
            value = token

        return {
            header_name: value
        }