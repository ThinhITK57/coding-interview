import os
from typing import Dict
from epm.monitoring.metrics import EndpointConfig


class TokenAuth:
    """Clarizen API Key Token Authenticator."""

    def __init__(self, endpoint_config: EndpointConfig):
        self.config = endpoint_config

    def get_token(self) -> str:
        token = os.getenv(self.config.auth_env_name, "")
        if not token:
            # Fallback to PROJECT_API_KEY
            token = os.getenv("PROJECT_API_KEY", "")
        return token

    def get_headers(self) -> Dict[str, str]:
        token = self.get_token()
        if not token:
            return {}

        prefix = self.config.auth_prefix.strip()
        auth_val = f"{prefix} {token}".strip() if prefix else token
        return {
            self.config.auth_header: auth_val
        }
