from abc import ABC, abstractmethod

from typing import Any, Dict, Optional


class BaseClient(ABC):

    @abstractmethod
    def request(
        self,
        method: str,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Dict[str, Any]] = None,
        timeout: int = 30,
        ssl_context=None,
    ):
        pass