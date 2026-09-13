from abc import ABC, abstractmethod
from typing import Dict


class BaseAuth(ABC):

    @abstractmethod
    def get_headers(self) -> Dict[str, str]:
        pass