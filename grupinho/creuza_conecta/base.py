from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

class BaseConnector(ABC):
    @property
    @abstractmethod
    def name(self) -> str:
        ...

    @property
    @abstractmethod
    def source_url(self) -> str:
        ...

    @abstractmethod
    async def fetch(self, **params: Any) -> dict[str, Any]:
        ...

    @abstractmethod
    def validate(self, data: dict[str, Any]) -> bool:
        ...

    @abstractmethod
    def save_bronze(self, data: dict[str, Any], output_dir: Path) -> Path:
        ...

    def __repr__(self) -> str:
        # Representação legível para logs e debug
        return f"<Connector: {self.name}>"