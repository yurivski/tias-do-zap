from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path

@dataclass
class Alert:
    code: str
    description: str
    score: int
    details: dict

class BaseAnalyzer(ABC):
    @property
    @abstractmethod
    def name(self) -> str:
        ...

    @abstractmethod
    def analyze(self, gold_dir: Path, deputado_id: int = None) -> list[Alert]:
        ...

    def __repr__(self) -> str:
        # Representação legível pra logs e debug:
        return f"<Analyzer: {self.name}>"