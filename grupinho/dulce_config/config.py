from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    # Caminhos
    data_dir: Path = Path("data")

    # Chaves de API
    transparencia_api_key: str = ""
    transparencia_base_url: str = "https://api.portaldatransparencia.gov.br/api-de-dados"

    # Logging
    log_level: str = "INFO"

    # Caminhos derivados a partir do data_dir
    @property
    def bronze_dir(self) -> Path:
        return self.data_dir / "bronze"

    @property
    def silver_dir(self) -> Path:
        return self.data_dir / "silver"
    
    @property
    def silver_spark(self) -> Path:
        return self.data_dir / "silver_spark"

    @property
    def gold_dir(self) -> Path:
        return self.data_dir / "gold"

    @property
    def gold_spark(self) -> Path:
        return self.data_dir / "gold_spark"

def get_settings() -> Settings:
    return Settings()