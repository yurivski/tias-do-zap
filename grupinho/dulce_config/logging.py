import logging
import structlog
from pathlib import Path
from datetime import datetime, timezone

def setup_logging(log_level: str = "INFO") -> None:
    # Converte a string do nível para a constante do logging
    numeric_level = getattr(logging, log_level.upper(), logging.INFO)

    # Configuração do logging padrão do Python
    logging.basicConfig(
        format="%(message)s",
        level=numeric_level,
    )

    # Configuração do structlog
    structlog.configure(
        processors=[
            # Adiciona o nível do log, ex: info, debug, error...
            structlog.stdlib.add_log_level,
            # Adiciona timestamp em ISO 8601
            structlog.processors.TimeStamper(fmt="iso"),
            # Formata exceções com tarceback completo
            structlog.processors.format_exc_info,
            # Renderiza a saída como JSON legível no terminal
            structlog.dev.ConsoleRenderer(),
        ],
        # Usa o logger padrão do Python por baixo
        wrapper_class=structlog.stdlib.BoundLogger,
        # Cria uma nova instância a cada chamada (thread-safe)
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

def setup_file_logging(command_name: str, log_dir: str = "vera_logs") -> Path:
    """Redireciona logs para arquivo com timestamp."""

    log_path = Path(log_dir)
    log_path.mkdir(parents=True, exist_ok=True)

    now = datetime.now(timezone.utc)
    filename = f"{command_name}_{now.strftime('%Y-%m-%d_%H%M%S')}.log"
    filepath = log_path / filename

    # Configura o file handler
    file_handler = logging.FileHandler(filepath, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter("%(message)s"))

    root_logger = logging.getLogger()
    # Remove handlers de console
    for handler in root_logger.handlers[:]:
        if isinstance(handler, logging.StreamHandler):
            root_logger.removeHandler(handler)
    root_logger.addHandler(file_handler)
    root_logger.setLevel(logging.DEBUG)

    # Reconfigura structlog para usar o logging (que agora vai pro arquivo)
    structlog.configure(
        processors=[
            structlog.stdlib.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.format_exc_info,
            structlog.dev.ConsoleRenderer(colors=False),
        ],
        wrapper_class=structlog.stdlib.BoundLogger,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=False,
    )

    return filepath