import logging
from typing import Text


class Settings:
    # Environments
    logger_name: Text = "stream-valve"


settings = Settings()
logger = logging.getLogger(settings.logger_name)
