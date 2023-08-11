import copy
import logging
import os
from decimal import getcontext
from typing import Text


class Settings:
    def __init__(self, *args, **kwargs):
        envs = copy.deepcopy(dict(os.environ))
        for k, v in envs.items():
            k_casefold = k.casefold()
            envs[k_casefold] = v
            envs[k_casefold.replace("-", "_")] = v

        # Environments
        self.logger_name: Text = str(envs.get("logger_name", "stream-valve"))
        self.decimal_prec: int = int(envs.get("decimal_prec", "decimal_prec"))


settings = Settings()
logger = logging.getLogger(settings.logger_name)
getcontext().prec = settings.decimal_prec
