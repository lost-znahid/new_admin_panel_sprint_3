import logging
import sys


def get_logger(name):
    """Настройка и возврат логгера"""
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger


# Создаем глобальный логгер для модуля
logger = get_logger(__name__)