import time
import logging
from elasticsearch import Elasticsearch
import psycopg2
import backoff

from etl.extractor import PostgresExtractor
from etl.transformer import Transformer
from etl.loader import ElasticsearchLoader
from etl.state import State, RedisStorage
from config.settings import Settings
from utils.logger import get_logger
import redis

logger = get_logger(__name__)

def backoff_hdlr(details):
    logger.warning(f"Backing off {details['wait']:0.1f}s after {details['tries']} tries "
                   f"calling {details['target'].__name__} with args {details['args']}.")


def main():
    settings = Settings()

    # Подключение к Redis для состояния
    redis_conn = redis.Redis(host=settings.redis_host, port=settings.redis_port, db=0)
    state_storage = RedisStorage(redis_conn)
    state = State(state_storage)

    # Подключение к ES
    es_client = Elasticsearch(hosts=[settings.es_host])

    # Инициализация компонентов ETL
    extractor = PostgresExtractor(dsn=settings.pg_dsn, state=state, batch_size=settings.batch_size)
    loader = ElasticsearchLoader()

    transformer = Transformer()

    @backoff.on_exception(backoff.expo, Exception, max_tries=5, on_backoff=backoff_hdlr)
    def run_etl_batch():
        # Извлечь пачку
        raw_data = extractor.extract_modified_filmworks()
        if not raw_data:
            logger.info("No new data to process, sleeping...")
            return False

        logger.info(f"Sample raw film data:\n{raw_data[0]}")

        # Обработка всей пачки
        transformed_batch = [transformer.transform_filmwork(film) for film in raw_data]

        # Загрузка всей пачки сразу
        success = loader.load(transformed_batch)
        if not success:
            raise Exception("Failed to load batch")

        # Обновить состояние: максимальное время modified из пачки
        max_modified = max(film["modified"] for film in raw_data)
        state.set_state("last_modified", max_modified)
        logger.info(f"Updated state last_modified: {max_modified}")
        return True

    while True:
        try:
            processed = run_etl_batch()
            if not processed:
                time.sleep(settings.poll_interval)
        except Exception as e:
            logger.error(f"ETL batch failed: {e}")
            time.sleep(settings.backoff_on_error)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
