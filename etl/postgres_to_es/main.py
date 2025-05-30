import time
import logging
import backoff
import redis

from elasticsearch import Elasticsearch

from etl.extractor import PostgresExtractor
from etl.transformer import Transformer
from etl.loader import ElasticsearchLoader
from etl.state import State, RedisStorage
from config.settings import Settings
from utils.logger import get_logger


logger = get_logger(__name__)


def backoff_hdlr(details):
    logger.warning(
        f"Backing off {details['wait']:0.1f}s after {details['tries']} tries "
        f"calling {details['target'].__name__} with args {details['args']}."
    )


def main():
    settings = Settings()
    logger.info(f"Загружены настройки: {settings.model_dump()}")

    # Подключение к Redis для хранения состояния
    redis_conn = redis.Redis(
        host=settings.redis_host,
        port=settings.redis_port,
        db=settings.redis_db
    )
    state = State(RedisStorage(redis_conn))

    # Инициализация Elasticsearch loader
    loader = ElasticsearchLoader()

    # Пересоздание индекса, если указано в настройках
    if settings.recreate_index:
        loader.create_index(index_name=settings.es_index)

    # Инициализация остальных компонентов
    extractor = PostgresExtractor(dsn=settings.pg_dsn, state=state, batch_size=settings.batch_size)
    transformer = Transformer()

    @backoff.on_exception(backoff.expo, Exception, max_tries=5, on_backoff=backoff_hdlr)
    def run_etl_batch():
        raw_data = extractor.extract_modified_filmworks()
        if not raw_data:
            logger.info("Нет новых данных для обработки, уходим в сон...")
            return False

        logger.info(f"Обнаружены новые записи, пример: {raw_data[0]}")
        transformed_batch = [transformer.transform_filmwork(film) for film in raw_data]

        success = loader.load(transformed_batch, index_name=settings.es_index)
        if not success:
            raise Exception("Не удалось загрузить пакет данных в Elasticsearch")

        # Обновление состояния
        max_modified = max(film["modified"] for film in raw_data)
        state.set_state("last_modified", max_modified)
        logger.info(f"Обновлено состояние last_modified: {max_modified}")
        return True

    # Основной цикл
    while True:
        try:
            processed = run_etl_batch()
            if not processed:
                time.sleep(settings.poll_interval)
        except Exception as e:
            logger.error(f"Ошибка при выполнении ETL-пакета: {e}")
            time.sleep(settings.backoff_on_error)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
