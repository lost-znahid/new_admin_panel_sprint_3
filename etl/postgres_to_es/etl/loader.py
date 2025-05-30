import json
import os
from datetime import datetime, date
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from utils.logger import get_logger


class ElasticsearchLoader:
    def __init__(self, host="http://elasticsearch:9200"):
        self.logger = get_logger(__name__)
        try:
            self.es = Elasticsearch(
                host,
                basic_auth=("elastic", "password"),
                request_timeout=30
            )
            if not self.es.ping():
                raise ConnectionError("Не удалось подключиться к Elasticsearch")
            self.logger.info("Успешное подключение к Elasticsearch")
        except Exception as e:
            self.logger.error(f"Ошибка подключения к Elasticsearch: {e}")
            raise

    @staticmethod
    def deep_convert(obj):
        """Рекурсивно преобразует объекты для сериализации"""
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        elif isinstance(obj, dict):
            return {k: ElasticsearchLoader.deep_convert(v) for k, v in obj.items()}
        elif isinstance(obj, (list, tuple)):
            return [ElasticsearchLoader.deep_convert(v) for v in obj]
        return obj

    def clean_field(self, value):
        """Очистка полей от N/A и пустых значений"""
        if isinstance(value, str):
            if value.strip().upper() in ['N/A', '']:
                return None
            return value.strip()
        return value

    def validate_record(self, record):
        """Валидация структуры записи"""
        required_fields = ['id', 'title']
        for field in required_fields:
            if field not in record:
                raise ValueError(f"Отсутствует обязательное поле {field}")
        return True

    def prepare_records(self, db_rows: list[dict]) -> list[dict]:
        """Преобразует данные из Postgres в формат для Elasticsearch."""
        records = []

        for row in db_rows:
            try:
                film_id = str(row.get("id", "")).strip()
                title = self.clean_field(row.get("title", ""))

                if not film_id or not title:
                    raise ValueError("Пропущен обязательный ID или title")

                # Список жанров как список keyword-строк
                genres = [
                    self.clean_field(g)
                    for g in row.get("genres", [])
                    if isinstance(g, str) and self.clean_field(g)
                ]

                # Подготовка персоналий
                actors = self._prepare_people(row.get("actors", []))
                writers = self._prepare_people(row.get("writers", []))
                directors = self._prepare_people(row.get("directors", []))

                record = {
                    "id": film_id,
                    "title": title,
                    "description": self.clean_field(row.get("description", "")) or "",
                    "imdb_rating": (
                        float(row["imdb_rating"]) if row.get("imdb_rating") is not None else None
                    ),
                    "genres": genres,
                    "actors": actors,
                    "writers": writers,
                    "directors": directors,
                    "actors_names": [p["name"] for p in actors],
                    "writers_names": [p["name"] for p in writers],
                    "directors_names": [p["name"] for p in directors],
                }

                # Глубокая очистка от datetime и др.
                record = self.deep_convert(record)

                records.append(record)

            except Exception as e:
                self.logger.error(f"Ошибка подготовки записи {row.get('id')}: {str(e)}")
                continue

        return records

    def _prepare_people(self, people: list[dict]) -> list[dict]:
        """Вспомогательный метод для подготовки информации о людях (актерах, режиссерах и т.д.)"""
        return [{"id": str(p["id"]), "name": str(p["name"])} for p in people if p and p.get("name")]

    def create_index(self, index_name="movies", schema_path="movies_index.json"):
        """Создание индекса с заданной схемой"""
        try:
            if self.es.indices.exists(index=index_name):
                self.logger.info(f"Удаление существующего индекса {index_name}")
                self.es.indices.delete(index=index_name)

            with open(schema_path, "r", encoding="utf-8") as f:
                schema = json.load(f)

            self.es.indices.create(index=index_name, body=schema)
            self.logger.info(f"Создан индекс '{index_name}' из схемы '{schema_path}'")
        except Exception as e:
            self.logger.error(f"Ошибка при создании индекса: {str(e)}")
            raise

    def load(self, records: list, index_name: str = "movies") -> bool:
        actions = []
        for record in records:
            try:
                # Проверка сериализуемости
                json_record = json.dumps(record)
                actions.append({
                    "_index": index_name,
                    "_id": record["id"],
                    "_source": json.loads(json_record)  # Двойная проверка
                })
            except Exception as e:
                print(f"Ошибка подготовки документа {record.get('id')}: {str(e)}")
                continue

        try:
            success, errors = bulk(
                self.es,
                actions,
                stats_only=False,  # Получаем детали ошибок
                raise_on_error=False
            )

            if errors:
                print("\nДетали ошибок индексации:")
                for error in errors:
                    print(f"ID: {error['index']['_id']}, Ошибка: {error['index']['error']}")

            self.es.indices.refresh(index=index_name)
            return len(errors) == 0

        except Exception as e:
            print(f"Фатальная ошибка bulk-запроса: {str(e)}")
            return False