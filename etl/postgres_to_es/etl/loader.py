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

    def prepare_records(self, db_rows):
        """Подготовка записей для загрузки в Elasticsearch"""
        records = []
        for row in db_rows:
            try:
                # Очистка полей
                row = {k: self.clean_field(v) if isinstance(v, str) else v
                       for k, v in row.items()}

                # Подготовка вложенных структур
                actors = [a for a in (row.get("actors") or []) if a and a.get("name")]
                writers = [w for w in (row.get("writers") or []) if w and w.get("name")]
                directors = [d for d in (row.get("directors") or []) if d and d.get("name")]

                record = {
                    "id": str(row["id"]),
                    "title": self.clean_field(row["title"]),
                    "description": self.clean_field(row.get("description")),
                    "imdb_rating": float(row["imdb_rating"]) if row.get("imdb_rating") else None,
                    "genres": [self.clean_field(g) for g in (row.get("genre") or []) if g],
                    "actors": actors,
                    "writers": writers,
                    "directors": directors,
                    "actors_names": [self.clean_field(a["name"]) for a in actors if a.get("name")],
                    "writers_names": [self.clean_field(w["name"]) for w in writers if w.get("name")],
                    "directors_names": [self.clean_field(d["name"]) for d in directors if d.get("name")],
                }

                self.validate_record(record)
                records.append(record)
            except Exception as e:
                self.logger.warning(f"Пропущена запись {row.get('id')}: {str(e)}")
                continue

        return records

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
        """Пакетная загрузка данных в Elasticsearch"""
        actions = []
        for record in records:
            try:
                # Проверка сериализуемости записи
                json.dumps(record)
                actions.append({
                    "_index": index_name,
                    "_id": record["id"],
                    "_source": self.deep_convert(record),
                })
            except (TypeError, ValueError) as e:
                self.logger.warning(f"Пропущена запись {record.get('id')}: {str(e)}")
                continue

        try:
            success, errors = bulk(self.es, actions, stats_only=True)
            self.es.indices.refresh(index=index_name)
            self.logger.info(f"Успешно загружено {success} записей")
            if errors:
                self.logger.warning(f"Ошибки при загрузке {errors} записей")
            return True
        except Exception as e:
            self.logger.error(f"Ошибка bulk-запроса: {str(e)}")
            return False