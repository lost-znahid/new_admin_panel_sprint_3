from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import datetime
import json


class ElasticsearchLoader:
    def __init__(self, host="http://elasticsearch:9200"):
        self.es = Elasticsearch(host, basic_auth=("elastic", "password"))

    def create_index(self, index_name: str, mapping_path: str):
        try:
            if self.es.indices.exists(index=index_name):
                self.es.indices.delete(index=index_name)

            with open(mapping_path, "r", encoding="utf-8") as f:
                mapping = json.load(f)
            self.es.indices.create(index=index_name, body=mapping)

            print(f"Index '{index_name}' created.")
        except Exception as e:
            print(f"Error creating index: {e}")

    @staticmethod
    def deep_convert(obj):
        if isinstance(obj, dict):
            return {k: ElasticsearchLoader.deep_convert(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [ElasticsearchLoader.deep_convert(v) for v in obj]
        elif isinstance(obj, (datetime.datetime, datetime.date)):
            return obj.isoformat()
        else:
            return obj

    def prepare_records(self, db_rows):
        records = []
        for row in db_rows:
            if isinstance(row, str):
                row = json.loads(row)

            # Конвертируем даты во всём входном словаре
            row = ElasticsearchLoader.deep_convert(row)

            record = {
                "id": str(row["id"]),
                "title": row["title"],
                "description": row.get("description"),
                "creation_date": row.get("creation_date"),
                "rating": row.get("imdb_rating"),
                "type": row.get("type", "movie"),
                "genres": row.get("genre", []),
                "actors": row.get("actors") or [],
                "directors": row.get("directors") or [],
                "writers": row.get("writers") or [],
                "created": row.get("created"),
                "modified": row.get("modified"),
            }

            record = ElasticsearchLoader.deep_convert(record)
            records.append(record)

        # Проверка: нет ли datetime в итоговых записях
        def check_no_datetime(obj, path="root"):
            if isinstance(obj, dict):
                for k, v in obj.items():
                    check_no_datetime(v, f"{path}.{k}")
            elif isinstance(obj, list):
                for i, v in enumerate(obj):
                    check_no_datetime(v, f"{path}[{i}]")
            elif isinstance(obj, (datetime.datetime, datetime.date)):
                raise TypeError(f"❗ Found datetime at {path}: {obj}")

        for rec in records:
            check_no_datetime(rec)

        return records

    def load(self, records: list, index_name: str = "movies") -> bool:
        actions = [
            {
                "_index": index_name,
                "_id": record["id"],
                "_source": record,
            }
            for record in records
        ]
        try:
            def check_no_datetime(obj, path="root"):
                if isinstance(obj, dict):
                    for k, v in obj.items():
                        check_no_datetime(v, f"{path}.{k}")
                elif isinstance(obj, list):
                    for i, v in enumerate(obj):
                        check_no_datetime(v, f"{path}[{i}]")
                elif isinstance(obj, (datetime.datetime, datetime.date)):
                    raise TypeError(f"❗ Found datetime at {path}: {obj}")

            success, _ = bulk(self.es, actions)
            print(f"Загружено {success} записей в индекс '{index_name}'")
            return True
        except Exception as e:
            print(f"Ошибка при загрузке данных: {e}")
            return False
