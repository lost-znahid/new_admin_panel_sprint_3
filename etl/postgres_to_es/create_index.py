from elasticsearch import Elasticsearch
import json

# Создаём клиент с базовыми настройками
es = Elasticsearch("http://localhost:9200")

# Создаём объект с нужными заголовками через .options()
es_v8 = es.options(headers={
    "Accept": "application/vnd.elasticsearch+json; compatible-with=8",
    "Content-Type": "application/vnd.elasticsearch+json; compatible-with=8"
})

index_name = "movies"

with open("movies_index.json", "r", encoding="utf-8") as f:
    mapping = json.load(f)

# Удалим индекс, если уже существует
if es_v8.indices.exists(index=index_name):
    es_v8.indices.delete(index=index_name)

# Создадим новый индекс
es_v8.indices.create(index=index_name, body=mapping)
print(f"Index '{index_name}' created.")
