from typing import Dict, Any, List


class Transformer:
    @staticmethod
    def transform_filmwork(raw_filmwork: Dict[str, Any]) -> Dict[str, Any]:
        def safe_name_list(field) -> List[str]:
            items = raw_filmwork.get(field) or []
            if isinstance(items, list) and all(isinstance(i, dict) and "name" in i for i in items):
                return [i["name"] for i in items]
            return []

        transformed = {
            "id": raw_filmwork["id"],
            "imdb_rating": raw_filmwork.get("imdb_rating"),
            "genre": raw_filmwork.get("genre") or [],
            "genres": [
                {"id": "", "name": g} for g in raw_filmwork.get("genre") or []
            ],
            "title": raw_filmwork.get("title"),
            "description": raw_filmwork.get("description"),
            "actors": raw_filmwork.get("actors") if isinstance(raw_filmwork.get("actors"), list) else [],
            "writers": raw_filmwork.get("writers") if isinstance(raw_filmwork.get("writers"), list) else [],
            "directors": raw_filmwork.get("directors") if isinstance(raw_filmwork.get("directors"), list) else [],
            "actors_names": safe_name_list("actors"),
            "writers_names": safe_name_list("writers"),
            "directors_names": safe_name_list("directors"),
        }
        return transformed

