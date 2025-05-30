import psycopg2
import psycopg2.extras
import json
from typing import List, Dict, Any
from datetime import datetime
from utils.logger import get_logger
from etl.state import State


class PostgresExtractor:
    def __init__(self, dsn: str, state: State, batch_size: int = 100):
        self.dsn = dsn
        self.state = state
        self.batch_size = batch_size
        self.logger = get_logger(__name__)

    def connect(self):
        conn = psycopg2.connect(self.dsn, cursor_factory=psycopg2.extras.DictCursor)
        psycopg2.extras.register_default_jsonb(conn, loads=json.loads)
        return conn

    def extract_modified_filmworks(self) -> List[Dict[str, Any]]:
        last_modified = self.state.get_state("last_modified") or "1970-01-01T00:00:00"

        query = """
            SELECT 
                fw.id, 
                fw.title, 
                fw.description, 
                fw.rating AS imdb_rating, 
                fw.modified,
                COALESCE(
                    array_agg(DISTINCT g.name) FILTER (WHERE g.name IS NOT NULL),
                    ARRAY[]::varchar[]
                ) AS genres,  
                COALESCE(
                    json_agg(DISTINCT jsonb_build_object('id', p.id, 'name', p.full_name)) 
                    FILTER (WHERE pfw.role = 'actor' AND p.id IS NOT NULL),
                    '[]'::json
                ) AS actors,
                COALESCE(
                    json_agg(DISTINCT jsonb_build_object('id', p.id, 'name', p.full_name)) 
                    FILTER (WHERE pfw.role = 'writer' AND p.id IS NOT NULL),
                    '[]'::json
                ) AS writers,
                COALESCE(
                    json_agg(DISTINCT jsonb_build_object('id', p.id, 'name', p.full_name)) 
                    FILTER (WHERE pfw.role = 'director' AND p.id IS NOT NULL),
                    '[]'::json
                ) AS directors
            FROM content.film_work fw
            LEFT JOIN content.genre_film_work gfw ON fw.id = gfw.film_work_id
            LEFT JOIN content.genre g ON g.id = gfw.genre_id
            LEFT JOIN content.person_film_work pfw ON fw.id = pfw.film_work_id
            LEFT JOIN content.person p ON p.id = pfw.person_id
            WHERE fw.modified > %s
            GROUP BY fw.id
            ORDER BY fw.modified
            LIMIT %s;
        """

        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (last_modified, self.batch_size))
                records = cur.fetchall()
                self.logger.info(f"Extracted {len(records)} records from PostgreSQL")

                converted_records = []
                for row in records:
                    row_dict = dict(row)

                    # Преобразование данных
                    if "modified" in row_dict and isinstance(row_dict["modified"], datetime):
                        row_dict["modified"] = row_dict["modified"].isoformat()

                    # Гарантируем, что поля существуют даже если NULL
                    row_dict.setdefault("genres", [])
                    row_dict.setdefault("actors", [])
                    row_dict.setdefault("writers", [])
                    row_dict.setdefault("directors", [])

                    converted_records.append(row_dict)

                if converted_records:
                    self.logger.debug(f"Sample extracted record: {converted_records[0]}")

                return converted_records