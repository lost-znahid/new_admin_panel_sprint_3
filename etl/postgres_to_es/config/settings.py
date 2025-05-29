from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    pg_user: str
    pg_password: str
    pg_host: str
    pg_port: int
    pg_db: str

    pg_dsn: str | None = None

    redis_host: str
    redis_port: int
    redis_db: int

    es_host: str
    es_index: str

    batch_size: int
    poll_interval: int
    backoff_on_error: int

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    def __init__(self, **values):
        super().__init__(**values)
        # Формируем DSN, если не передан
        if not self.pg_dsn:
            self.pg_dsn = (
                f"postgresql://{self.pg_user}:{self.pg_password}@"
                f"{self.pg_host}:{self.pg_port}/{self.pg_db}"
            )
