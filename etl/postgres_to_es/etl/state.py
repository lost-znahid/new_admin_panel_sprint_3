import json
from typing import Any, Dict
from abc import ABC, abstractmethod
import redis


class BaseStorage(ABC):
    @abstractmethod
    def save_state(self, state: Dict[str, Any]) -> None:
        pass

    @abstractmethod
    def retrieve_state(self) -> Dict[str, Any]:
        pass


class RedisStorage(BaseStorage):
    def __init__(self, redis_client: redis.Redis, key: str = "etl_state"):
        self._redis = redis_client
        self._key = key

    def save_state(self, state: Dict[str, Any]) -> None:
        data = json.dumps(state)
        self._redis.set(self._key, data)

    def retrieve_state(self) -> Dict[str, Any]:
        data = self._redis.get(self._key)
        if not data:
            return {}
        return json.loads(data)


class State:
    def __init__(self, storage: BaseStorage) -> None:
        self._storage = storage
        self._state = self._storage.retrieve_state()

    def set_state(self, key: str, value: Any) -> None:
        self._state[key] = value
        self._storage.save_state(self._state)

    def get_state(self, key: str) -> Any:
        return self._state.get(key)
