"""PGSync RedisQueue."""

import json
import logging
import sys
import typing as t

from redis import Redis
from redis.exceptions import ConnectionError

try:
    from ddtrace import tracer as _dd_tracer
except ImportError:
    _dd_tracer = None

from .settings import (
    REDIS_READ_CHUNK_SIZE,
    REDIS_RETRY_ON_TIMEOUT,
    REDIS_SOCKET_TIMEOUT,
)
from .urls import get_redis_url

logger = logging.getLogger(__name__)


class RedisQueue(object):
    """Simple Queue with Redis/Valkey Backend."""

    def __init__(self, name: str, namespace: str = "queue", **kwargs):
        """Init Simple Queue with Redis/Valkey Backend."""
        url: str = get_redis_url(**kwargs)
        self.key: str = f"{namespace}:{name}"
        self._meta_key: str = f"{self.key}:meta"
        try:
            self.__db: Redis = Redis.from_url(
                url,
                socket_timeout=REDIS_SOCKET_TIMEOUT,
                retry_on_timeout=REDIS_RETRY_ON_TIMEOUT,
            )
            self.__db.ping()
        except ConnectionError as e:
            logger.exception(f"Redis server is not running: {e}")
            raise

    @property
    def qsize(self) -> int:
        """Return the approximate size of the queue."""
        return self.__db.llen(self.key)

    def pop(self, chunk_size: t.Optional[int] = None) -> t.List[dict]:
        """Remove and return multiple items from the queue."""
        chunk_size = chunk_size or REDIS_READ_CHUNK_SIZE
        if self.qsize > 0:
            span = None
            if _dd_tracer:
                span = _dd_tracer.trace(
                    "pgsync.redis.pop", resource="pgsync.redis.pop"
                )
                span.set_tag("queue_key", self.key)
                span.__enter__()
            try:
                pipeline = self.__db.pipeline()
                pipeline.lrange(self.key, 0, chunk_size - 1)
                pipeline.ltrim(self.key, chunk_size, -1)
                items: t.List = pipeline.execute()
                logger.debug(f"pop size: {len(items[0])}")
                return list(map(lambda value: json.loads(value), items[0]))
            finally:
                if span:
                    span.__exit__(*sys.exc_info())
        return []

    def pop_visible_in_snapshot(
        self,
        pg_visible_in_snapshot: t.Callable[[t.List[int]], dict],
        chunk_size: t.Optional[int] = None,
    ) -> t.List[dict]:
        """
        Pop items in the queue that are visible in the current snapshot.
        Uses the provided pg_visible_in_snapshot function to determine visibility.
        This function is useful for read-only consumers that need to process items
        that are visible in the current PostgreSQL snapshot.
        """
        chunk_size = chunk_size or REDIS_READ_CHUNK_SIZE
        span = None
        if _dd_tracer:
            span = _dd_tracer.trace(
                "pgsync.redis.pop_visible",
                resource="pgsync.redis.pop_visible",
            )
            span.set_tag("queue_key", self.key)
            span.set_tag("chunk_size", chunk_size)
            span.__enter__()
        try:
            items: t.List = self.__db.lrange(self.key, 0, chunk_size - 1)
            if not items:
                if span:
                    span.set_tag("peeked_count", 0)
                    span.set_tag("visible_count", 0)
                    span.set_tag("lrem_count", 0)
                return []
            payloads = [json.loads(i) for i in items]

            if span:
                span.set_tag("peeked_count", len(items))

            # Check visibility against PG snapshot
            vis_span = None
            if _dd_tracer:
                vis_span = _dd_tracer.trace(
                    "pgsync.redis.pg_visible_check",
                    resource="pgsync.redis.pg_visible_check",
                )
                vis_span.set_tag("xmin_count", len(payloads))
                vis_span.__enter__()
            try:
                visible_map: dict = pg_visible_in_snapshot()(
                    [payload["xmin"] for payload in payloads]
                )
            finally:
                if vis_span:
                    vis_span.__exit__(*sys.exc_info())

            visible: t.List[dict] = []
            lrem_count = 0

            # lrem loop — O(N) per call, this is the known bottleneck
            lrem_span = None
            if _dd_tracer:
                lrem_span = _dd_tracer.trace(
                    "pgsync.redis.lrem_loop",
                    resource="pgsync.redis.lrem_loop",
                )
                lrem_span.set_tag("queue_key", self.key)
                lrem_span.__enter__()
            try:
                for item, payload in zip(items, payloads):
                    if visible_map.get(payload["xmin"]):
                        # Claim atomically
                        removed = self.__db.lrem(self.key, 1, item)
                        lrem_count += 1
                        if removed:
                            visible.append(payload)
            finally:
                if lrem_span:
                    lrem_span.set_tag("lrem_count", lrem_count)
                    lrem_span.set_tag("visible_count", len(visible))
                    lrem_span.__exit__(*sys.exc_info())

            if span:
                span.set_tag("visible_count", len(visible))
                span.set_tag("lrem_count", lrem_count)
            return visible
        finally:
            if span:
                span.__exit__(*sys.exc_info())

    def push(self, items: t.List) -> None:
        """Push multiple items onto the queue."""
        if not items:
            return
        span = None
        if _dd_tracer:
            span = _dd_tracer.trace(
                "pgsync.redis.push", resource="pgsync.redis.push"
            )
            span.set_tag("queue_key", self.key)
            span.set_tag("item_count", len(items))
            span.__enter__()
        try:
            self.__db.rpush(self.key, *map(json.dumps, items))
        finally:
            if span:
                span.__exit__(*sys.exc_info())

    def delete(self) -> None:
        """Delete all items from the named queue."""
        logger.info(f"Deleting redis key: {self.key}")
        self.__db.delete(self.key)
        logger.info(f"Deleted redis key: {self.key}")

    def set_meta(self, value: t.Any) -> None:
        """Store an arbitrary JSON-serialisable value in a dedicated key."""
        self.__db.set(self._meta_key, json.dumps(value))

    def get_meta(self, default: t.Any = None) -> t.Any:
        """Retrieve the stored value (or *default* if nothing is set)."""
        raw = self.__db.get(self._meta_key)
        return json.loads(raw) if raw is not None else default
