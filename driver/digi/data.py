import os
import sys
import json
import threading
from abc import ABC, abstractmethod
from typing import List

import zed
import digi
from digi import logger, util

lake_url = "http://lake:6534"
os.environ["ZED_LAKE"] = lake_url


class Pool(ABC):
    @abstractmethod
    def __init__(self, name: str):
        self.name = name
        self.lock = threading.Lock()

    @abstractmethod
    def load(self, objects: List[dict]):
        raise NotImplementedError

    @abstractmethod
    def query(self, query: str):
        raise NotImplementedError


class ZedPool(Pool):
    def __init__(self, name):
        super().__init__(name)
        self.client = zed.Client(base_url=lake_url)

    def load(self, objects: List[dict]):
        ts = util.get_ts()
        for o in objects:
            if "ts" in o:
                o["event_ts"] = o["ts"]
            o["ts"] = ts
        data = "".join(json.dumps(o) for o in objects)

        self.lock.acquire()
        try:
            self.client.load(self.name, data)
        finally:
            self.lock.release()

    def query(self, query):
        return self.client.query(query)


def pool_name(g, v, r, n, ns):
    _, _, _ = g, v, r
    if ns == "default":
        return f"{n}"
    else:
        return f"{ns}-{n}"


providers = {
    "zed": ZedPool
    # ...
}


def create_pool():
    global providers
    if digi.pool_provider == "":
        digi.pool_provider = "zed"
    if digi.pool_provider in {"none", "false"}:
        return None
    if digi.pool_provider not in providers:
        logger.fatal(f"unknown pool provider {digi.pool_provider}")
        sys.exit(1)
    return providers[digi.pool_provider](
        pool_name(*digi.auri)
    )


pool = None
