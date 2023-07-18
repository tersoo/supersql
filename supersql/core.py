from typing import List

from .helpers import Connexer
from .query import Query


class Supersql(object):
    def __init__(self, dsn: str):
        self._dsn = dsn
        self._vendor = None
        self._connexion = None
        self._pooled = False

    async def connect(self, pooled = False):
        self._pooled = pooled
        vendor, connector = await Connexer(self._dsn, self._pooled)
        self._connexion = connector
        self._vendor = vendor

    async def disconnect(self):
        assert self._connexion
        connexion = self._connexion
        await connexion.close()
        self._connexion = None

    def connects(self):
        """connect synchronous"""
        raise NotImplementedError

    def disconnects(self):
        """disconnect synchronous"""
        raise NotImplementedError

    @property
    def vendor(self) -> str:
        return self._vendor

    @vendor.setter
    def vendor(self, value: str):
        raise ValueError('To set Supersql vendor kindly provide a DSN at initialization')

    @property
    def query(self) -> Query:
        """Initializes Query and proxies Query.SELECT method directly"""
        return Query(self)
