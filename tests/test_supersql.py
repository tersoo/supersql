from os import path, remove
from unittest import TestCase, IsolatedAsyncioTestCase

# installed
from aiosqlite import Connection as LiteConnection

from asyncpg import Connection, Pool
from asyncpg.exceptions import InvalidAuthorizationSpecificationError
from pytest import mark

# src code
from supersql import Supersql, Query
from supersql.constants import POSTGRES_LIVE_DSN


pytest_plugins = ('pytest_asyncio',)


TESTING_DSN = 'testing://development'


class TestSupersql(TestCase):
    def test_supersql_engine_select(self):
        ssql = Supersql('testing://dsn')
    
    def test_supersql_synchronous_connect(self):
        ssql = Supersql(TESTING_DSN)
        self.assertRaises(NotImplementedError, ssql.connects)
    
    def test_supersql_synchronous_disconnect(self):
        ssql = Supersql(TESTING_DSN)
        self.assertRaises(NotImplementedError, ssql.disconnects)
    
    def test_supersql_select(self):
        ssql = Supersql(TESTING_DSN)
        self.assertIsInstance(ssql.query, Query)
    
    def test_vendor_unsettable(self):
        ssql = Supersql(TESTING_DSN)
        with self.assertRaises(ValueError):
            ssql.vendor = 'oracle'


@mark.asyncio
class TestSupersqlAsync(IsolatedAsyncioTestCase):
    async def test_supersql_pg_connection_error(self):
        wrong_dsn = 'postgres://username:password@localhost:5432/database'
        ssql = Supersql(wrong_dsn)
        with self.assertRaises(InvalidAuthorizationSpecificationError):
            await ssql.connect()
    
    async def test_supersql_pg_connection(self):
        ssql = Supersql(POSTGRES_LIVE_DSN)
        await ssql.connect()
        self.assertIsInstance(ssql._connexion, Connection)
        await ssql.disconnect()
        self.assertIsNone(ssql._connexion)

    async def test_supersql_sqlite_connects(self):
        dbname = 'supersql.db'
        ssql = Supersql(f'sqlite://{dbname}')
        await ssql.connect()
        self.assertIsInstance(ssql._connexion, LiteConnection)
        await ssql.disconnect()
        self.assertIsNone(ssql._connexion)

        # check somewhere.db file exists and delete it afterwards
        db = path.isfile(dbname)
        self.assertTrue(db)
        if db: remove(dbname)
    
    async def test_supersql_pg_connection_pool(self):
        ssql = Supersql(POSTGRES_LIVE_DSN)
        await ssql.connect(pooled=True)
        self.assertIsInstance(ssql._connexion, Pool)
        await ssql.disconnect()
        self.assertIsNone(ssql._connexion)
