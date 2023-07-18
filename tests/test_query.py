from dataclasses import dataclass
from unittest import IsolatedAsyncioTestCase, TestCase

from pydantic import BaseModel
from pytest import mark

from supersql import Query, Rows, Supersql, Table, query
from supersql.constants import POSTGRES, POSTGRES_LIVE_DSN, SQLITE_LIVE_DSN

SELECT_STATEMENT = 'SELECT first_name FROM students WHERE identifier = ?'
TESTING_DSN = 'testing://development'


class TestQuery(TestCase):
    def setUp(self) -> None:
        self.ssql = Supersql(TESTING_DSN)
        self.table = lambda t: Table(t)

    def tearDown(self) -> None:
        return super().tearDown()
    
    def test_case_when_string(self):
        query = self.ssql.query
        tab = self.table('tab')
        CASE = query.utils('CASE')
        query = query.SELECT(
            tab.username,
            CASE
                .WHEN(tab.age > 18).THEN('can_vote').ELSE('no_vote')
            .END
            .AS('age_group')
        ).FROM(tab)
        self.assertEqual(query.sql(), "SELECT username, CASE WHEN age > 18 THEN 'can_vote' ELSE 'no_vote' END AS age_group FROM tab")
    
    def test_case_when(self):
        query = self.ssql.query
        tab = self.table('tab')
        CASE = query.utils('CASE')
        query = query.SELECT(
            tab.username,
            CASE
                .WHEN(tab.age).THEN(tab.can_vote).ELSE('no_vote')
            .END
            .AS('age_group')
        ).FROM(tab)
        self.assertEqual(query.sql(), "SELECT username, CASE WHEN age THEN can_vote ELSE 'no_vote' END AS age_group FROM tab")
    
    def test_case_when_pg(self):
        query = self.ssql.query
        tab = self.table('tab')
        query._engine._vendor = POSTGRES
        CASE = query.utils('CASE')
        query = query.SELECT(
            tab.username,
            CASE
                .WHEN(tab.age > 18).THEN(tab.age * 10).ELSE(tab.age)
            .END
            .AS('age_group')
        ).FROM(tab)
        self.assertEqual(query.sql(), "SELECT username, CASE WHEN age > 18 THEN age * 10 ELSE age END AS age_group FROM tab")

    def test_clone_query(self):
        query = self.ssql.query
        clone = query._clone()
        self.assertIsInstance(clone, Query)
        self.assertNotEqual(clone, query)
        self.assertNotEqual(self.ssql.query, self.ssql.query)
    
    def test_create_table(self):
        query = self.ssql.query
        customer = self.table('customer')
        self.assertEqual(query.CREATE(customer).sql(), 'CREATE TABLE customers')

    def test_select_statement_with_str(self):
        query = self.ssql.query.SELECT('first_name').FROM('students').WHERE('identifier = ?', 5)
        self.assertEqual(SELECT_STATEMENT, str(query))

    def test_select_statement_with_constant(self):
        query = self.ssql.query.SELECT(5)
        self.assertEqual(str(query), 'SELECT 5')

    def test_select_statement_with_table_fields(self):
        foo = Table('foo')
        bar = Table('bar')
        expected = 'SELECT name, email, identifier'
        query = self.ssql.query.SELECT(foo.name, foo.email, bar.identifier)
        self.assertEqual(expected, str(query))

    def test_select_from(self):
        foo = Table('foo')
        bar = Table('bar')
        ssql = Supersql(TESTING_DSN)
        expected = 'SELECT f.id, f.email, f.gender, b.role FROM foo AS f, bar AS b'
        query = ssql.query.SELECT(foo.id, foo.email, foo.gender, bar.role).FROM(foo.AS('f'), bar.AS('b'))
        self.assertEqual(expected, str(query))

    def select_from_where(self):
        tab = Table('tab')
        ssql = Supersql(TESTING_DSN)
        query = ssql.query.SELECT().FROM(tab)
        self.assertEqual(str(query), 'SELECT * FROM tab')
        sql = 'SELECT * FROM tab WHERE username = ?'
        self.assertEqual(str(query.WHERE(tab.username == 4)), sql)
        return sql, query, tab

    def test_where(self):
        self.select_from_where()

    def test_where_and(self):
        tab = Table('tab')
        ssql = Supersql(TESTING_DSN)
        sql = 'SELECT * FROM tab WHERE username = ? AND password = ?'
        query = ssql.query.SELECT().FROM(
            tab
        ).WHERE(
            tab.username == 5
        ).AND(
            tab.password == 'password')
        self.assertEqual(query.sql(), sql)

    def test_where_or(self):
        tab = Table('tab')
        ssql = Supersql(TESTING_DSN)
        sql = 'SELECT * FROM tab WHERE username = ? OR password = ?'
        query = ssql.query.SELECT().FROM(
            tab
        ).WHERE(
            tab.username == 5
        ).OR(
            tab.password == 'password')
        self.assertEqual(query.sql(), sql)

    def test_where_or_pg(self):
        tab = self.table('tab')
        ssql = Supersql(TESTING_DSN)
        ssql._vendor = 'postgres'
        sql = 'SELECT * FROM tab WHERE username = $1 OR password = $2'
        query = ssql.query.SELECT().FROM(
            tab
        ).WHERE(
            tab.username == 5
        ).OR(
            tab.password == 'password')
        self.assertEqual(query.sql(), sql)

    def test_query_sql(self):
        tab = Table('tab')
        ssql = Supersql(TESTING_DSN)
        xql = 'SELECT username, email FROM tab WHERE email = a@you.io'
        query = ssql.query.SELECT(tab.username, tab.email).FROM(tab).WHERE(tab.email == 'a@you.io')
        self.assertEqual(xql, query.sql(unsafe=True))

    def test_query_sql_pg(self):
        tab = Table('tab')
        ssql = Supersql(TESTING_DSN)
        ssql._vendor = POSTGRES
        xql = 'SELECT username, email FROM tab WHERE email = a@you.io'
        query = ssql.query.SELECT(tab.username, tab.email).FROM(tab).WHERE(tab.email == 'a@you.io')
        self.assertEqual(xql, query.sql(unsafe=True))

    def test_query_offset_limit(self):
        tab = Table('tab')
        ssql = Supersql(TESTING_DSN)
        sql = 'SELECT * FROM tab LIMIT 10 OFFSET 15'
        query = ssql.query.SELECT().FROM(tab).LIMIT(10).OFFSET(15)
        with self.assertRaises(ValueError):
            ssql.query.LIMIT('abcde')
        self.assertEqual(query.sql(), sql)

    def test_order_by(self):
        tab = Table('tab')
        ssql = Supersql(TESTING_DSN)
        sql = 'SELECT * FROM tab ORDER BY age'
        self.assertEqual(ssql.query.SELECT().FROM(tab).ORDER_BY(tab.age).sql(), sql)

    def test_asc_desc(self):
        tab = Table('tab')
        ssql = Supersql(TESTING_DSN)
        sql = 'SELECT * FROM tab ORDER BY age'
        self.assertEqual(ssql.query.SELECT().FROM(tab).ORDER_BY(+tab.age).sql(), f'{sql} ASC')
        self.assertEqual(ssql.query.SELECT().FROM(tab).ORDER_BY(-tab.age).sql(), f'{sql} DESC')

        self.assertEqual(ssql.query.SELECT().FROM(tab).ORDER_BY(tab.age).ASC().sql(), f'{sql} ASC')
        self.assertEqual(ssql.query.SELECT().FROM(tab).ORDER_BY(tab.age).DESC().sql(), f'{sql} DESC')

    def test_insert_into(self):
        query = self.ssql.query
        tab = self.table('tab')
        query = query.INSERT(
            tab.username, tab.email, tab.age
        ).INTO(tab)
        q = self.ssql.query.INSERT_INTO(tab, (tab.username, tab.email, tab.age))
        self.assertEqual(query.sql(), q.sql())

    def test_select_into(self):
        self.assertEqual(
            self.ssql.query.SELECT().INTO('tab').FROM('customers').sql(),
            'SELECT * INTO tab FROM customers'
        )

    def test_values_returning(self):
        query = self.ssql.query
        tab = self.table('tab')
        query = query.INSERT(tab.username, tab.email, tab.age).INTO(tab).VALUES(
            ('obi', 'a@a.a', 50,)
        ).RETURNING(tab.rowid)
        self.assertEqual(
            query.sql(),
            "INSERT INTO tab (username, email, age) VALUES (?, ?, ?) RETURNING rowid"
        )
        with self.assertRaises(ValueError):
            self.ssql.query.VALUES(4, 5, 6)

    def test_values_returning_pg(self):
        query = self.ssql.query
        tab = self.table('tab')
        query = query.INSERT(tab.username, tab.email, tab.age).INTO(tab).VALUES(
            ('obi', 'a@a.a', 50,)
        ).RETURNING(tab.rowid)
        query._engine._vendor = POSTGRES
        self.assertEqual(
            query.sql(),
            "INSERT INTO tab (username, email, age) VALUES ($1, $2, $3) RETURNING rowid"
        )

    def test_string_without_param_in_where_fails(self):
        tab = Table('tab')
        ssql = Supersql(TESTING_DSN)
        with self.assertRaises(ValueError):
            ssql.query.SELECT().FROM(tab).WHERE('username = ?')
        with self.assertRaises(ValueError):
            ssql.query.SELECT().FROM(tab).WHERE(1)

    def test_update(self):
        foo = self.table('foo')
        query = self.ssql.query
        self.assertEqual(
            self.ssql.query.UPDATE(foo).SET(
                name = 'alpha',
                email = 'beta'
            ).WHERE(foo.id == 5).sql(),
            'UPDATE foo SET name = ?, email = ? WHERE id = ?'
        )

    def test_update_pg(self):
        bar = self.table('bar')
        query = self.ssql.query
        query._engine._vendor = POSTGRES
        self.assertEqual(
            self.ssql.query.UPDATE(bar).SET(
                name = 'alpha',
                email = 'beta'
            ).WHERE(bar.id == 5).sql(),
            'UPDATE bar SET name = $1, email = $2 WHERE id = $3'
        )

    def test_delete_query(self):
        bar = self.table('bar')
        self.assertEqual(self.ssql.query.DELETE().FROM(
            bar
        ).WHERE(bar.id == 5).sql(), 'DELETE FROM bar WHERE id = ?')

    def test_join(self):
        bar = self.table('bar').AS('b')
        foo = self.table('foo').AS('f')
        sql = 'SELECT b.name, f.age FROM bar AS b WHERE b.id = $1 JOIN foo AS f ON f.id = b.id'
        query = self.ssql.query
        query._engine._vendor = POSTGRES
        self.assertEqual(self.ssql.query.SELECT(bar.name, foo.age).FROM
            (
                bar
            ).WHERE(
                bar.id == 5
            ).JOIN(foo).ON(foo.id == bar.id).sql(), sql
        )

    def test_multiple_joins(self):
        foo = self.table('foo').AS('f')
        bar = self.table('bar').AS('b')
        zoo = self.table('zoo').AS('z')
        sql = 'SELECT b.name, f.age FROM bar AS b WHERE b.id = $1 JOIN foo AS f ON f.id = b.id JOIN zoo AS z ON z.id = b.id'

        self.ssql.query._engine._vendor = POSTGRES
        query = self.ssql.query
        self.assertEqual(query.SELECT(bar.name, foo.age).FROM
            (
                bar
            ).WHERE(
                bar.id == 5
            ).JOIN(foo).ON(foo.id == bar.id)
            .JOIN(zoo).ON(zoo.id == bar.id).sql(), sql
        )


@mark.asyncio
class TestAsyncQuery(IsolatedAsyncioTestCase):  # pragma: no cover
    def setUp(self) -> None:
        # to test supersql locally, change these constants to your local variants
        self.dsn = POSTGRES_LIVE_DSN
        self.dsb = SQLITE_LIVE_DSN

    async def test_query_execution(self):
        sqlite = Supersql(self.dsb)
        try:
            await sqlite.connect()
            rows = await sqlite.query.SELECT().FROM('microservices').go()
        except Exception as exc:
            print(exc)
            raise ValueError(exc)
        finally: await sqlite.disconnect()

        self.assertIsNotNone(rows.row(1).column('microservice'))
        self.assertIsInstance(rows, Rows)

    async def test_query_execution_pg(self):
        postgres = Supersql(self.dsn)
        try:
            await postgres.connect()
            rows = await postgres.query.SELECT().FROM('identities').go()
        except Exception as exc:
            print(exc)
            raise ValueError()
        finally: await postgres.disconnect()

        self.assertIsNotNone(rows.row(1).column('username'))
        self.assertIsInstance(rows, Rows)

    async def test_insert_delete(self):
        postgres = Supersql(self.dsn)
        members = Table('members')
        query = postgres.query.INSERT(
            members.username,
            members.hpassword
        ).INTO(
            members
        ).VALUES(
            ('greatnoone@example.org', 'password'),
            ('somepaser@example.org', 'passworsd')
        ).RETURNING('id')
        try:
            await postgres.connect()
            await query.go()
        except Exception as exc:
            print(exc)
            raise ValueError(exc)
        finally: await postgres.disconnect()

        usernames = ['greatnoone@example.org', 'somepaser@example.org']
        q = postgres.query.DELETE().FROM(members).WHERE(members.username).IN(*usernames)

        try:
            await postgres.connect()
            await q.go()
        except Exception as exc:
            print(exc)
            raise ValueError(exc)
        finally:
            await postgres.disconnect()
    
    async def test_intelligent_select(self):
        postgres = Supersql(self.dsn)

        @dataclass
        class Members:
            id: int
            username: str
            hpassword: str

        command = postgres.query.SELECT().FROM('members')
        try:
            await postgres.connect()
            rows = await command.go(Members)
        except Exception as exc: print(exc); raise ValueError(exc)
        else:
            for row in rows:
                self.assertIsInstance(row, Members)
        finally: await postgres.disconnect()
