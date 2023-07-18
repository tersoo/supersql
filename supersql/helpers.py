from .constants import POSTGRES, POSTGRESQL, SQLITE


async def Connexer(dsn: str, pooled = False):
    if not dsn: raise ValueError('DSN connection string is required to connect to database')
    vendor, _, remainder = dsn.partition('://')
    if vendor in [POSTGRES, POSTGRESQL]:
        from asyncpg import connect, create_pool
        if not pooled:
            return vendor, await connect(dsn=dsn)
        return vendor, await create_pool(dsn)
    elif vendor == SQLITE:
        from aiosqlite import connect, Row
        connection = await connect(remainder)
        connection.row_factory = Row
        return vendor, connection
    raise ValueError('Database string could not be used to determine a valid engine to connect to')
