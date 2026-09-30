import hashlib
import json
from importlib.resources import files

import asyncpg

from eventvault.config import Settings


async def init_connection(connection):
    await connection.set_type_codec(
        "jsonb", encoder=json.dumps, decoder=json.loads, schema="pg_catalog"
    )
    await connection.execute("SET TIME ZONE 'UTC'")


async def create_pool(settings: Settings):
    return await asyncpg.create_pool(
        settings.database_url.get_secret_value(),
        min_size=settings.db_pool_min,
        max_size=settings.db_pool_max,
        init=init_connection,
        command_timeout=30,
        server_settings={"application_name": "eventvault", "timezone": "UTC"},
    )


async def migrate(settings: Settings):
    connection = await asyncpg.connect(settings.database_url.get_secret_value())
    try:
        async with connection.transaction():
            await connection.execute("SELECT pg_advisory_xact_lock(716248310)")
            await connection.execute(
                "CREATE TABLE IF NOT EXISTS schema_migrations "
                "(name TEXT PRIMARY KEY, checksum TEXT NOT NULL, applied_at TIMESTAMPTZ "
                "DEFAULT now())"
            )
            for path in sorted(
                files("eventvault").joinpath("migrations").iterdir(), key=lambda p: p.name
            ):
                if path.name.endswith(".sql"):
                    source = path.read_text()
                    checksum = hashlib.sha256(source.encode()).hexdigest()
                    previous = await connection.fetchval(
                        "SELECT checksum FROM schema_migrations WHERE name=$1", path.name
                    )
                    if previous and previous != checksum:
                        raise RuntimeError(f"Migration checksum changed: {path.name}")
                    if not previous:
                        await connection.execute(source)
                        await connection.execute(
                            "INSERT INTO schema_migrations(name, checksum) VALUES ($1,$2)",
                            path.name,
                            checksum,
                        )
    finally:
        await connection.close()
