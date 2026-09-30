import hashlib
import json
from datetime import datetime
from decimal import Decimal
from uuid import uuid4

import asyncpg


class DomainError(Exception):
    def __init__(self, status, detail):
        self.status, self.detail = status, detail
        super().__init__(str(detail))


def serialize(row):
    if row is None:
        return None
    result = dict(row)
    for key, value in result.items():
        if isinstance(value, datetime):
            result[key] = value.isoformat()
        elif isinstance(value, Decimal):
            result[key] = str(value)  # Exact money, no binary floating-point rounding.
        elif key == "event_id":
            result[key] = str(value)
    if "reserved_quantity" in result:
        result["available_quantity"] = result["quantity"] - result["reserved_quantity"]
    return result


class Repository:
    def __init__(self, pool, metrics, faults):
        self.pool, self.metrics, self.faults = pool, metrics, faults

    async def item(self, item_id):
        self.metrics.inc("db_read_queries")
        return serialize(await self.pool.fetchrow("SELECT * FROM items WHERE id=$1", item_id))

    async def version(self, item_id):
        self.metrics.inc("db_read_queries")
        return await self.pool.fetchval("SELECT version FROM items WHERE id=$1", item_id)

    async def mutate(self, operation, item_id, data, key, expected=None):
        canonical = json.dumps([operation, item_id, data, expected], sort_keys=True, default=str)
        fingerprint = hashlib.sha256(canonical.encode()).hexdigest()
        lock = int.from_bytes(hashlib.sha256(key.encode()).digest()[:8], "big", signed=True)
        try:
            async with self.pool.acquire() as connection, connection.transaction():
                # Hash collisions only serialize unrelated requests; the exact key
                # and fingerprint remain authoritative in the table.
                await connection.execute("SELECT pg_advisory_xact_lock($1)", lock)
                previous = await connection.fetchrow("SELECT * FROM idempotency WHERE key=$1", key)
                if previous:
                    if previous["fingerprint"] != fingerprint:
                        raise DomainError(409, "Idempotency key was used for a different request")
                    return previous["response"], previous["status"], False
                if operation == "create":
                    row = await connection.fetchrow(
                        "INSERT INTO items(sku,name,description,quantity,unit_price) "
                        "VALUES($1,$2,$3,$4,$5) RETURNING *",
                        data["sku"],
                        data["name"],
                        data.get("description"),
                        data["quantity"],
                        Decimal(data["unit_price"]),
                    )
                    event_type, status = "ITEM_CREATED", 201
                else:
                    row = await connection.fetchrow(
                        "SELECT * FROM items WHERE id=$1 FOR UPDATE", item_id
                    )
                    if not row:
                        raise DomainError(404, "Item not found")
                    if expected is not None and row["version"] != expected:
                        raise DomainError(
                            409,
                            {
                                "message": "Stale version",
                                "expected_version": expected,
                                "current_version": row["version"],
                            },
                        )
                    if operation in {"reserve", "release"}:
                        delta = data["quantity"] * (1 if operation == "reserve" else -1)
                        reserved = row["reserved_quantity"] + delta
                        if reserved < 0 or reserved > row["quantity"]:
                            raise DomainError(409, "Insufficient available or reserved inventory")
                        row = await connection.fetchrow(
                            "UPDATE items SET reserved_quantity=$2,version=version+1,"
                            "updated_at=clock_timestamp() WHERE id=$1 RETURNING *",
                            item_id,
                            reserved,
                        )
                        event_type = "INVENTORY_RESERVED" if delta > 0 else "INVENTORY_RELEASED"
                    elif operation == "update":
                        row = await connection.fetchrow(
                            "UPDATE items SET name=$2,description=$3,unit_price=$4,"
                            "version=version+1,"
                            "updated_at=clock_timestamp() WHERE id=$1 RETURNING *",
                            item_id,
                            data.get("name", row["name"]),
                            data.get("description", row["description"]),
                            Decimal(data.get("unit_price", row["unit_price"])),
                        )
                        event_type = "ITEM_UPDATED"
                    else:
                        raise ValueError("Unknown operation")
                    status = 200
                self.faults.check("transaction_after_item_update")
                response = serialize(row)
                # Allocate only after item work. This lock is held until commit,
                # making committed event IDs a gap-free feed watermark.
                event_cursor = await connection.fetchval(
                    "UPDATE event_clock SET value=value+1 WHERE singleton RETURNING value"
                )
                await connection.execute(
                    "INSERT INTO events(id,event_id,event_type,aggregate_id,aggregate_version,"
                    "idempotency_key,payload) VALUES($1,$2,$3,$4,$5,$6,$7)",
                    event_cursor,
                    uuid4(),
                    event_type,
                    row["id"],
                    row["version"],
                    key,
                    {"changes": data, "state": response},
                )
                await connection.execute(
                    "INSERT INTO event_jobs(event_id) VALUES($1)", event_cursor
                )
                await connection.execute(
                    "INSERT INTO idempotency(key,fingerprint,response,status) VALUES($1,$2,$3,$4)",
                    key,
                    fingerprint,
                    response,
                    status,
                )
                self.faults.check("transaction_before_commit")
        except asyncpg.UniqueViolationError as exc:
            raise DomainError(409, "SKU or event version already exists") from exc
        self.metrics.inc("events_created")
        return response, status, True

    async def events(
        self, *, item_id=None, event_type=None, start=None, end=None, after=0, upper=None, limit=100
    ):
        clauses, values = ["id > $1"], [after]
        # SQL fragments are application constants, never request text.
        for column, operator, value in (
            ("aggregate_id", "=", item_id),
            ("event_type", "=", event_type),
            ("created_at", ">=", start),
            ("created_at", "<=", end),
            ("id", "<=", upper),
        ):
            if value is not None:
                values.append(value)
                clauses.append(f"{column} {operator} ${len(values)}")
        values.append(limit)
        query = "SELECT * FROM events WHERE " + " AND ".join(clauses)
        query += f" ORDER BY id LIMIT ${len(values)}"
        return [serialize(row) for row in await self.pool.fetch(query, *values)]

    async def after_version(self, item_id, version, limit=100):
        return [
            serialize(row)
            for row in await self.pool.fetch(
                "SELECT * FROM events WHERE aggregate_id=$1 AND aggregate_version>$2 "
                "ORDER BY aggregate_version LIMIT $3",
                item_id,
                version,
                limit,
            )
        ]

    async def stats(self):
        return dict(
            await self.pool.fetchrow(
                "SELECT (SELECT count(*) FROM items) AS items,"
                "(SELECT count(*) FROM events) AS events,"
                "(SELECT count(*) FROM event_jobs WHERE processed_at IS NULL AND NOT "
                "dead) AS pending,"
                "(SELECT count(*) FROM event_jobs WHERE dead) AS dead,"
                "(SELECT count(*) FROM event_jobs WHERE processed_at IS NOT NULL) AS processed"
            )
        )
