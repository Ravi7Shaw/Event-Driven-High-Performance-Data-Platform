from decimal import Decimal

from eventvault.repository import DomainError, serialize


async def replay(pool, item_id):
    # Snapshot prevents live writes from creating false mismatches between reads.
    async with (
        pool.acquire() as connection,
        connection.transaction(isolation="repeatable_read", readonly=True),
    ):
        actual = serialize(await connection.fetchrow("SELECT * FROM items WHERE id=$1", item_id))
        if actual is None:
            raise DomainError(404, "Item not found")
        state, count, errors = None, 0, []
        async for event in connection.cursor(
            "SELECT * FROM events WHERE aggregate_id=$1 ORDER BY aggregate_version",
            item_id,
        ):
            count += 1
            changes = event["payload"]["changes"]
            snapshot = event["payload"]["state"]
            if event["aggregate_version"] != count:
                errors.append(
                    f"Non-contiguous version: expected {count}, got {event['aggregate_version']}"
                )
            if event["event_type"] == "ITEM_CREATED":
                if state is not None:
                    errors.append("Duplicate creation event")
                state = {
                    **changes,
                    "id": item_id,
                    "reserved_quantity": 0,
                    "version": 1,
                    "created_at": snapshot["created_at"],
                    "updated_at": snapshot["updated_at"],
                }
            elif state is None:
                errors.append("Missing creation event")
                continue
            else:
                if event["event_type"] == "ITEM_UPDATED":
                    state.update(changes)
                elif event["event_type"] == "INVENTORY_RESERVED":
                    state["reserved_quantity"] += changes["quantity"]
                elif event["event_type"] == "INVENTORY_RELEASED":
                    state["reserved_quantity"] -= changes["quantity"]
                state["version"] += 1
                state["updated_at"] = snapshot["updated_at"]
            state["unit_price"] = str(Decimal(state["unit_price"]).quantize(Decimal(".01")))
            state["available_quantity"] = state["quantity"] - state["reserved_quantity"]
            if not 0 <= state["reserved_quantity"] <= state["quantity"]:
                errors.append(f"Invalid inventory at version {count}")
            if state != snapshot:
                errors.append(f"Event snapshot differs from reconstructed state at version {count}")
        if state is None:
            errors.append("No events")
        else:
            for field, value in actual.items():
                if state.get(field) != value:
                    errors.append(
                        f"{field}: reconstructed={state.get(field)!r}, database={value!r}"
                    )
        return {
            "item_id": item_id,
            "events": count,
            "reconstructed": state,
            "database": actual,
            "result": "INCONSISTENT" if errors else "CONSISTENT",
            "differences": errors,
        }
