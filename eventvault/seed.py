import asyncio

CATEGORIES = (
    "LAPTOP",
    "MONITOR",
    "KEYBOARD",
    "MOUSE",
    "PHONE",
    "TABLET",
    "HEADSET",
    "CAMERA",
    "ROUTER",
    "STORAGE",
)


async def seed(repository, count=1000):
    """Stable business inputs and request keys; interrupted runs safely resume."""

    async def one(index):
        sku = f"SKU-{index:06d}"
        category = CATEGORIES[(index - 1) % len(CATEGORIES)]
        body = {
            "sku": sku,
            "name": f"{category.title()} {index:06d}",
            "quantity": 100 + index % 20,
            "description": f"WH-JPR-01 | Jaipur | Asia/Kolkata | {category}",
            "unit_price": f"{1000 + (index % 100) * 125}.00",
        }
        item, _, _ = await repository.mutate("create", None, body, f"seed:v1:{sku}:create")
        operations = [
            ("reserve", {"quantity": 2}),
            ("update", {"name": body["name"] + " checked"}),
            ("release", {"quantity": 1}),
        ] * 3 + [("reserve", {"quantity": 2})]
        for step, (operation, data) in enumerate(operations):
            await repository.mutate(operation, item["id"], data, f"seed:v1:{sku}:{step}")

    # Sequential creation makes IDs reproducible in a fresh DB. Bounded chunks
    # yield to the event loop without buffering the whole dataset.
    for index in range(1, count + 1):
        await one(index)
        if index % 100 == 0:
            await asyncio.sleep(0)
    return {"items": count, "events": count * 11, "warehouse": "WH-JPR-01", "seed_version": 1}
