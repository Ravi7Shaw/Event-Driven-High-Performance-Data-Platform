import base64
import hashlib
import json

from eventvault.repository import DomainError


def filter_hash(filters):
    return hashlib.sha256(json.dumps(filters, sort_keys=True, default=str).encode()).hexdigest()[
        :16
    ]


def encode_cursor(after, upper, filters):
    data = {"v": 1, "after": after, "upper": upper, "filters": filter_hash(filters)}
    return (
        base64.urlsafe_b64encode(json.dumps(data, separators=(",", ":")).encode())
        .decode()
        .rstrip("=")
    )


def decode_cursor(cursor, filters):
    try:
        if len(cursor) > 512:
            raise ValueError
        data = json.loads(
            base64.b64decode(cursor + "=" * (-len(cursor) % 4), altchars=b"-_", validate=True)
        )
        if data["v"] != 1 or data["filters"] != filter_hash(filters):
            raise ValueError
        after, upper = data["after"], data["upper"]
        if type(after) is not int or type(upper) is not int or not 0 <= after <= upper < 2**63:
            raise ValueError
        return after, upper
    except (ValueError, KeyError, TypeError) as exc:
        raise DomainError(422, "Invalid cursor or changed filters") from exc
