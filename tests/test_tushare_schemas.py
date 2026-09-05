from __future__ import annotations

import hashlib
import json

from quant_data.backends.tushare_schemas import TUSHARE_SCHEMAS


EXPECTED_SCHEMA_SIGNATURES = {
    "daily_basic": (
        19,
        "231581e5df759186791c72d964b721b5094834bb6e6fb51bc0598026bee9f70c",
    ),
    "income": (94, "a34a71f86c4f0031d881e95e56107d2a0a91f582edee81497e2e34f3c67722d5"),
    "balancesheet": (
        158,
        "ba28828d874253eb6a6bc0af9289ab5a5dbd835550466cb014a8dcda9b454d3a",
    ),
    "cashflow": (97, "6747821117a60a1355e405a8994526b33f6f07ae3af0877e811aa806d34c0e36"),
    "fina_indicator": (
        167,
        "4fdeabc916280408dd34280801742b3d8f1c599aaa2ff5dd967954f99c981ada",
    ),
    "express": (32, "b70be55d20ee30b5c81b7e88f2667554169094efa7b5fb7515a6b4b42b65aeca"),
    "forecast": (12, "2b26d16124c17fd986459fa9d06653e52615b127a14e462cb1996b8694a73821"),
    "stk_holdernumber": (
        4,
        "da9c2a4118fc8e1557a9caf047506753fbe29234e326af97b55d09a77321e638",
    ),
    "industry_member": (
        11,
        "2742e39b99573f99c9b5869a9e0c562f432c24194409546c49e261dadfccd40a",
    ),
}


def test_tushare_schema_signatures_are_stable() -> None:
    signatures: dict[str, tuple[int, str]] = {}
    for name, schema in TUSHARE_SCHEMAS.items():
        normalized = json.dumps(
            [(field.name, str(field.type)) for field in schema],
            separators=(",", ":"),
        )
        signatures[name] = (
            len(schema),
            hashlib.sha256(normalized.encode()).hexdigest(),
        )

    assert signatures == EXPECTED_SCHEMA_SIGNATURES
