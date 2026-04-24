from __future__ import annotations

from dataclasses import dataclass, field

from nexuskv.contracts.generated import CacheEntry, KeyIdentity, QueryKey
from nexuskv.execution.types import PayloadHandle


def identity_tuple(identity: KeyIdentity) -> tuple:
    return (
        identity.tenant,
        identity.namespace,
        identity.model,
        identity.engine_family.value,
        identity.semantic_type.value,
        tuple(identity.tokens),
        identity.block_id,
        identity.page_id,
    )


@dataclass(slots=True)
class StoreRecord:
    entry: CacheEntry
    payload_handle: PayloadHandle
    writes: int = 1


@dataclass(slots=True)
class PrefetchIntent:
    query: QueryKey
    locator: str | None
    payload_handle: PayloadHandle | None = None


@dataclass(slots=True)
class InMemoryEntryStore:
    entries: dict[tuple, StoreRecord] = field(default_factory=dict)
    prefetch_intents: list[PrefetchIntent] = field(default_factory=list)

    def put(self, entry: CacheEntry, payload_handle: PayloadHandle) -> StoreRecord:
        key = identity_tuple(entry.identity.key)
        existing = self.entries.get(key)
        if existing is None:
            record = StoreRecord(entry=entry, payload_handle=payload_handle)
            self.entries[key] = record
            return record
        existing.entry = entry
        existing.payload_handle = payload_handle
        existing.writes += 1
        return existing

    def get(self, query: QueryKey) -> StoreRecord | None:
        return self.entries.get(identity_tuple(query.identity))

    def get_identity(self, identity: KeyIdentity) -> StoreRecord | None:
        return self.entries.get(identity_tuple(identity))

    def record_prefetch(self, query: QueryKey, locator: str | None, payload_handle: PayloadHandle | None) -> None:
        self.prefetch_intents.append(PrefetchIntent(query=query, locator=locator, payload_handle=payload_handle))
