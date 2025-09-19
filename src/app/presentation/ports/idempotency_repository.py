from typing import Protocol, Optional

class IIdempotencyRepository(Protocol):
    def get_last_state_hash(self, key: str) -> Optional[str]:
        ...

    def save_state_hash(self, key: str, state_hash: str) -> None:
        ...