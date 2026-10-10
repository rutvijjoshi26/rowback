"""Exception types raised by rowback."""
from __future__ import annotations


class RowbackError(Exception):
    """A user-facing error with an actionable message."""


class NotUndoableError(RowbackError):
    """The selected transaction cannot be undone safely."""


class ConflictError(RowbackError):
    """Rows touched by the transaction were modified again afterwards."""

    def __init__(self, message: str, conflicts: list) -> None:  # type: ignore[type-arg]
        super().__init__(message)
        self.conflicts = conflicts
