"""Compatibility helpers for bridging library/runtime gaps."""

from __future__ import annotations

import inspect
import typing
from types import FunctionType


def patch_typing_forward_ref() -> bool:
    """Patch typing.ForwardRef._evaluate for Python 3.12 if needed.

    Python 3.12 changed the signature of ForwardRef._evaluate to require a
    keyword-only ``recursive_guard`` argument. Older libraries (FastAPI +
    Pydantic<2) still call it with the legacy positional signature, which raises
    ``TypeError``. We wrap the method so both signatures keep working.

    Returns True when a patch was applied, False when no change was necessary.
    """

    forward_ref = getattr(typing, "ForwardRef", None)
    if forward_ref is None:
        return False

    evaluate: FunctionType = getattr(forward_ref, "_evaluate", None)
    if evaluate is None:
        return False

    try:
        signature = inspect.signature(evaluate)
    except (TypeError, ValueError):
        return False

    recursive_guard_param = signature.parameters.get("recursive_guard")
    # Already compatible if parameter missing or already optional
    if recursive_guard_param is None:
        return False
    if recursive_guard_param.default is not inspect._empty:
        return False

    # Avoid double patching
    if getattr(evaluate, "__patched_for_recursive_guard__", False):
        return False

    def _evaluate(self, globalns, localns, recursive_guard=None):  # type: ignore[override]
        if recursive_guard is None:
            recursive_guard = set()
        return evaluate(self, globalns, localns, recursive_guard=recursive_guard)

    _evaluate.__patched_for_recursive_guard__ = True  # type: ignore[attr-defined]
    forward_ref._evaluate = _evaluate
    return True


__all__ = ["patch_typing_forward_ref"]
