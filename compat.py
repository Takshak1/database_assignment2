from __future__ import annotations

import inspect
import typing
from types import FunctionType


def patch_typing_forward_ref() -> bool:


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
    if recursive_guard_param is None:
        return False
    if recursive_guard_param.default is not inspect._empty:
        return False

    if getattr(evaluate, "__patched_for_recursive_guard__", False):
        return False

    def _evaluate(self, globalns, localns, *args, **kwargs):  # type: ignore[override]
        recursive_guard = None
        if args:
            recursive_guard = args[0]
        if "recursive_guard" in kwargs:
            if recursive_guard is None:
                recursive_guard = kwargs.pop("recursive_guard")
            else:
                kwargs.pop("recursive_guard")
        if recursive_guard is None:
            recursive_guard = set()
        return evaluate(self, globalns, localns, recursive_guard=recursive_guard)

    _evaluate.__patched_for_recursive_guard__ = True  
    forward_ref._evaluate = _evaluate
    return True


__all__ = ["patch_typing_forward_ref"]
