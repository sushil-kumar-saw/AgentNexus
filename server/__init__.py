"""Root FastAPI server package."""

import typing
if hasattr(typing, "ForwardRef") and hasattr(typing.ForwardRef, "_evaluate"):
    _orig_eval = typing.ForwardRef._evaluate
    def _compat_eval(self, globalns, localns, *args, **kwargs):
        import inspect
        sig = inspect.signature(_orig_eval)
        kw = {}
        if "recursive_guard" in sig.parameters:
            kw["recursive_guard"] = kwargs.get("recursive_guard", frozenset())
        if "type_params" in sig.parameters and args:
            kw["type_params"] = args[0]
        return _orig_eval(self, globalns, localns, **kw)
    typing.ForwardRef._evaluate = _compat_eval
