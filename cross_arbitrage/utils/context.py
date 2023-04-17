import sys
import threading
import time
from typing import Any, Dict, Optional

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


class Context:
    forward: Optional[Self]

    mapping: Dict[str, Any]

    rlock = threading.RLock()
    lock = threading.Lock()

    def __init__(self, ctx: Optional[Self] = None):
        self.forward = ctx
        self.mapping = {}

    def get(self, name, default=None):
        with Context.rlock:
            if name in self.mapping:
                return self.mapping[name]
            if self.forward is not None:
                return self.forward.get(name, default)
            return default

    def set(self, name, value):
        with self.rlock and self.lock:
            self.mapping[name] = value

    def remove(self, name):
        with self.rlock and self.lock:
            if name in self.mapping:
                del self.mapping[name]


class CancelContext(Context):
    _cancel: bool

    def __init__(self, ctx: Optional[Self] = None):
        super().__init__(ctx)
        self._cancel = False

    def cancel(self):
        with self.rlock and self.lock:
            self._cancel = True

    def uncancel(self):
        with self.rlock and self.lock:
            self._cancel = False

    def is_canceled(self):
        with self.rlock:
            is_canceled = self._cancel
            if is_canceled:
                return True

            # if one of `forward` context is `CancelContext` and is cancle, return True
            forward = self.forward
            while forward is not None:
                if (
                    isinstance(forward, CancelContext)
                    and forward.is_canceled()
                ):
                    return True
                forward = forward.forward
        return False


def sleep_with_context(ctx: CancelContext, seconds: float, interval: float = 0.1):
    start = time.time()
    while not ctx.is_canceled():
        if time.time() - start > seconds:
            return

        time.sleep(interval)
