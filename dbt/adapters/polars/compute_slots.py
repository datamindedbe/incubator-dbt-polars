import functools
import threading
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import Any, TypeVar

Function = TypeVar("Function", bound=Callable[..., Any])


class ComputeSlots:
    """Limits how many models run Polars at the same time.

    A model holds a slot while it runs. Where it holds no collected data, it
    releases the slot while it waits on a commit, so the commits of other
    models continue in parallel. It only takes a slot again when it computes
    again, so a model that is done after its commit doesn't wait for one.
    """

    def __init__(self) -> None:
        self.semaphore: threading.Semaphore | None = None
        self.state = threading.local()

    def configure(self, limit: int | None) -> None:
        self.semaphore = threading.Semaphore(limit) if limit else None

    def is_running_model(self) -> bool:
        return getattr(self.state, "running_model", False)

    def is_holding(self) -> bool:
        return getattr(self.state, "holding", False)

    @contextmanager
    def hold(self) -> Iterator[None]:
        if self.semaphore is None or self.is_running_model():
            yield
            return
        self.semaphore.acquire()
        self.state.running_model = True
        self.state.holding = True
        try:
            yield
        finally:
            if self.is_holding():
                self.semaphore.release()
            self.state.running_model = False
            self.state.holding = False

    @contextmanager
    def releasable(self, enabled: bool) -> Iterator[None]:
        """Allow `release` within the block, when the model holds no collected data."""
        previous = getattr(self.state, "releasable", False)
        self.state.releasable = enabled
        try:
            yield
        finally:
            self.state.releasable = previous

    def release(self) -> None:
        if (
            self.semaphore is not None
            and self.is_holding()
            and getattr(self.state, "releasable", False)
        ):
            self.semaphore.release()
            self.state.holding = False

    def ensure_held(self) -> None:
        if (
            self.semaphore is not None
            and self.is_running_model()
            and not self.is_holding()
        ):
            self.semaphore.acquire()
            self.state.holding = True


compute_slots = ComputeSlots()


def holds_compute_slot(function: Function) -> Function:
    @functools.wraps(function)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        with compute_slots.hold():
            return function(*args, **kwargs)

    return wrapper  # type: ignore[return-value]


def releases_compute_slot(function: Function) -> Function:
    @functools.wraps(function)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        compute_slots.release()
        return function(*args, **kwargs)

    return wrapper  # type: ignore[return-value]
