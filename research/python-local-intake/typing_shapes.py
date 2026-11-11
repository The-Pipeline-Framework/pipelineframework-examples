"""Test-only candidate type shapes; future bindings are emitted by the Java compiler."""
from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from typing import Any, Generic, TypeAlias, TypeVar, final


@final
@dataclass(frozen=True, slots=True)
class InputValue:
    value: int


@final
@dataclass(frozen=True, slots=True)
class OutputValue:
    value: int


@dataclass(frozen=True, slots=True)
class InvocationContext:
    executionId: str
    traceId: str


T = TypeVar('T', covariant=True)


@dataclass(frozen=True, slots=True)
class Value(Generic[T]):
    value: T


Handler: TypeAlias = Callable[
    [InputValue, InvocationContext], Value[OutputValue] | Coroutine[Any, Any, Value[OutputValue]]
]


@dataclass(frozen=True, slots=True)
class Registry:
    transform: Handler
