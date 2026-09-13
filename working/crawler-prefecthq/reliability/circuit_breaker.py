import time
import threading
import logging
import json

from enum import Enum

from exceptions.errors import CircuitBreakerOpenError

logger = logging.getLogger(__name__)

class CircuitState(str, Enum):
    """Circuit breaker states"""
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    """
    State transition :
        CLOSED : failure_count >= threshold --> OPEN
        OPEN : recovery_timeout elapsed --> Half open
        HALF_OPEN : success --> closed
        HALF_OPEN : failure --> open
    """
    def __init__(
            self, 
            failure_threshold=5,
            recovery_timeout=60,
        ):
        self._failure_threshold = failure_threshold
        self._recovery_timeout = recovery_timeout

        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._last_failure_time = None

        self._lock = threading.Lock()

    def call(self, fn):
        with self._lock:
            current_state = self._evaluate_state()

        if current_state == CircuitState.OPEN:
            raise CircuitBreakerOpenError(
                f"Circuit breaker is OPEN. "
                f"Recovery in {self._seconds_until_recovery():.0f}s"
                f"Failures: {self._failure_count}/{self._failure_threshold}"
            )

        try:
            result = fn()
            self._on_success()
            return result
        except Exception:
            self._on_failure()
            raise

    def _evaluate_state(self):
        if (
            self._state == CircuitState.OPEN and self._last_failure_time is not None
        ):
            elapsed = time.monotonic() - self._last_failure_time
            if elapsed >= self._recovery_timeout:
                self._transition_to(CircuitState.HALF_OPEN)
        return self._state

    def _on_success(self):
        with self._lock:
            if self._state == CircuitState.HALF_OPEN:
                self._transition_to(CircuitState.CLOSED)
            self._failure_count = 0

    def _on_failure(self):
        with self._lock:
            self._failure_count += 1
            self._last_failure_time = time.monotonic()

            if self._state == CircuitState.HALF_OPEN:
                self._transition_to(CircuitState.OPEN)
            elif self._failure_count >= self._failure_threshold:
                self._transition_to(CircuitState.OPEN)

    def _transition_to(self, new_state):
        old_state = self._state
        self._state = new_state

        if new_state == CircuitState.CLOSED:
            self._failure_count = 0

        logger.info(
            json.dumps(
                {
                    "event": "circuit_breaker_transition",
                    "from_state": old_state.value,
                    "to_state": new_state.value,
                    "failure_count": self._failure_count
                }
            )
        )

    def _seconds_until_recovery(self):
        if self._last_failure_time is None:
            return 0.0

        elapsed = time.monotonic() - self._last_failure_time
        return max(0, self._recovery_timeout - elapsed)

    @property
    def state(self):
        with self._lock:
            return self._evaluate_state()

    def get_status(self):
        with self._lock:
            current = self._evaluate_state()
            return {
                "state": current.value,
                "failure_count": self._failure_count,
                "failure_threshold": self._failure_threshold,
                "recovery_timeout": self._recovery_timeout,
                "seconds_until_recovery": (
                    self._seconds_until_recovery() 
                    if current == CircuitState.OPEN else 0.0
                )
            }