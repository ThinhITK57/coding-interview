import time
import threading
import logging
import json
from enum import Enum

from exceptions.errors import CircuitBreakerOpenError

logger = logging.getLogger(__name__)


class CircuitState(str, Enum):
    """Circuit breaker states."""
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    """
    Circuit breaker pattern to prevent cascade failures on upstream API downtime.

    Deep module design:
        Interface (small):  call(fn) -> result
        Implementation (deep): three-state machine (CLOSED -> OPEN -> HALF_OPEN),
        failure counting, recovery timeout, thread-safe transitions,
        structured JSON logging on state changes.

    State transitions:
        CLOSED  -- failure_count >= threshold --> OPEN
        OPEN    -- recovery_timeout elapsed  --> HALF_OPEN
        HALF_OPEN -- success                 --> CLOSED
        HALF_OPEN -- failure                 --> OPEN
    """

    def __init__(
        self,
        failure_threshold=5,
        recovery_timeout=60,
    ):
        """Initialize circuit breaker.

        Args:
            failure_threshold: Number of consecutive failures before opening circuit.
            recovery_timeout: Seconds to wait in OPEN state before testing with HALF_OPEN.
        """
        self._failure_threshold = failure_threshold
        self._recovery_timeout = recovery_timeout

        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._last_failure_time = None

        self._lock = threading.Lock()

    def call(self, fn):
        """Execute fn() through the circuit breaker.

        Args:
            fn: Callable to execute.

        Returns:
            Result of fn() on success.

        Raises:
            CircuitBreakerOpenError: If circuit is OPEN and recovery timeout
                has not elapsed.
            Any exception raised by fn() is re-raised after updating state.
        """
        with self._lock:
            current_state = self._evaluate_state()

        if current_state == CircuitState.OPEN:
            raise CircuitBreakerOpenError(
                f"Circuit breaker is OPEN. "
                f"Recovery in {self._seconds_until_recovery():.0f}s. "
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
        """Evaluate whether OPEN circuit should transition to HALF_OPEN."""
        if (
            self._state == CircuitState.OPEN
            and self._last_failure_time is not None
        ):
            elapsed = time.monotonic() - self._last_failure_time
            if elapsed >= self._recovery_timeout:
                self._transition_to(CircuitState.HALF_OPEN)
        return self._state

    def _on_success(self):
        """Handle successful call — reset failures, close circuit if HALF_OPEN."""
        with self._lock:
            if self._state == CircuitState.HALF_OPEN:
                self._transition_to(CircuitState.CLOSED)
            self._failure_count = 0

    def _on_failure(self):
        """Handle failed call — increment failures, open circuit if threshold reached."""
        with self._lock:
            self._failure_count += 1
            self._last_failure_time = time.monotonic()

            if self._state == CircuitState.HALF_OPEN:
                self._transition_to(CircuitState.OPEN)
            elif self._failure_count >= self._failure_threshold:
                self._transition_to(CircuitState.OPEN)

    def _transition_to(self, new_state):
        """Transition to a new state with structured logging."""
        old_state = self._state
        self._state = new_state

        if new_state == CircuitState.CLOSED:
            self._failure_count = 0

        logger.info(json.dumps({
            "event": "circuit_breaker_transition",
            "from_state": old_state.value,
            "to_state": new_state.value,
            "failure_count": self._failure_count,
        }))

    def _seconds_until_recovery(self):
        """Calculate seconds remaining until HALF_OPEN transition."""
        if self._last_failure_time is None:
            return 0.0
        elapsed = time.monotonic() - self._last_failure_time
        return max(0.0, self._recovery_timeout - elapsed)

    @property
    def state(self):
        """Current circuit state (evaluates OPEN -> HALF_OPEN transition)."""
        with self._lock:
            return self._evaluate_state()

    def get_status(self):
        """Return circuit breaker status for monitoring.

        Returns:
            dict: State, failure count, and recovery info.
        """
        with self._lock:
            current = self._evaluate_state()
            return {
                "state": current.value,
                "failure_count": self._failure_count,
                "failure_threshold": self._failure_threshold,
                "recovery_timeout": self._recovery_timeout,
                "seconds_until_recovery": (
                    self._seconds_until_recovery()
                    if current == CircuitState.OPEN
                    else 0.0
                ),
            }
