import time
from typing import Type, Tuple
from dataclasses import dataclass, field

@dataclass
class RetryPolicy:
    max_attempts: int = 3
    delay_seconds: float = 2.0
    backoff_factor: float = 2.0
    retry_on_exceptions: Tuple[Type[Exception], ...] = field(default_factory=lambda: (Exception,))

    def should_retry(self, error: Exception, attempt: int) -> bool:
        return attempt < self.max_attempts and isinstance(error, self.retry_on_exceptions)

    def get_delay(self, attempt: int) -> float:
        return self.delay_seconds * (self.backoff_factor ** (attempt - 1))

def retryable_execute(func, retry_policy: RetryPolicy, *args, **kwargs):
    attempt = 0
    while True:
        try:
            attempt += 1
            return func(*args, **kwargs)
        except Exception as e:
            if retry_policy.should_retry(e, attempt):
                delay = retry_policy.get_delay(attempt)
                print(f"Retry {attempt}/{retry_policy.max_attempts} after {delay}s due to {e}")
                time.sleep(delay)
            else:
                print(f"Exceeded retry limit ({retry_policy.max_attempts}), error: {e}")
                raise
