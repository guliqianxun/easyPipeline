from abc import ABC, abstractmethod
from typing import Any, Optional
import logging

from commom.types import StepStatus
from commom.metrics import Metrics
from commom.retry import RetryPolicy, retryable_execute

class Step(ABC):
    def __init__(
        self,
        name: str,
        retry_policy: Optional[RetryPolicy] = None,
    ):
        self.name = name
        self.retry_policy = retry_policy or RetryPolicy()
        self.metrics = Metrics()
        self.status = StepStatus.NOT_STARTED
        self.result = None
        self.error = None  # Store any error that occurred
        self.logger = logging.getLogger(f"Step.{self.name}")

    def execute(self, data: Any) -> Any:
        """
        Execute the step with the given data
        
        Args:
            data: Input data for the step
            
        Returns:
            Processed data
        """
        # Update state first - this is the primary mechanism
        self.metrics.start()
        self.status = StepStatus.RUNNING
        self.error = None
        self.logger.info(f"Step [{self.name}] started execution.")

        try:
            # Main execution with retry logic
            result = retryable_execute(
                self.process, self.retry_policy, data
            )
            
            # Update state on success
            self.result = result
            self.status = StepStatus.COMPLETED
            self.metrics.stop()
            self.logger.info(f"Step [{self.name}] completed successfully.")

            return result

        except Exception as e:
            # Update state on failure
            self.status = StepStatus.FAILED
            self.error = e
            self.metrics.stop()
            self.logger.error(f"Step [{self.name}] failed: {e}")
            raise

    @abstractmethod
    def process(self, data: Any) -> Any:
        """
        Each Step's specific logic will be overwritten by subclass.
        """
        pass

    def get_metrics(self) -> Metrics:
        return self.metrics

    def get_result(self) -> Any:
        return self.result
        
    def get_status(self) -> StepStatus:
        """Get the current status of this step"""
        return self.status
        
    def get_error(self) -> Optional[Exception]:
        """Get any error that occurred during execution"""
        return self.error
