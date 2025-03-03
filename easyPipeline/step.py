# -*- coding: UTF-8 -*-
'''
@Author  : Zhiheng Liu
@Email   : visitorindark@gmail.com
@CreateDate    : 2024/11/11 15:35 
@ModifyDate    : 2024/11/11 15:35
@Desc    : step  计算最小单元 
'''
from abc import ABC, abstractmethod
import logging
from typing import Dict, List, Callable, Optional, Any
import time
from datetime import datetime
from .types import ExecutionMode, RetryPolicy, StepStatus, MetricsData

class PipelineStep(ABC):
    def __init__(
        self,
        name: str,
        description: str,
        version: str = "1.0.0",
        input_type: str = "RadarData",
        output_type: str = "DataProduct",
        parameters: Dict = None,
        dependencies: List[str] = None,
        preconditions: List[str] = None,
        postconditions: List[str] = None,
        execution_mode: ExecutionMode = ExecutionMode.SEQUENTIAL,
        timeout: int = 3600,
        error_handling: str = "stop",
        retry_policy: RetryPolicy = None,
        logger: logging.Logger = None,
    ):
        self.name = name
        self.description = description
        self.version = version
        self.input_type = input_type
        self.output_type = output_type
        self.parameters = parameters or {}
        self.dependencies = dependencies or []
        self.preconditions = preconditions or []
        self.postconditions = postconditions or []
        self.execution_mode = execution_mode
        self.timeout = timeout
        self.error_handling = error_handling
        self.retry_policy = retry_policy or RetryPolicy()
        
        self.status = StepStatus.NOT_STARTED
        self.metrics = MetricsData()
        self.logger = logger if logger else logging.getLogger(f"pipeline.step.{name}")

    def logger_set(self, logger: logging.Logger):
        self.logger = logger

    def log_step_info(self) -> None:
        """Log step information and metrics"""
        self.logger.info(f"Step: {self.name} (v{self.version})")
        self.logger.info(f"Status: {self.status.value}")
        if self.metrics.duration_seconds is not None:
            self.logger.info(f"Duration: {self.metrics.duration_seconds:.2f}s")

    def _check_preconditions(self) -> bool:
        """Check if all preconditions are met"""
        for condition in self.preconditions:
            if not eval(condition):
                self.logger.warning(f"Precondition failed: {condition}")
                return False
        return True

    def _check_postconditions(self) -> bool:
        """Check if all postconditions are met"""
        for condition in self.postconditions:
            if not eval(condition):
                self.logger.warning(f"Postcondition failed: {condition}")
                return False
        return True

    def execute(self, data: Any) -> Any:
        """Execute the step with retry policy and metrics tracking"""
        self.metrics.start_time = datetime.now()
        self.status = StepStatus.RUNNING
        
        if not self._check_preconditions():
            self.status = StepStatus.SKIPPED
            return data

        attempts = 0
        while attempts < self.retry_policy.max_attempts:
            try:
                result = self.process(data)
                if self._check_postconditions():
                    self.status = StepStatus.COMPLETED
                    self.metrics.end_time = datetime.now()
                    self.metrics.duration_seconds = (
                        self.metrics.end_time - self.metrics.start_time
                    ).total_seconds()
                    self.log_step_info()
                    return result
                raise ValueError("Postconditions not met")
            
            except Exception as e:
                attempts += 1
                self.logger.error(f"Step {self.name} failed: {str(e)}")
                if attempts < self.retry_policy.max_attempts:
                    delay = self.retry_policy.delay_seconds * (
                        self.retry_policy.backoff_factor ** (attempts - 1)
                    )
                    time.sleep(delay)
                else:
                    self.status = StepStatus.FAILED
                    raise

        return data

    @abstractmethod
    def process(self, data: Any) -> Any:
        """Process the data - to be implemented by concrete steps"""
        pass