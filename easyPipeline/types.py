# -*- coding: UTF-8 -*-
'''
@Author  : Zhiheng Liu
@Email   : visitorindark@gmail.com
@CreateDate    : 2024/11/11 15:31 
@ModifyDate    : 2024/11/11 15:31
@Desc    : pipeline 基本类型
'''
from enum import Enum
from datetime import datetime
from dataclasses import dataclass, field
from typing import Dict, List, Any, Optional

class StepStatus(Enum):
    NOT_STARTED = "not_started"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"

class ExecutionMode(Enum):
    SYNCHRONOUS = "synchronous"
    ASYNCHRONOUS = "async"
    PARALLEL = "parallel"

@dataclass
class RetryPolicy:
    max_attempts: int = 3
    delay_seconds: int = 5
    backoff_factor: float = 2.0
    exceptions_to_retry: List[Exception] = field(default_factory=lambda: [ValueError, RuntimeError])

@dataclass
class MetricsData:
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    duration_seconds: Optional[float] = None
    memory_usage_mb: Optional[float] = None
    input_data_size: Optional[int] = None
    output_data_size: Optional[int] = None
    custom_metrics: Dict[str, Any] = field(default_factory=dict)