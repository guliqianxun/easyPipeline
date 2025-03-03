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
from typing import Dict, List, Any, Optional, Callable, Union, TypeVar, Generic

class StepStatus(Enum):
    NOT_STARTED = "not_started"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"

class ExecutionMode(Enum):
    PARALLEL = "parallel"
    SEQUENTIAL = "sequential"
    BATCH = "batch"

@dataclass
class RetryPolicy:
    max_attempts: int = 3
    delay_seconds: int = 5
    backoff_factor: float = 2.0
    exceptions_to_retry: List[Exception] = field(default_factory=lambda: [ValueError, RuntimeError])

@dataclass
class ResourceUsage:
    cpu_percent: Optional[float] = None
    memory_mb: Optional[float] = None
    peak_memory_mb: Optional[float] = None
    disk_io_reads: Optional[int] = None
    disk_io_writes: Optional[int] = None

@dataclass
class MetricsData:
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    duration_seconds: Optional[float] = None
    memory_usage_mb: Optional[float] = None
    input_data_size: Optional[int] = None
    output_data_size: Optional[int] = None
    resource_usage: ResourceUsage = field(default_factory=ResourceUsage)
    custom_metrics: Dict[str, Any] = field(default_factory=dict)

T = TypeVar('T')

class ProgressCallback(Generic[T]):
    """Callback interface for tracking pipeline progress"""
    def on_pipeline_start(self, pipeline_name: str, input_data: Any) -> None:
        """Called when a pipeline starts execution"""
        pass
        
    def on_pipeline_complete(self, pipeline_name: str, result: T, duration: float) -> None:
        """Called when a pipeline completes successfully"""
        pass
        
    def on_pipeline_error(self, pipeline_name: str, error: Exception, duration: float) -> None:
        """Called when a pipeline execution fails"""
        pass
        
    def on_step_start(self, pipeline_name: str, step_name: str) -> None:
        """Called when a pipeline step starts execution"""
        pass
        
    def on_step_complete(self, pipeline_name: str, step_name: str, duration: float) -> None:
        """Called when a pipeline step completes successfully"""
        pass
        
    def on_step_error(self, pipeline_name: str, step_name: str, error: Exception) -> None:
        """Called when a pipeline step execution fails"""
        pass
        
    def on_batch_progress(self, total_items: int, completed_items: int, 
                          successful_items: int, failed_items: int) -> None:
        """Called to report batch processing progress"""
        pass