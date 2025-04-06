# metrics.py
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Any, Optional, List

@dataclass
class Metrics:
    """Base metrics class with timing functionality"""
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    custom_metrics: Dict[str, Any] = field(default_factory=dict)

    def start(self):
        """Mark the start time of an operation"""
        self.start_time = datetime.now()

    def stop(self):
        """Mark the end time of an operation"""
        self.end_time = datetime.now()

    @property
    def duration(self) -> float:
        """Calculate duration in seconds between start and end time"""
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return 0.0

    def to_dict(self) -> Dict[str, Any]:
        """Convert metrics to dictionary format"""
        return {
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "duration": self.duration,
            "custom_metrics": self.custom_metrics,
        }

@dataclass
class StepMetrics(Metrics):
    """步骤执行指标"""
    
    def __init__(self, name: str, metrics: Metrics = None):
        super().__init__()
        self.name = name
        self.metrics = metrics or Metrics()
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典表示"""
        return {
            "name": self.name,
            "metrics": self.metrics.to_dict()   
        }

@dataclass
class PipelineMetrics(Metrics):
    """管道执行指标"""
    
    def __init__(self):
        super().__init__()
        self.step_count = 0
        self.successful_steps = 0
        self.failed_steps = 0
        self.step_metrics: List[StepMetrics] = []
    
    def aggregate(self) -> Dict[str, Any]:
        """聚合所有指标"""
        steps_dict = [metrics.to_dict() for metrics in self.step_metrics]
        
        return {
            "duration": self.duration,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "step_count": self.step_count,
            "successful_steps": self.successful_steps,
            "failed_steps": self.failed_steps,
            "steps": steps_dict
        }

@dataclass
class BatchMetrics(Metrics):
    """Metrics for batch processing operations"""
    total_items: int = 0
    completed_items: int = 0
    successful_items: int = 0
    failed_items: int = 0
    
    @property
    def success_rate(self) -> float:
        """Calculate the success rate as a percentage"""
        if self.completed_items == 0:
            return 0.0
        return (self.successful_items / self.completed_items) * 100
    
    @property
    def completion_rate(self) -> float:
        """Calculate the completion rate as a percentage"""
        if self.total_items == 0:
            return 0.0
        return (self.completed_items / self.total_items) * 100
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert batch metrics to dictionary format"""
        base_dict = super().to_dict()
        base_dict.update({
            "total_items": self.total_items,
            "completed_items": self.completed_items,
            "successful_items": self.successful_items,
            "failed_items": self.failed_items,
            "success_rate": self.success_rate,
            "completion_rate": self.completion_rate,
        })
        return base_dict
