from enum import Enum
from dataclasses import dataclass, field
from typing import Any, Dict
from datetime import datetime

class StepStatus(Enum):
    NOT_STARTED = "not_started"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"

class PipelineStatus(Enum):
    NOT_STARTED = "not_started"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"

class TaskStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"

@dataclass
class TaskMetadata:
    task_id: str
    created_at: datetime = field(default_factory=datetime.now)
    extra: Dict[str, Any] = field(default_factory=dict)
