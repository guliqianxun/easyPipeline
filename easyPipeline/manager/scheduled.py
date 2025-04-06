from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
from datetime import datetime

@dataclass
class TaskMetadata:
    task_id: str
    pipeline_name: str
    created_at: datetime = field(default_factory=datetime.now)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    success: bool = False
    error: Optional[Exception] = None
    extra: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "task_id": self.task_id,
            "pipeline_name": self.pipeline_name,
            "created_at": self.created_at,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "success": self.success,
            "error": str(self.error) if self.error else None,
            "extra": self.extra,
        }

@dataclass
class ScheduledTask:
    task_id: str
    pipeline_name: str
    input_data: Any
    metadata: TaskMetadata
    result: Any

@dataclass
class TaskGroupHandle:
    group_id: str
    task_ids: List[str] = field(default_factory=list)
    results: Dict[str, Any] = field(default_factory=dict)
    status: str = "submitted"
    results_by_pipeline: Dict[str, List[Any]] = field(default_factory=dict)
    extra: Dict[str, Any] = field(default_factory=dict)

    def is_complete(self) -> bool:
        return all(v is not None for v in self.results.values())

    def __init__(self, group_id: str):
        self.group_id = group_id
        self.task_ids: List[str] = []
        self.results: Dict[str, Any] = {}
        self.results_by_pipeline: Dict[str, List[Any]] = {}
        self.extra: Dict[str, Any] = {}
