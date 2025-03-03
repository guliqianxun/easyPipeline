# -*- coding: UTF-8 -*-
'''
@Author  : Zhiheng Liu
@Email   : visitorindark@gmail.com
@CreateDate    : 2025/03/03 19:41
@ModifyDate    : 2025/03/03 19:41
@Desc    : Task scheduler for managing parallel execution of pipeline tasks
'''

import uuid
import pickle
import logging
import threading
import time
from concurrent.futures import ProcessPoolExecutor, as_completed, ThreadPoolExecutor
from typing import Dict, Any, List, Optional, Tuple, Callable, Union, Set
from enum import Enum, auto
from dataclasses import dataclass
from datetime import datetime
import queue
import copy


class TaskType(Enum):
    """Types of tasks that can be scheduled"""
    PIPELINE = auto()  # A single pipeline execution
    DATA_CHUNK = auto()  # A data chunk to be processed by a pipeline
    
    
class TaskStatus(Enum):
    """Status of a task in the scheduler"""
    PENDING = auto()
    RUNNING = auto()
    COMPLETED = auto()
    FAILED = auto()
    CANCELLED = auto()


@dataclass
class Task:
    """Represents a task to be executed"""
    id: str
    type: TaskType
    pipeline_name: str
    pipeline_pickle: bytes  # Pickled pipeline object
    data: Any
    chunk_id: Optional[str] = None
    timeout: Optional[float] = None
    created_at: datetime = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    status: TaskStatus = TaskStatus.PENDING
    result: Any = None
    error: Optional[str] = None
    
    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now()
            
    @property
    def duration(self) -> Optional[float]:
        """Calculate the task duration in seconds"""
        if self.started_at and self.completed_at:
            return (self.completed_at - self.started_at).total_seconds()
        return None


class TaskScheduler:
    """
    Unified scheduler for pipeline and data-parallel tasks.
    
    This scheduler can operate in different modes:
    1. Pipeline-parallel: Each pipeline runs in its own process
    2. Data-parallel: Data chunks are processed in parallel
    3. Hybrid: Combination of both approaches
    
    It manages task queues, handles execution, and provides status tracking.
    """
    
    def __init__(self, 
                 max_workers: Optional[int] = None,
                 min_workers: Optional[int] = 1,
                 logger: Optional[logging.Logger] = None):
        """
        Initialize the task scheduler
        
        Args:
            max_workers: Maximum number of worker processes
            min_workers: Minimum number of worker processes
            logger: Optional logger instance
        """
        import os
        self.max_workers = max_workers or os.cpu_count()
        self.min_workers = min(self.max_workers, max(1, min_workers))
        self.logger = logger or logging.getLogger("pipeline.task_scheduler")
        
        # Task tracking
        self.tasks: Dict[str, Task] = {}
        self.task_queue = queue.Queue()
        self.active_tasks: Set[str] = set()
        
        # Status tracking
        self._running = False
        self._cancel_requested = False
        self._scheduler_thread = None
        self._scheduler_lock = threading.RLock()
        
        # Results callback
        self.result_callback: Optional[Callable[[str, Any], None]] = None

    def submit_pipeline_task(self, 
                           pipeline_name: str, 
                           pipeline_pickle: bytes, 
                           data: Any,
                           timeout: Optional[float] = None) -> str:
        """
        Submit a single pipeline execution task
        
        Args:
            pipeline_name: Name of the pipeline
            pipeline_pickle: Pickled pipeline object
            data: Input data for the pipeline
            timeout: Optional execution timeout
            
        Returns:
            Task ID string
        """
        task_id = str(uuid.uuid4())
        task = Task(
            id=task_id,
            type=TaskType.PIPELINE,
            pipeline_name=pipeline_name,
            pipeline_pickle=pipeline_pickle,
            data=data,
            timeout=timeout
        )
        
        with self._scheduler_lock:
            self.tasks[task_id] = task
            self.task_queue.put(task_id)
            self.logger.debug(f"Submitted pipeline task {task_id} for {pipeline_name}")
        
        return task_id

    def submit_data_chunk_task(self,
                              pipeline_name: str,
                              pipeline_pickle: bytes,
                              data_chunk: List[Any],
                              chunk_id: str,
                              timeout: Optional[float] = None) -> str:
        """
        Submit a data chunk to be processed by a pipeline
        
        Args:
            pipeline_name: Name of the pipeline
            pipeline_pickle: Pickled pipeline object
            data_chunk: List of data items to process
            chunk_id: Identifier for this chunk
            timeout: Optional execution timeout
            
        Returns:
            Task ID string
        """
        task_id = str(uuid.uuid4())
        task = Task(
            id=task_id,
            type=TaskType.DATA_CHUNK,
            pipeline_name=pipeline_name,
            pipeline_pickle=pipeline_pickle,
            data=data_chunk,
            chunk_id=chunk_id,
            timeout=timeout
        )
        
        with self._scheduler_lock:
            self.tasks[task_id] = task
            self.task_queue.put(task_id)
            self.logger.debug(f"Submitted data chunk task {task_id} for {pipeline_name}, chunk {chunk_id}")
        
        return task_id

    def start(self, blocking: bool = False) -> None:
        """
        Start the scheduler
        
        Args:
            blocking: If True, blocks until scheduler completes all tasks
        """
        with self._scheduler_lock:
            if self._running:
                self.logger.warning("Scheduler is already running")
                return
                
            self._running = True
            self._cancel_requested = False
            
            self._scheduler_thread = threading.Thread(
                target=self._scheduler_loop,
                daemon=not blocking
            )
            self._scheduler_thread.start()
            
            if blocking:
                self._scheduler_thread.join()

    def stop(self, wait: bool = True, timeout: Optional[float] = None) -> None:
        """
        Stop the scheduler
        
        Args:
            wait: If True, wait for all tasks to complete
            timeout: Maximum time to wait
        """
        with self._scheduler_lock:
            if not self._running:
                return
                
            self._running = False
            
        if wait and self._scheduler_thread:
            self._scheduler_thread.join(timeout)

    def cancel_all(self) -> None:
        """Cancel all pending and running tasks"""
        with self._scheduler_lock:
            self._cancel_requested = True
            
            # Mark all pending tasks as cancelled
            for task_id, task in self.tasks.items():
                if task.status == TaskStatus.PENDING:
                    task.status = TaskStatus.CANCELLED
                    
            # Clear the queue
            while not self.task_queue.empty():
                try:
                    self.task_queue.get_nowait()
                except queue.Empty:
                    break
                    
            self.logger.info("Cancelled all pending tasks")

    def get_task_status(self, task_id: str) -> Optional[TaskStatus]:
        """Get the status of a specific task"""
        with self._scheduler_lock:
            task = self.tasks.get(task_id)
            if task:
                return task.status
        return None

    def get_task_result(self, task_id: str) -> Optional[Any]:
        """Get the result of a completed task"""
        with self._scheduler_lock:
            task = self.tasks.get(task_id)
            if task and task.status == TaskStatus.COMPLETED:
                return task.result
        return None

    def get_all_results(self) -> Dict[str, Any]:
        """Get all completed task results"""
        results = {}
        with self._scheduler_lock:
            for task_id, task in self.tasks.items():
                if task.status == TaskStatus.COMPLETED:
                    results[task_id] = task.result
        return results

    def get_pipeline_results(self, pipeline_name: str) -> Dict[str, Any]:
        """Get all completed task results for a specific pipeline"""
        results = {}
        with self._scheduler_lock:
            for task_id, task in self.tasks.items():
                if (task.pipeline_name == pipeline_name and 
                    task.status == TaskStatus.COMPLETED):
                    results[task_id] = task.result
        return results

    def set_result_callback(self, callback: Callable[[str, Any], None]) -> None:
        """
        Set a callback function to be called when a task completes
        
        Args:
            callback: Function that takes (task_id, result) arguments
        """
        self.result_callback = callback

    def wait_for_tasks(self, task_ids: List[str], timeout: Optional[float] = None) -> bool:
        """
        Wait for specific tasks to complete
        
        Args:
            task_ids: List of task IDs to wait for
            timeout: Maximum time to wait in seconds
            
        Returns:
            True if all tasks completed, False if timed out
        """
        pending = set(task_ids)
        start_time = time.time()
        
        while pending and (timeout is None or time.time() - start_time < timeout):
            completed = set()
            
            with self._scheduler_lock:
                for task_id in pending:
                    task = self.tasks.get(task_id)
                    if task and task.status in (TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED):
                        completed.add(task_id)
            
            pending -= completed
            
            if not pending:
                return True
                
            time.sleep(0.1)  # Prevent tight loop
            
        return len(pending) == 0

    def clear_completed_tasks(self, older_than_seconds: Optional[float] = None) -> int:
        """
        Clear completed, failed or cancelled tasks from memory
        
        Args:
            older_than_seconds: Only clear tasks older than this many seconds
            
        Returns:
            Number of tasks cleared
        """
        to_remove = []
        cutoff_time = None
        
        if older_than_seconds is not None:
            cutoff_time = datetime.now().timestamp() - older_than_seconds
        
        with self._scheduler_lock:
            for task_id, task in self.tasks.items():
                if task.status in (TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED):
                    if cutoff_time is None or task.completed_at.timestamp() < cutoff_time:
                        to_remove.append(task_id)
            
            for task_id in to_remove:
                del self.tasks[task_id]
                
        return len(to_remove)

    def _scheduler_loop(self) -> None:
        """Main scheduler loop that processes tasks"""
        self.logger.info(f"Starting task scheduler with {self.max_workers} workers")
        
        with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {}
            
            while self._running or not self.task_queue.empty() or futures:
                # Check for completed futures
                completed_futures = []
                for future in list(futures.keys()):
                    if future.done():
                        task_id = futures[future]
                        self._process_completed_task(task_id, future)
                        completed_futures.append(future)
                
                # Remove completed futures
                for future in completed_futures:
                    del futures[future]
                
                # Check if we can submit more tasks
                can_submit = len(futures) < self.max_workers
                
                if can_submit and not self.task_queue.empty() and not self._cancel_requested:
                    try:
                        task_id = self.task_queue.get_nowait()
                        
                        with self._scheduler_lock:
                            task = self.tasks.get(task_id)
                            
                            if task and task.status == TaskStatus.PENDING:
                                # Mark task as running
                                task.status = TaskStatus.RUNNING
                                task.started_at = datetime.now()
                                self.active_tasks.add(task_id)
                                
                                # Submit the task to the executor
                                if task.type == TaskType.PIPELINE:
                                    future = executor.submit(
                                        self._execute_pipeline_task,
                                        task.pipeline_pickle,
                                        task.data,
                                        task.pipeline_name
                                    )
                                else:  # DATA_CHUNK
                                    future = executor.submit(
                                        self._execute_data_chunk_task,
                                        task.pipeline_pickle,
                                        task.data,
                                        task.chunk_id
                                    )
                                
                                futures[future] = task_id
                                self.logger.debug(f"Started execution of task {task_id}")
                            else:
                                self.logger.warning(f"Task {task_id} not found or not pending")
                    except queue.Empty:
                        pass
                
                # Small delay to prevent tight loop
                time.sleep(0.01)
                
        self.logger.info("Task scheduler stopped")

    def _process_completed_task(self, task_id: str, future) -> None:
        """Process a completed task future"""
        with self._scheduler_lock:
            task = self.tasks.get(task_id)
            if not task:
                return
                
            try:
                result = future.result(timeout=0.1)  # Should be immediate since future is done
                task.result = result
                task.status = TaskStatus.COMPLETED
                task.completed_at = datetime.now()
                self.logger.debug(f"Task {task_id} completed successfully")
                
                # Call result callback if set
                if self.result_callback:
                    try:
                        self.result_callback(task_id, result)
                    except Exception as e:
                        self.logger.error(f"Error in result callback: {e}")
                        
            except Exception as e:
                task.error = str(e)
                task.status = TaskStatus.FAILED
                task.completed_at = datetime.now()
                self.logger.error(f"Task {task_id} failed: {e}")
            
            # Remove from active tasks
            self.active_tasks.discard(task_id)

    @staticmethod
    def _execute_pipeline_task(pipeline_pickle: bytes, data: Any, pipeline_name: str) -> Any:
        """Execute a single pipeline with the given data"""
        pipeline = pickle.loads(pipeline_pickle)
        
        # Make a deep copy of the data to ensure isolation
        data_copy = copy.deepcopy(data)
        
        # Execute the pipeline
        return pipeline.execute(data_copy)

    @staticmethod
    def _execute_data_chunk_task(pipeline_pickle: bytes, chunk_data: List[Any], chunk_id: str) -> List[Dict]:
        """Execute a pipeline on a chunk of data items"""
        results = []
        
        # Only make one deep copy when receiving the data
        chunk_data = copy.deepcopy(chunk_data)
        pipeline = pickle.loads(pipeline_pickle)

        if not isinstance(chunk_data, (list, tuple, dict)):
            chunk_data = [chunk_data]

        if isinstance(chunk_data, dict):
            items = chunk_data.items()
        else:
            items = enumerate(chunk_data)

        for key, data in items:
            instance_id = f"{chunk_id}_{uuid.uuid4()}"
            try:
                result = pipeline.execute(data)
                results.append({
                    'instance_id': instance_id,
                    'success': True,
                    'result': result,
                    'error': None,
                    'status': {'status': 'completed'}
                })
            except Exception as e:
                results.append({
                    'instance_id': instance_id,
                    'success': False,
                    'result': None,
                    'error': str(e),
                    'status': {'status': 'failed'}
                })
                
        return results