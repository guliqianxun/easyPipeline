# -*- coding: UTF-8 -*-
'''
@Author  : Zhiheng Liu
@Email   : visitorindark@gmail.com
@CreateDate    : 2024/11/11 15:35 
@ModifyDate    : 2025/02/11 15:35
@Desc    : pipeline Manager 管理单元，负责一整套算法的调度
'''
import logging
from datetime import datetime
from typing import Dict, Any, Union, List, Optional, Tuple
from .pipeline import Pipeline
from .types import ProgressCallback, ExecutionMode
from multiprocessing import freeze_support
import pickle
import os
import threading
from .task_scheduler import TaskScheduler, TaskStatus

def _create_child_logger(parent_logger, suffix, level_decrease=10):
    """Create a child logger with a decreased level"""
    logger_name = f"{parent_logger.name}.{suffix}"
    logger = logging.getLogger(logger_name)
    new_level = max(logging.DEBUG, parent_logger.level - level_decrease)
    logger.setLevel(new_level)
    return logger

class PipelineManager:
    """
    Unified manager for pipeline execution with multiple execution strategies.
    
    The manager supports three execution modes:
    - SEQUENTIAL: Execute pipelines one at a time, process data sequentially
    - PARALLEL: Execute pipelines in parallel, but each pipeline processes data sequentially
    - BATCH: Execute data in batches through pipelines, with each data chunk processed in parallel
    
    It also implements hierarchical logging, where each component logs at a progressively
    lower level to maintain readable logs.
    """
    
    def __init__(self, 
                 name: str = "pipeline_manager",
                 execution_mode: ExecutionMode = ExecutionMode.SEQUENTIAL,
                 max_workers: Optional[int] = None,
                 min_workers: Optional[int] = 1,
                 logger: logging.Logger = None,
                 callbacks: Optional[List[ProgressCallback]] = None,
                 auto_cleanup_failed: bool = True):
        """
        Initialize the pipeline manager.
        
        Args:
            name: Unique name for this manager
            execution_mode: Mode of execution (SEQUENTIAL, PARALLEL, BATCH)
            max_workers: Maximum number of worker processes (defaults to CPU count)
            min_workers: Minimum number of worker processes
            logger: Optional logger (creates one if not provided)
            callbacks: Optional list of progress callbacks
            auto_cleanup_failed: Automatically cleanup failed tasks
        """
        self.name = name
        self.execution_mode = execution_mode
        self.max_workers = max_workers or os.cpu_count()
        self.min_workers = min_workers
        self.pipelines: Dict[str, Pipeline] = {}
        self.logger = logger if logger is not None else logging.getLogger(f"pipeline_manager.{name}")
        self.callbacks = callbacks or []
        self.auto_cleanup_failed = auto_cleanup_failed
        
        # To track execution metrics
        self.execution_metrics: Dict[str, Dict[str, Union[int, float]]] = {}
        
        # For cancellation support
        self._cancel_event = threading.Event()
        
        # Initialize task scheduler
        self.task_scheduler = TaskScheduler(
            max_workers=self.max_workers,
            min_workers=self.min_workers,
            logger=_create_child_logger(self.logger, "scheduler")
        )
        self.task_scheduler.set_result_callback(self._on_task_completed)
        
        # Task ID mappings
        self._pipeline_tasks: Dict[str, Dict[str, str]] = {}  # {pipeline_name: {data_id: task_id}}
        self._batch_tasks: Dict[str, List[str]] = {}  # {pipeline_name: [task_ids]}
        
        # Failed task tracking
        self._failed_tasks: List[str] = []
        
        # Initialize multiprocessing support if needed
        if execution_mode in [ExecutionMode.PARALLEL, ExecutionMode.BATCH]:
            freeze_support()
            # Start the scheduler
            self.task_scheduler.start()

    def add_pipeline(self, pipeline: Pipeline) -> None:
        """
        Add a pipeline to the manager with hierarchical logging.
        
        Args:
            pipeline: Pipeline instance to add
        """
        # Set up hierarchical logging
        pipeline_logger = _create_child_logger(self.logger, pipeline.name)
        pipeline.logger = pipeline_logger
        
        # Add pipeline and initialize metrics
        self.pipelines[pipeline.name] = pipeline
        self.execution_metrics[pipeline.name] = {'call_count': 0, 
                                               'total_duration': 0.0,
                                               'success_count': 0,
                                               'failure_count': 0}
        
        self.logger.info(f"Added pipeline: {pipeline.name}")
        
        # Pass any callbacks to the pipeline
        for callback in self.callbacks:
            if callback not in pipeline.callbacks:
                pipeline.callbacks.append(callback)

    def remove_pipeline(self, pipeline_name: str) -> None:
        """
        Remove a pipeline from the manager.
        
        Args:
            pipeline_name: Name of the pipeline to remove
        """
        if pipeline_name in self.pipelines:
            del self.pipelines[pipeline_name]
            del self.execution_metrics[pipeline_name]
            self.logger.info(f"Removed pipeline: {pipeline_name}")
        else:
            self.logger.warning(f"Attempted to remove non-existent pipeline: {pipeline_name}")

    def execute_pipeline(self, pipeline_name: str, data: Any, 
                        timeout: Optional[float] = None) -> Any:
        """
        Execute a single pipeline and track its metrics.
        
        Args:
            pipeline_name: Name of the pipeline to execute
            data: Input data for the pipeline
            timeout: Optional timeout in seconds
            
        Returns:
            Result of the pipeline execution
            
        Raises:
            ValueError: If pipeline not found
            TimeoutError: If execution exceeds timeout
        """
        if pipeline_name not in self.pipelines:
            raise ValueError(f"Pipeline {pipeline_name} not found")
        
        pipeline = self.pipelines[pipeline_name]
        
        # Notify execution start
        for callback in self.callbacks:
            try:
                callback.on_pipeline_start(pipeline_name, data)
            except Exception as e:
                self.logger.warning(f"Callback error on pipeline start: {str(e)}")
        
        start_time = datetime.now()
        
        try:
            self.logger.info(f"Executing pipeline: {pipeline_name}")
            
            if self.execution_mode == ExecutionMode.SEQUENTIAL:
                # Direct execution in the current process
                result = pipeline.execute(data)
            else:
                # Execute via task scheduler
                pipeline_pickle = pickle.dumps(pipeline)
                task_id = self.task_scheduler.submit_pipeline_task(
                    pipeline_name, pipeline_pickle, data, timeout
                )
                
                # Store the task ID for tracking
                data_id = id(data)
                if pipeline_name not in self._pipeline_tasks:
                    self._pipeline_tasks[pipeline_name] = {}
                self._pipeline_tasks[pipeline_name][str(data_id)] = task_id
                
                # Wait for the task to complete
                if not self.task_scheduler.wait_for_tasks([task_id], timeout):
                    raise TimeoutError(f"Pipeline execution timed out after {timeout} seconds")
                
                # Get the result
                result = self.task_scheduler.get_task_result(task_id)
                status = self.task_scheduler.get_task_status(task_id)
                
                if status == TaskStatus.FAILED:
                    task = self.task_scheduler.tasks.get(task_id)
                    if task and task.error:
                        raise RuntimeError(f"Pipeline execution failed: {task.error}")
            
            # Track success metrics
            duration = (datetime.now() - start_time).total_seconds()
            self._update_metrics(pipeline_name, True, duration)
            
            # Notify execution completion
            for callback in self.callbacks:
                try:
                    callback.on_pipeline_complete(pipeline_name, result, duration)
                except Exception as e:
                    self.logger.warning(f"Callback error on pipeline completion: {str(e)}")
            
            return result
            
        except Exception as e:
            # Track failure metrics
            duration = (datetime.now() - start_time).total_seconds()
            self._update_metrics(pipeline_name, False, duration)
            
            # Notify execution failure
            for callback in self.callbacks:
                try:
                    callback.on_pipeline_error(pipeline_name, e, duration)
                except Exception as callback_e:
                    self.logger.warning(f"Callback error on pipeline error: {str(callback_e)}")
            
            self.logger.error(f"Pipeline {pipeline_name} failed: {str(e)}")
            raise

    def execute(self, pipeline_data_map: Dict[str, Any], 
                chunk_size: Optional[int] = None, 
                timeout: Optional[float] = None) -> Dict[str, Any]:
        """
        Execute pipelines based on a mapping of pipeline names to their input data.
        
        Args:
            pipeline_data_map: Dictionary mapping pipeline names to their input data
                             For batch mode, values should be lists of data items
            chunk_size: Optional size of data chunks for batch processing
            timeout: Optional timeout in seconds
            
        Returns:
            Dictionary mapping pipeline names to their results
            For batch mode, values are lists of result dictionaries
            
        Raises:
            ValueError: If pipeline not found
        """
        # Reset cancellation flag
        self._cancel_event.clear()
        
        # Validate pipeline names
        invalid_pipelines = [name for name in pipeline_data_map.keys() if name not in self.pipelines]
        if invalid_pipelines:
            raise ValueError(f"Invalid pipeline names: {invalid_pipelines}")
        
        results = {}
        
        # Start the scheduler if it's not running
        if not self.task_scheduler._running and self.execution_mode != ExecutionMode.SEQUENTIAL:
            self.task_scheduler.start()
        
        # Execute based on mode
        if self.execution_mode == ExecutionMode.SEQUENTIAL:
            # One pipeline at a time
            for name, data in pipeline_data_map.items():
                if isinstance(data, list) and len(data) > 0:
                    results[name] = self.execute_batch(name, data, chunk_size, timeout)
                else:
                    try:
                        results[name] = self.execute_pipeline(name, data, timeout)
                    except Exception as e:
                        self.logger.error(f"Pipeline {name} failed: {str(e)}")
                        results[name] = None
        
        elif self.execution_mode == ExecutionMode.PARALLEL:
            # All pipelines in parallel
            task_ids = {}
            
            for name, data in pipeline_data_map.items():
                if isinstance(data, list) and len(data) > 0:
                    # Handle batch data
                    batch_results = self.execute_batch(name, data, chunk_size, timeout)
                    results[name] = batch_results
                else:
                    # Schedule single pipeline execution
                    pipeline = self.pipelines[name]
                    pipeline_pickle = pickle.dumps(pipeline)
                    
                    task_id = self.task_scheduler.submit_pipeline_task(
                        name, pipeline_pickle, data, timeout
                    )
                    task_ids[name] = task_id
            
            # Wait for all pipeline tasks to complete
            if task_ids:
                self.task_scheduler.wait_for_tasks(list(task_ids.values()), timeout)
                
                # Collect results
                for name, task_id in task_ids.items():
                    status = self.task_scheduler.get_task_status(task_id)
                    
                    if status == TaskStatus.COMPLETED:
                        results[name] = self.task_scheduler.get_task_result(task_id)
                    else:
                        self.logger.error(f"Pipeline {name} task did not complete successfully: {status}")
                        results[name] = None
        
        elif self.execution_mode == ExecutionMode.BATCH:
            # Process all data in batches
            for name, data in pipeline_data_map.items():
                if isinstance(data, list) and len(data) > 0:
                    results[name] = self.execute_batch(name, data, chunk_size, timeout)
                else:
                    # Single item as a batch of one
                    results[name] = self.execute_batch(name, [data], chunk_size, timeout)[0]
        
        return results

    def execute_batch(self, pipeline_name: str, input_data_list: List[Any], 
                     chunk_size: Optional[int] = None,
                     timeout: Optional[float] = None) -> List[Dict]:
        """
        Execute a batch of data through a pipeline.
        
        Args:
            pipeline_name: Name of the pipeline to execute
            input_data_list: List of data items to process
            chunk_size: Optional size of data chunks for each worker
            timeout: Optional timeout in seconds
            
        Returns:
            List of result dictionaries with status, result, and error information
        """
        if pipeline_name not in self.pipelines:
            raise ValueError(f"Pipeline {pipeline_name} not found")
            
        # 处理空列表输入
        if not input_data_list:
            self.logger.info(f"Empty input list for batch execution of {pipeline_name}")
            return []
        
        pipeline = self.pipelines[pipeline_name]
        start_time = datetime.now()
        
        try:
            self.logger.info(f"Starting batch execution for pipeline {pipeline_name} "
                           f"with {len(input_data_list)} items")
            
            # Determine chunk size if not specified
            if chunk_size is None:
                chunk_size = max(1, len(input_data_list) // self.max_workers)
                self.logger.info(f"Auto-calculated chunk size: {chunk_size}")
            
            # Create chunks of data
            chunks = [
                input_data_list[i:i + chunk_size]
                for i in range(0, len(input_data_list), chunk_size)
            ]
            
            # Use task scheduler for batch processing
            pipeline_pickle = pickle.dumps(pipeline)
            task_ids = []
            
            # Submit tasks for each chunk
            for i, chunk in enumerate(chunks):
                chunk_id = f"{pipeline_name}_chunk_{i}"
                task_id = self.task_scheduler.submit_data_chunk_task(
                    pipeline_name, pipeline_pickle, chunk, chunk_id, timeout
                )
                task_ids.append(task_id)
            
            # Track batch tasks
            self._batch_tasks[pipeline_name] = task_ids
            
            # Wait for all chunks to complete
            all_completed = self.task_scheduler.wait_for_tasks(task_ids, timeout)
            
            if not all_completed:
                self.logger.warning(f"Timeout waiting for batch execution of {pipeline_name}")
            
            # Collect results from all completed chunks
            all_results = []
            success_count = 0
            failure_count = 0
            
            for task_id in task_ids:
                status = self.task_scheduler.get_task_status(task_id)
                
                if status == TaskStatus.COMPLETED:
                    chunk_results = self.task_scheduler.get_task_result(task_id)
                    all_results.extend(chunk_results)
                    
                    # Count successes and failures
                    success_count += sum(1 for r in chunk_results if r.get('success', False))
                    failure_count += sum(1 for r in chunk_results if not r.get('success', False))
                else:
                    self.logger.error(f"Chunk task {task_id} did not complete successfully: {status}")
            
            # Track metrics
            duration = (datetime.now() - start_time).total_seconds()
            self.execution_metrics[pipeline_name]['call_count'] += len(input_data_list)
            self.execution_metrics[pipeline_name]['total_duration'] += duration
            self.execution_metrics[pipeline_name]['success_count'] += success_count
            self.execution_metrics[pipeline_name]['failure_count'] += failure_count
            
            self.logger.info(f"Batch execution completed: {success_count} successes, "
                           f"{failure_count} failures in {duration:.2f}s")
            
            return all_results
            
        except Exception as e:
            self.logger.error(f"Batch execution for pipeline {pipeline_name} failed: {str(e)}")
            raise

    def cancel_execution(self):
        """Signal cancellation for any ongoing batch executions"""
        self.logger.warning("Cancellation requested for execution")
        self._cancel_event.set()
        self.task_scheduler.cancel_all()

    def _update_metrics(self, pipeline_name: str, success: bool, duration: float) -> None:
        """Update execution metrics for a pipeline"""
        if pipeline_name not in self.execution_metrics:
            return
            
        self.execution_metrics[pipeline_name]['call_count'] += 1
        self.execution_metrics[pipeline_name]['total_duration'] += duration
        
        if success:
            self.execution_metrics[pipeline_name]['success_count'] += 1
        else:
            self.execution_metrics[pipeline_name]['failure_count'] += 1

    def _on_task_completed(self, task_id: str, result: Any) -> None:
        """Callback when a task is completed"""
        # Check the task status to see if it failed
        with self.task_scheduler._scheduler_lock:
            task = self.task_scheduler.tasks.get(task_id)
            if task and task.status == TaskStatus.FAILED:
                self._on_task_failed(task_id, task)
            
        # Additional custom handling for completed tasks can go here

    def _on_task_failed(self, task_id: str, task) -> None:
        """Handle a failed task"""
        # Log the failure with details
        self.logger.error(f"Task {task_id} ({task.pipeline_name}) failed: {task.error}")
        
        # Add to failed tasks list
        self._failed_tasks.append(task_id)
        
        # Notify callbacks
        for callback in self.callbacks:
            try:
                callback.on_pipeline_error(task.pipeline_name, Exception(task.error), 
                                          task.duration or 0.0)
            except Exception as e:
                self.logger.warning(f"Error in failure callback: {e}")
        
        # Update metrics
        self._update_metrics(task.pipeline_name, False, task.duration or 0.0)
        
        # Auto cleanup if enabled
        if self.auto_cleanup_failed:
            self.cleanup_failed_task(task_id)

    def cleanup_failed_task(self, task_id: str) -> bool:
        """
        Cleanup resources associated with a failed task
        
        Args:
            task_id: ID of the failed task
            
        Returns:
            True if cleanup was successful, False otherwise
        """
        try:
            # Get the task from scheduler
            with self.task_scheduler._scheduler_lock:
                task = self.task_scheduler.tasks.get(task_id)
                if not task:
                    self.logger.warning(f"Task {task_id} not found for cleanup")
                    return False
                
                # If the task isn't marked as failed yet, mark it
                if task.status != TaskStatus.FAILED:
                    task.status = TaskStatus.FAILED
                    task.completed_at = datetime.now() if not task.completed_at else task.completed_at
                
                # Remove from active tasks
                self.task_scheduler.active_tasks.discard(task_id)
                
                # Clear any large data fields to free memory
                task.data = None
                task.pipeline_pickle = None
                task.result = None
                
                # Keep minimal error information
                if task.error and len(task.error) > 1000:
                    task.error = task.error[:997] + "..."
                
                # Remove from tracking dictionaries
                self._remove_task_from_tracking(task_id, task.pipeline_name)
                
                self.logger.info(f"Cleaned up failed task {task_id}")
                return True
            
        except Exception as e:
            self.logger.error(f"Error cleaning up failed task {task_id}: {e}")
            return False

    def _remove_task_from_tracking(self, task_id: str, pipeline_name: str) -> None:
        """Remove a task from all tracking dictionaries"""
        # Remove from pipeline tasks
        if pipeline_name in self._pipeline_tasks:
            data_ids = [did for did, tid in self._pipeline_tasks[pipeline_name].items() 
                        if tid == task_id]
            for data_id in data_ids:
                self._pipeline_tasks[pipeline_name].pop(data_id, None)
        
        # Remove from batch tasks
        if pipeline_name in self._batch_tasks:
            if task_id in self._batch_tasks[pipeline_name]:
                self._batch_tasks[pipeline_name].remove(task_id)

    def cleanup_all_failed_tasks(self) -> int:
        """
        Clean up all failed tasks
        
        Returns:
            Number of tasks cleaned up
        """
        cleaned_count = 0
        
        # Get a copy of the failed tasks list since we'll be modifying it
        tasks_to_clean = self._failed_tasks.copy()
        
        for task_id in tasks_to_clean:
            if self.cleanup_failed_task(task_id):
                cleaned_count += 1
                self._failed_tasks.remove(task_id)
        
        # Also check for any failed tasks in the scheduler that might not be in our list
        with self.task_scheduler._scheduler_lock:
            for task_id, task in list(self.task_scheduler.tasks.items()):
                if task.status == TaskStatus.FAILED and task_id not in self._failed_tasks:
                    if self.cleanup_failed_task(task_id):
                        cleaned_count += 1
        
        self.logger.info(f"Cleaned up {cleaned_count} failed tasks")
        return cleaned_count

    def get_failed_tasks(self) -> List[Dict[str, Any]]:
        """
        Get information about all failed tasks
        
        Returns:
            List of dictionaries with task information
        """
        failed_tasks_info = []
        
        with self.task_scheduler._scheduler_lock:
            for task_id in self._failed_tasks:
                task = self.task_scheduler.tasks.get(task_id)
                if task:
                    failed_tasks_info.append({
                        'task_id': task_id,
                        'pipeline_name': task.pipeline_name,
                        'type': task.type.name,
                        'chunk_id': task.chunk_id,
                        'error': task.error,
                        'created_at': task.created_at,
                        'completed_at': task.completed_at,
                        'duration': task.duration
                    })
        
        return failed_tasks_info

    def retry_failed_task(self, task_id: str) -> Optional[str]:
        """
        Retry a failed task
        
        Args:
            task_id: ID of the failed task to retry
            
        Returns:
            New task ID if successful, None otherwise
        """
        with self.task_scheduler._scheduler_lock:
            task = self.task_scheduler.tasks.get(task_id)
            if not task or task.status != TaskStatus.FAILED:
                self.logger.warning(f"Task {task_id} not found or not in failed state")
                return None
            
            # Create a new task with the same parameters
            if task.type == TaskType.PIPELINE:
                # For a regular pipeline task
                new_task_id = self.task_scheduler.submit_pipeline_task(
                    task.pipeline_name,
                    task.pipeline_pickle,
                    task.data,
                    task.timeout
                )
            else:
                # For a data chunk task
                new_task_id = self.task_scheduler.submit_data_chunk_task(
                    task.pipeline_name,
                    task.pipeline_pickle,
                    task.data,
                    task.chunk_id,
                    task.timeout
                )
            
            # Update tracking dictionaries
            self._update_tracking_for_retry(task_id, new_task_id, task.pipeline_name)
            
            # If original task is in failed tasks list, remove it
            if task_id in self._failed_tasks:
                self._failed_tasks.remove(task_id)
            
            self.logger.info(f"Retrying failed task {task_id} with new task {new_task_id}")
            return new_task_id

    def _update_tracking_for_retry(self, old_task_id: str, new_task_id: str, pipeline_name: str) -> None:
        """Update tracking dictionaries when retrying a task"""
        # Update pipeline tasks
        if pipeline_name in self._pipeline_tasks:
            for data_id, task_id in list(self._pipeline_tasks[pipeline_name].items()):
                if task_id == old_task_id:
                    self._pipeline_tasks[pipeline_name][data_id] = new_task_id
        
        # Update batch tasks
        if pipeline_name in self._batch_tasks:
            if old_task_id in self._batch_tasks[pipeline_name]:
                idx = self._batch_tasks[pipeline_name].index(old_task_id)
                self._batch_tasks[pipeline_name][idx] = new_task_id

    def cleanup(self) -> None:
        """Clean up resources and completed tasks"""
        # Clean up failed tasks first
        self.cleanup_all_failed_tasks()
        
        # Stop scheduler if running
        if hasattr(self, 'task_scheduler') and self.task_scheduler._running:
            self.task_scheduler.stop(wait=True, timeout=1.0)
            self.task_scheduler.clear_completed_tasks()

    def __del__(self):
        """Ensure resources are cleaned up when this object is deleted"""
        try:
            self.cleanup()
        except:
            pass

    def summarize_metrics(self) -> Dict[str, Any]:
        """
        Summarize pipeline execution metrics.

        Returns:
            A dictionary containing:
            - Total pipelines called
            - Total execution time across all pipelines
            - Mean execution time per pipeline
            - Success/failure counts and rates
        """
        total_pipelines = len(self.execution_metrics)
        total_duration = sum(metrics['total_duration'] for metrics in self.execution_metrics.values())
        total_calls = sum(metrics['call_count'] for metrics in self.execution_metrics.values())
        total_success = sum(metrics['success_count'] for metrics in self.execution_metrics.values())
        total_failures = sum(metrics['failure_count'] for metrics in self.execution_metrics.values())
        
        mean_duration = total_duration / total_calls if total_calls > 0 else 0.0
        success_rate = (total_success / total_calls * 100) if total_calls > 0 else 0.0

        summary = {
            "total_pipelines": total_pipelines,
            "total_calls": total_calls,
            "total_duration": total_duration,
            "mean_duration": mean_duration,
            "total_success": total_success,
            "total_failures": total_failures,
            "success_rate": success_rate,
            "detailed_metrics": {
                name: {
                    "calls": metrics["call_count"],
                    "duration": metrics["total_duration"],
                    "avg_duration": metrics["total_duration"] / metrics["call_count"] if metrics["call_count"] > 0 else 0,
                    "success_rate": (metrics["success_count"] / metrics["call_count"] * 100) 
                                    if metrics["call_count"] > 0 else 0
                }
                for name, metrics in self.execution_metrics.items()
            }
        }

        self.logger.info(f"Pipeline Metrics Summary: {summary}")
        return summary

    def get_status(self) -> Dict:
        """Get the current status of all pipelines and their metrics"""
        return {
            "manager_name": self.name,
            "execution_mode": self.execution_mode.value,
            "total_pipelines": len(self.pipelines),
            "pipeline_statuses": {
                name: pipeline.get_status()
                for name, pipeline in self.pipelines.items()
            },
            "execution_metrics": self.execution_metrics
        }

    def log_pipeline_metrics(self) -> None:
        """Log metrics for each pipeline"""
        self.logger.info("Pipeline Execution Metrics:")
        for name, metrics in self.execution_metrics.items():
            calls = metrics['call_count']
            duration = metrics['total_duration']
            avg_duration = duration / calls if calls > 0 else 0
            success_rate = (metrics['success_count'] / calls * 100) if calls > 0 else 0
            
            self.logger.info(
                f"Pipeline: {name}, Calls: {calls}, Total Duration: {duration:.2f}s, "
                f"Avg Duration: {avg_duration:.2f}s, Success Rate: {success_rate:.1f}%"
            )
