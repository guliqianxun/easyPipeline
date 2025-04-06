import uuid
import threading
import logging
import concurrent.futures
from typing import Dict, Any, List, Optional
from datetime import datetime
from enum import Enum
import time
import os

from manager.scheduled import TaskGroupHandle, TaskMetadata, ScheduledTask
from pipeline.pipeline import Pipeline

class ExecutionMode(Enum):
    SEQUENTIAL = "sequential"
    PARALLEL_THREAD = "parallel_thread"
    PARALLEL_PROCESS = "parallel_process"
    BATCH = "batch"

class TaskScheduler:
    """Scheduler for pipeline execution tasks with state-driven architecture"""
    
    def __init__(self, 
                 max_workers: Optional[int] = None,
                 logger: Optional[logging.Logger] = None):
        """
        Initialize the task scheduler
        
        Args:
            max_workers: Maximum number of worker threads/processes
            logger: Optional logger instance
        """
        self.max_workers = max_workers or min(32, (os.cpu_count() or 4) + 4)
        self.logger = logger or logging.getLogger("TaskScheduler")
        
        # Task storage
        self.tasks: Dict[str, ScheduledTask] = {}
        self.results: Dict[str, Any] = {}
        self.pipeline_tasks: Dict[str, List[str]] = {}  # pipeline_name -> [task_ids]
        self.task_groups: Dict[str, TaskGroupHandle] = {}
        
        # Thread safety
        self.lock = threading.RLock()
        
        # Thread pool for parallel execution
        self.thread_pool = concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers)
        
        # Process pool for parallel execution - initialize only when needed
        self.process_pool: Optional[concurrent.futures.ProcessPoolExecutor] = None
        
        # Status tracking
        self.active_tasks: int = 0
        self.completed_tasks: int = 0
        self.failed_tasks: int = 0
        
        # Pipeline stats
        self.pipeline_stats: Dict[str, Dict[str, Any]] = {}
    
    def _get_process_pool(self) -> concurrent.futures.ProcessPoolExecutor:
        """Get or create the process pool"""
        if self.process_pool is None:
            self.process_pool = concurrent.futures.ProcessPoolExecutor(max_workers=self.max_workers)
        return self.process_pool
    
    def _execute_task(self, pipeline: Pipeline, task: ScheduledTask) -> None:
        """
        Execute a single task using the provided pipeline
        
        Args:
            pipeline: Pipeline to use for execution
            task: Task to execute
        """
        self.logger.debug(f"Executing task {task.task_id} with pipeline {pipeline.name}")
        
        # Update task metadata
        with self.lock:
            task.metadata.started_at = datetime.now()
            self.active_tasks += 1
        
        try:
            # Execute the pipeline
            result = pipeline.run(task.input_data)
            
            # Store the result and update task metadata
            with self.lock:
                task.result = result
                self.results[task.task_id] = result
                task.metadata.completed_at = datetime.now()
                task.metadata.success = True
                task.metadata.status_flag = "success"
                
                # Get metrics from pipeline
                if hasattr(pipeline, 'get_metrics'):
                    task.metadata.extra['metrics'] = pipeline.get_metrics()
                
                # Get state snapshot if available
                if hasattr(pipeline, 'get_state_snapshot'):
                    task.metadata.extra['state_snapshot'] = pipeline.get_state_snapshot()
                
                # Calculate duration
                if task.metadata.started_at and task.metadata.completed_at:
                    duration = (task.metadata.completed_at - task.metadata.started_at).total_seconds()
                    task.metadata.extra['duration'] = duration
                
                # Update counters
                self.active_tasks -= 1
                self.completed_tasks += 1
                
                # Update pipeline stats
                if task.pipeline_name not in self.pipeline_stats:
                    self.pipeline_stats[task.pipeline_name] = {
                        "total": 0,
                        "success": 0,
                        "failed": 0,
                        "in_progress": 0
                    }
                self.pipeline_stats[task.pipeline_name]["success"] += 1
                if self.pipeline_stats[task.pipeline_name]["in_progress"] > 0:
                    self.pipeline_stats[task.pipeline_name]["in_progress"] -= 1
                
                self.logger.info(f"Task {task.task_id} completed successfully in {duration:.2f}s")
            
        except Exception as e:
            # Handle error and update task metadata
            with self.lock:
                task.metadata.completed_at = datetime.now()
                task.metadata.success = False
                task.metadata.status_flag = "failed"
                task.metadata.error = e
                
                # Calculate duration
                if task.metadata.started_at and task.metadata.completed_at:
                    duration = (task.metadata.completed_at - task.metadata.started_at).total_seconds()
                    task.metadata.extra['duration'] = duration
                
                # Update counters
                self.active_tasks -= 1
                self.failed_tasks += 1
                
                # Update pipeline stats
                if task.pipeline_name not in self.pipeline_stats:
                    self.pipeline_stats[task.pipeline_name] = {
                        "total": 0,
                        "success": 0,
                        "failed": 0,
                        "in_progress": 0
                    }
                self.pipeline_stats[task.pipeline_name]["failed"] += 1
                if self.pipeline_stats[task.pipeline_name]["in_progress"] > 0:
                    self.pipeline_stats[task.pipeline_name]["in_progress"] -= 1
                
                self.logger.error(f"Task {task.task_id} failed after {duration:.2f}s: {e}")
    
    def _process_executor(self, pipeline_name: str, input_data: Any, pipeline_class_module: str, 
                         pipeline_class_name: str, steps_config: Optional[dict] = None) -> Dict[str, Any]:
        """
        Function that can be executed in a separate process
        
        Args:
            pipeline_name: Name of the pipeline to execute
            input_data: Input data for the pipeline
            pipeline_class_module: Module containing the pipeline class
            pipeline_class_name: Name of the pipeline class
            steps_config: Optional configuration for pipeline steps
                
        Returns:
            Dictionary containing result and execution metrics
        """
        # Create a simple local logger for this process
        process_logger = logging.getLogger(f"ProcessExecutor-{pipeline_name}")
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s\n'))
        process_logger.addHandler(handler)
        
        # Set up timing metrics
        start_time = time.time()
        
        try:
            # Import the pipeline class dynamically
            module = __import__(pipeline_class_module, fromlist=[pipeline_class_name])
            pipeline_class = getattr(module, pipeline_class_name)
            
            # Create pipeline instance
            pipeline = pipeline_class(name=pipeline_name)
            
            # Configure pipeline steps if configuration provided
            if steps_config and hasattr(pipeline, 'configure_steps'):
                pipeline.configure_steps(steps_config)
            
            # Run the pipeline
            result = pipeline.run(input_data)
            execution_time = time.time() - start_time
            
            # Gather state information
            state_data = {}
            if hasattr(pipeline, 'get_state_snapshot'):
                state_data = pipeline.get_state_snapshot()
            elif hasattr(pipeline, 'get_metrics'):
                state_data = {'metrics': pipeline.get_metrics()}
            
            return {
                'result': result,
                'success': True,
                'execution_time': execution_time,
                'state_data': state_data
            }
        except Exception as e:
            execution_time = time.time() - start_time
            process_logger.exception(f"Pipeline execution failed: {str(e)}")
            return {
                'result': None, 
                'success': False,
                'error': str(e),
                'error_type': type(e).__name__,
                'execution_time': execution_time
            }
    
    def _execute_sequential(self, pipeline_map: Dict[str, Pipeline], tasks: List[ScheduledTask]) -> None:
        """
        Execute tasks sequentially
        
        Args:
            pipeline_map: Map of pipeline names to pipeline instances
            tasks: List of tasks to execute
        """
        for task in tasks:
            if task.pipeline_name not in pipeline_map:
                self.logger.error(f"Pipeline {task.pipeline_name} not found for task {task.task_id}")
                task.metadata.completed_at = datetime.now()
                task.metadata.success = False
                task.metadata.error = ValueError(f"Pipeline {task.pipeline_name} not found")
                continue
            
            pipeline = pipeline_map[task.pipeline_name]
            self._execute_task(pipeline, task)
    
    def _execute_parallel_thread(self, pipeline_map: Dict[str, Pipeline], tasks: List[ScheduledTask]) -> None:
        """
        Execute tasks in parallel using threads
        
        Args:
            pipeline_map: Map of pipeline names to pipeline instances
            tasks: List of tasks to execute
        """
        futures = []
        
        for task in tasks:
            if task.pipeline_name not in pipeline_map:
                self.logger.error(f"Pipeline {task.pipeline_name} not found for task {task.task_id}")
                task.metadata.completed_at = datetime.now()
                task.metadata.success = False
                task.metadata.error = ValueError(f"Pipeline {task.pipeline_name} not found")
                continue
            
            pipeline = pipeline_map[task.pipeline_name]
            
            # Update pipeline stats to track in-progress tasks
            with self.lock:
                if task.pipeline_name not in self.pipeline_stats:
                    self.pipeline_stats[task.pipeline_name] = {
                        "total": 0,
                        "success": 0,
                        "failed": 0,
                        "in_progress": 0
                    }
                self.pipeline_stats[task.pipeline_name]["in_progress"] += 1
            
            future = self.thread_pool.submit(self._execute_task, pipeline, task)
            futures.append(future)
        
        # Wait for all tasks to complete
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()  # This will re-raise any exceptions from the task
            except Exception as e:
                self.logger.error(f"Task execution error: {e}")
    
    def _execute_parallel_process(self, pipeline_map: Dict[str, Pipeline], tasks: List[ScheduledTask]) -> None:
        """
        Execute tasks in parallel using processes
        
        Args:
            pipeline_map: Map of pipeline names to pipeline instances
            tasks: List of tasks to execute
        """
        process_pool = self._get_process_pool()
        futures = []
        total = len(tasks)
        completed = 0
        success = 0
        failed = 0
        
        try:
            # For each task, extract pipeline class info instead of passing the pipeline itself
            for task in tasks:
                if task.pipeline_name not in pipeline_map:
                    self.logger.error(f"Pipeline {task.pipeline_name} not found for task {task.task_id}")
                    task.metadata.completed_at = datetime.now()
                    task.metadata.success = False
                    task.metadata.error = ValueError(f"Pipeline {task.pipeline_name} not found")
                    failed += 1
                    completed += 1
                    continue
                
                pipeline = pipeline_map[task.pipeline_name]
                
                try:
                    # Update pipeline stats to track in-progress tasks
                    with self.lock:
                        if task.pipeline_name not in self.pipeline_stats:
                            self.pipeline_stats[task.pipeline_name] = {
                                "total": 0,
                                "success": 0,
                                "failed": 0,
                                "in_progress": 0
                            }
                        self.pipeline_stats[task.pipeline_name]["in_progress"] += 1
                    
                    # Extract class information rather than the instance
                    future = process_pool.submit(
                        self._process_executor, 
                        task.pipeline_name,
                        task.input_data,
                        pipeline.__class__.__module__,  # Module name
                        pipeline.__class__.__name__,    # Class name
                        getattr(pipeline, 'steps_config', None)  # Pipeline configuration if available
                    )
                    futures.append((future, task))
                    
                except Exception as e:
                    # Mark task as failed if submission fails
                    self.logger.error(f"Failed to submit task {task.task_id}: {e}")
                    task.metadata.completed_at = datetime.now()
                    task.metadata.success = False
                    task.metadata.error = e
                    failed += 1
                    completed += 1
                    
                    # Update pipeline stats
                    with self.lock:
                        if task.pipeline_name in self.pipeline_stats:
                            if self.pipeline_stats[task.pipeline_name]["in_progress"] > 0:
                                self.pipeline_stats[task.pipeline_name]["in_progress"] -= 1
                            self.pipeline_stats[task.pipeline_name]["failed"] += 1
            
            # Process results as they complete
            for future, task in futures:
                try:
                    process_result = future.result()  # Wait for completion
                    
                    # Update task metadata
                    task.metadata.completed_at = datetime.now() 
                    task.metadata.success = process_result.get('success', False)
                    
                    if task.metadata.success:
                        result = process_result.get('result')
                        task.result = result
                        
                        # Store the result
                        with self.lock:
                            self.results[task.task_id] = result
                        
                        # Store state data if available
                        if 'state_data' in process_result:
                            task.metadata.extra['state_data'] = process_result['state_data']
                        if 'execution_time' in process_result:
                            task.metadata.extra['execution_time'] = process_result['execution_time']
                        
                        # Update counters
                        completed += 1
                        success += 1
                        
                        # Update pipeline stats
                        with self.lock:
                            if task.pipeline_name in self.pipeline_stats:
                                if self.pipeline_stats[task.pipeline_name]["in_progress"] > 0:
                                    self.pipeline_stats[task.pipeline_name]["in_progress"] -= 1
                                self.pipeline_stats[task.pipeline_name]["success"] += 1
                        
                    else:
                        # Handle failure case
                        task.metadata.success = False
                        if 'error' in process_result:
                            task.metadata.error = Exception(process_result['error'])
                        else:
                            task.metadata.error = Exception("Unknown process execution error")
                        
                        # Update counters
                        completed += 1
                        failed += 1
                        
                        # Update pipeline stats
                        with self.lock:
                            if task.pipeline_name in self.pipeline_stats:
                                if self.pipeline_stats[task.pipeline_name]["in_progress"] > 0:
                                    self.pipeline_stats[task.pipeline_name]["in_progress"] -= 1
                                self.pipeline_stats[task.pipeline_name]["failed"] += 1
                    
                except Exception as e:
                    # Update task metadata on error
                    task.metadata.completed_at = datetime.now() 
                    task.metadata.success = False
                    task.metadata.error = e
                    
                    # Log the error
                    self.logger.error(f"Task {task.task_id} failed: {e}")
                    
                    # Update counters
                    completed += 1
                    failed += 1
                    
                    # Update pipeline stats
                    with self.lock:
                        if task.pipeline_name in self.pipeline_stats:
                            if self.pipeline_stats[task.pipeline_name]["in_progress"] > 0:
                                self.pipeline_stats[task.pipeline_name]["in_progress"] -= 1
                            self.pipeline_stats[task.pipeline_name]["failed"] += 1
            
        except Exception as e:
            self.logger.error(f"Error in parallel process execution: {e}")
            # Mark all remaining tasks as failed
            for future, task in futures:
                if not future.done():
                    future.cancel()
                    task.metadata.completed_at = datetime.now()
                    task.metadata.success = False
                    task.metadata.error = e
                    completed += 1
                    failed += 1
                    
                    # Update pipeline stats
                    with self.lock:
                        if task.pipeline_name in self.pipeline_stats:
                            if self.pipeline_stats[task.pipeline_name]["in_progress"] > 0:
                                self.pipeline_stats[task.pipeline_name]["in_progress"] -= 1
                            self.pipeline_stats[task.pipeline_name]["failed"] += 1
    
    def _execute_batch(self, pipeline_map: Dict[str, Pipeline], tasks: List[ScheduledTask], group_handle: TaskGroupHandle, batch_size: int = 10) -> None:
        """
        Execute tasks in batches using thread pool
        
        Args:
            pipeline_map: Map of pipeline names to pipeline instances
            tasks: List of tasks to execute
            group_handle: Task group handle
            batch_size: Size of each batch
        """
        if not tasks:
            return
        
        total = len(tasks)
        completed = 0
        success = 0
        failed = 0
        
        # Group tasks by pipeline for more efficient batch processing
        tasks_by_pipeline: Dict[str, List[ScheduledTask]] = {}
        for task in tasks:
            if task.pipeline_name not in tasks_by_pipeline:
                tasks_by_pipeline[task.pipeline_name] = []
            tasks_by_pipeline[task.pipeline_name].append(task)
        
        # Process each pipeline's tasks in batches
        for pipeline_name, pipeline_tasks in tasks_by_pipeline.items():
            pipeline = pipeline_map.get(pipeline_name)
            if not pipeline:
                self.logger.error(f"Pipeline {pipeline_name} not found")
                continue
            
            # Track batch statistics in group handle
            if "batch_stats" not in group_handle.extra:
                group_handle.extra["batch_stats"] = {
                    "total_batches": 0,
                    "completed_batches": 0,
                    "current_batch": 0
                }
            
            # Process in batches
            batch_count = (len(pipeline_tasks) + batch_size - 1) // batch_size
            group_handle.extra["batch_stats"]["total_batches"] += batch_count
            
            for batch_idx in range(0, batch_count):
                batch_start = batch_idx * batch_size
                batch_end = min(batch_start + batch_size, len(pipeline_tasks))
                batch = pipeline_tasks[batch_start:batch_end]
                
                group_handle.extra["batch_stats"]["current_batch"] += 1
                
                # Track batch tasks and futures
                futures = []
                batch_task_map = {}  # future -> task mapping
                
                # Start batch tasks
                for task in batch:
                    task.metadata.started_at = datetime.now()
                    
                    # Update pipeline stats to track in-progress tasks
                    with self.lock:
                        if task.pipeline_name not in self.pipeline_stats:
                            self.pipeline_stats[task.pipeline_name] = {
                                "total": 0,
                                "success": 0,
                                "failed": 0,
                                "in_progress": 0
                            }
                        self.pipeline_stats[task.pipeline_name]["in_progress"] += 1
                    
                    future = self.thread_pool.submit(self._execute_task, pipeline, task)
                    futures.append(future)
                    batch_task_map[future] = task
                
                # Process batch results
                for future in concurrent.futures.as_completed(futures):
                    task = batch_task_map[future]
                    try:
                        future.result()  # This will re-raise any exceptions
                        
                        # Update counters - success case handled in _execute_task
                        completed += 1
                        if task.metadata.success:
                            success += 1
                        else:
                            failed += 1
                            
                    except Exception as e:
                        # Handle unexpected errors (should be caught in _execute_task)
                        self.logger.error(f"Unexpected error in batch task {task.task_id}: {e}")
                        completed += 1
                        failed += 1
                
                # Update batch stats
                group_handle.extra["batch_stats"]["completed_batches"] += 1
        
        # Update final status in group handle
        group_handle.extra["status"] = {
            "total": total, 
            "completed": completed,
            "success": success, 
            "failed": failed
        }
    
    def schedule_pipelines(self, 
                          pipeline_map: Dict[str, Pipeline],
                          pipelines_with_data: Dict[str, List[Any]],
                          group_id: Optional[str] = None,
                          execution_mode: Optional[ExecutionMode] = None) -> str:
        """
        Schedule pipelines for execution
        
        Args:
            pipeline_map: Map of pipeline names to pipeline instances
            pipelines_with_data: Map of pipeline names to lists of data items
            group_id: Optional group ID, generated if not provided
            execution_mode: Execution mode
            
        Returns:
            Group ID
        """
        group_id = group_id or f"group-{uuid.uuid4()}"
        
        # Create task group
        group_handle = TaskGroupHandle(group_id)
        
        # Determine execution mode
        mode = execution_mode or ExecutionMode.SEQUENTIAL
        self.logger.info(f"Scheduling tasks for execution in {mode.name} mode")
        
        # Create tasks
        tasks = []
        total_tasks = 0
        
        for pipeline_name, data_items in pipelines_with_data.items():
            if pipeline_name not in pipeline_map:
                self.logger.error(f"Pipeline {pipeline_name} not found")
                continue
            
            # Initialize results tracking for this pipeline in the group
            if pipeline_name not in group_handle.results_by_pipeline:
                group_handle.results_by_pipeline[pipeline_name] = []
            
            # Create tasks for this pipeline
            pipeline_tasks = []
            for data_item in data_items:
                task_id = f"{group_id}-{pipeline_name}-{uuid.uuid4()}"
                task_metadata = TaskMetadata(task_id, pipeline_name)
                task = ScheduledTask(task_id, pipeline_name, data_item, task_metadata, None)
                
                # Record task in our tracking structures
                with self.lock:
                    self.tasks[task_id] = task
                    
                    if pipeline_name not in self.pipeline_tasks:
                        self.pipeline_tasks[pipeline_name] = []
                    self.pipeline_tasks[pipeline_name].append(task_id)
                    
                    # Update pipeline stats
                    if pipeline_name not in self.pipeline_stats:
                        self.pipeline_stats[pipeline_name] = {
                            "total": 0,
                            "success": 0,
                            "failed": 0,
                            "in_progress": 0
                        }
                    self.pipeline_stats[pipeline_name]["total"] += 1
                
                pipeline_tasks.append(task)
                group_handle.task_ids.append(task_id)
            
            tasks.extend(pipeline_tasks)
            total_tasks += len(pipeline_tasks)
        
        # Store the group handle
        with self.lock:
            self.task_groups[group_id] = group_handle
        
        # Execute based on mode
        if mode == ExecutionMode.SEQUENTIAL:
            self._execute_sequential(pipeline_map, tasks)
        elif mode == ExecutionMode.PARALLEL_THREAD:
            self._execute_parallel_thread(pipeline_map, tasks)
        elif mode == ExecutionMode.PARALLEL_PROCESS:
            self._execute_parallel_process(pipeline_map, tasks)
        elif mode == ExecutionMode.BATCH:
            batch_size = 10  # Could be configurable
            self._execute_batch(pipeline_map, tasks, group_handle, batch_size)
        
        return group_id
    
    def get_task_status(self, task_id: str) -> Optional[Dict[str, Any]]:
        """
        Get status of a task
        
        Args:
            task_id: Task ID
            
        Returns:
            Task status dictionary or None if task not found
        """
        with self.lock:
            if task_id not in self.tasks:
                return None
            
            task = self.tasks[task_id]
            
            return {
                "task_id": task_id,
                "pipeline_name": task.pipeline_name,
                "status": task.metadata.status_flag or "unknown",
                "success": task.metadata.success,
                "created_at": task.metadata.created_at,
                "started_at": task.metadata.started_at,
                "completed_at": task.metadata.completed_at,
                "error": str(task.metadata.error) if task.metadata.error else None,
                "has_result": task.task_id in self.results,
                "extra": task.metadata.extra
            }
    
    def get_group_status(self, group_id: str) -> Optional[Dict[str, Any]]:
        """
        Get status of a task group
        
        Args:
            group_id: Group ID
            
        Returns:
            Group status dictionary or None if group not found
        """
        with self.lock:
            if group_id not in self.task_groups:
                return None
            
            group = self.task_groups[group_id]
            
            # Count tasks by status
            total = len(group.task_ids)
            completed = 0
            success = 0
            failed = 0
            
            for task_id in group.task_ids:
                if task_id in self.tasks:
                    task = self.tasks[task_id]
                    if task.metadata.completed_at:
                        completed += 1
                        if task.metadata.success:
                            success += 1
                        else:
                            failed += 1
            
            return {
                "group_id": group_id,
                "total_tasks": total,
                "completed_tasks": completed,
                "successful_tasks": success,
                "failed_tasks": failed,
                "in_progress": total - completed,
                "is_complete": completed == total,
                "status": "completed" if completed == total else "in_progress",
                "results_by_pipeline": {k: len(v) for k, v in group.results_by_pipeline.items()},
                "extra": group.extra
            }
    
    def get_status_by_pipeline(self, pipeline_name: str) -> Dict[str, Any]:
        """
        Get status of tasks for a pipeline
        
        Args:
            pipeline_name: Name of the pipeline
            
        Returns:
            Status information
        """
        with self.lock:
            # Check if we have pipeline stats
            if pipeline_name in self.pipeline_stats:
                return self.pipeline_stats[pipeline_name]
            
            # If not, calculate stats from tasks
            if pipeline_name not in self.pipeline_tasks:
                return {
                    "total": 0,
                    "success": 0,
                    "failed": 0,
                    "in_progress": 0
                }
            
            task_ids = self.pipeline_tasks[pipeline_name]
            total = len(task_ids)
            success = 0
            failed = 0
            in_progress = 0
            
            for task_id in task_ids:
                if task_id in self.tasks:
                    task = self.tasks[task_id]
                    if not task.metadata.started_at:
                        # Not started
                        pass
                    elif not task.metadata.completed_at:
                        # Started but not completed
                        in_progress += 1
                    elif task.metadata.success:
                        # Completed successfully
                        success += 1
                    else:
                        # Completed with failure
                        failed += 1
            
            # Cache the calculated stats
            self.pipeline_stats[pipeline_name] = {
                "total": total,
                "success": success,
                "failed": failed,
                "in_progress": in_progress
            }
            
            return self.pipeline_stats[pipeline_name]
    
    def get_result_by_pipeline(self, pipeline_name: str) -> List[Any]:
        """
        Get results for a pipeline
        
        Args:
            pipeline_name: Name of the pipeline
            
        Returns:
            List of results
        """
        with self.lock:
            if pipeline_name not in self.pipeline_tasks:
                return []
            
            results = []
            task_ids = self.pipeline_tasks[pipeline_name]
            
            for task_id in task_ids:
                if task_id in self.results:
                    results.append(self.results[task_id])
            
            return results
    
    def get_failures_by_pipeline(self, pipeline_name: str) -> List[Dict[str, Any]]:
        """
        Get failure information for a pipeline
        
        Args:
            pipeline_name: Name of the pipeline
            
        Returns:
            List of failure information dictionaries
        """
        with self.lock:
            if pipeline_name not in self.pipeline_tasks:
                return []
            
            failures = []
            task_ids = self.pipeline_tasks[pipeline_name]
            
            for task_id in task_ids:
                if task_id in self.tasks:
                    task = self.tasks[task_id]
                    if task.metadata.completed_at and not task.metadata.success:
                        failures.append({
                            "task_id": task_id,
                            "error": str(task.metadata.error) if task.metadata.error else "Unknown error",
                            "input_data": task.input_data,
                            "created_at": task.metadata.created_at,
                            "completed_at": task.metadata.completed_at,
                            "extra": task.metadata.extra
                        })
            
            return failures
    
    def shutdown(self) -> None:
        """Shutdown the scheduler and release resources"""
        self.logger.info("Shutting down scheduler")
        
        # Shutdown thread pool
        self.thread_pool.shutdown(wait=True)
        
        # Shutdown process pool if it exists
        if self.process_pool:
            self.logger.info("Shutting down process pool")
            self.process_pool.shutdown(wait=True)
            self.process_pool = None
        
        self.logger.info("Scheduler shutdown complete")