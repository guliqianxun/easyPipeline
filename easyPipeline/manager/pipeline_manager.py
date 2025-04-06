import logging
import os
import uuid
from multiprocessing import freeze_support
from typing import Dict, Any, List, Optional

from pipeline.pipeline import Pipeline
from manager.task_scheduler import TaskScheduler, ExecutionMode

class PipelineManager:
    """
    Manager for pipeline execution with state-driven architecture
    """
    
    def __init__(
        self,
        name: str = "pipeline_manager",
        execution_mode: ExecutionMode = ExecutionMode.SEQUENTIAL,
        max_workers: Optional[int] = None,
        logger: Optional[logging.Logger] = None,
    ):
        """
        Initialize the pipeline manager
        
        Args:
            name: Manager name
            execution_mode: Default execution mode
            max_workers: Maximum number of workers for parallel execution
            logger: Optional logger instance
        """
        self.name = name
        self.execution_mode = execution_mode
        self.max_workers = max_workers or os.cpu_count()
        self.logger = logger or logging.getLogger(f"PipelineManager.{name}")

        # Store registered pipelines
        self.pipelines: Dict[str, Pipeline] = {}
        
        # Initialize task scheduler
        self.task_scheduler = TaskScheduler(
            max_workers=self.max_workers,
            logger=self.logger
        )
        
        # Initialize multiprocessing support if needed
        if execution_mode in [ExecutionMode.PARALLEL_PROCESS, ExecutionMode.BATCH]:
            freeze_support()

    def register_pipeline(self, pipeline: Pipeline) -> None:
        """
        Register a pipeline
        
        Args:
            pipeline: Pipeline to register
        """
        if pipeline.name in self.pipelines:
            self.logger.warning(f"Pipeline {pipeline.name} already registered, overwriting")
        
        self.pipelines[pipeline.name] = pipeline
        self.logger.info(f"Registered pipeline: {pipeline.name}")

    def execute(
        self,
        pipelines_with_data: Dict[str, List[Any]],
        execution_mode: Optional[ExecutionMode] = None
    ) -> str:
        """
        Execute pipelines with data
        
        Args:
            pipelines_with_data: Dictionary mapping pipeline names to lists of input data
            execution_mode: Optional execution mode override
            
        Returns:
            Group ID for tracking execution
        """
        if not pipelines_with_data:
            raise ValueError("No pipelines specified for execution")

        exec_mode = execution_mode or self.execution_mode

        # Validate pipelines
        for pipeline_name, data_list in pipelines_with_data.items():
            if pipeline_name not in self.pipelines:
                raise ValueError(f"Pipeline '{pipeline_name}' not registered")
            
        # Create a mapping from pipeline_name to pipeline reference
        pipeline_map = {name: self.pipelines[name] for name in pipelines_with_data}

        # Generate group ID
        group_id = f"group-{uuid.uuid4()}"

        # Schedule pipeline executions
        return self.task_scheduler.schedule_pipelines(
            pipeline_map=pipeline_map,
            pipelines_with_data=pipelines_with_data,
            group_id=group_id,
            execution_mode=exec_mode
        )

    def get_status(self, pipeline_name: str) -> Dict[str, Any]:
        """
        Get status of tasks for a pipeline
        
        Args:
            pipeline_name: Name of the pipeline
            
        Returns:
            Status information
        """
        if pipeline_name not in self.pipelines:
            self.logger.error(f"Pipeline {pipeline_name} not registered")
            raise ValueError(f"Pipeline {pipeline_name} not registered")
        
        return self.task_scheduler.get_status_by_pipeline(pipeline_name)
    
    def get_result(self, pipeline_name: str) -> List[Any]:
        """
        Get results for a pipeline
        
        Args:
            pipeline_name: Name of the pipeline
            
        Returns:
            List of results
        """
        if pipeline_name not in self.pipelines:
            self.logger.error(f"Pipeline {pipeline_name} not registered")
            raise ValueError(f"Pipeline {pipeline_name} not registered")
        
        return self.task_scheduler.get_result_by_pipeline(pipeline_name)
    
    def get_failures(self, pipeline_name: str) -> List[Dict[str, Any]]:
        """
        Get failure information for a pipeline
        
        Args:
            pipeline_name: Name of the pipeline
            
        Returns:
            List of failure information dictionaries
        """
        if pipeline_name not in self.pipelines:
            self.logger.error(f"Pipeline {pipeline_name} not registered")
            raise ValueError(f"Pipeline {pipeline_name} not registered")
        
        return self.task_scheduler.get_failures_by_pipeline(pipeline_name)
    
    def get_metrics(self, pipeline_name: str) -> Dict[str, Any]:
        """
        Get metrics for a pipeline
        
        Args:
            pipeline_name: Name of the pipeline
            
        Returns:
            Pipeline metrics
        """
        if pipeline_name not in self.pipelines:
            self.logger.error(f"Pipeline {pipeline_name} not registered")
            raise ValueError(f"Pipeline {pipeline_name} not registered")
        
        return self.pipelines[pipeline_name].get_metrics()
    
    def get_group_status(self, group_id: str) -> Optional[Dict[str, Any]]:
        """
        Get status of a task group
        
        Args:
            group_id: Group ID
            
        Returns:
            Group status dictionary or None if group not found
        """
        return self.task_scheduler.get_group_status(group_id)
    
    def shutdown(self) -> None:
        """Shutdown the manager and release resources"""
        self.task_scheduler.shutdown()
