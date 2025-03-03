# -*- coding: UTF-8 -*-
'''
@Author  : Zhiheng Liu
@Email   : visitorindark@gmail.com
@CreateDate    : 2024/11/11 15:35 
@ModifyDate    : 2024/11/11 15:35
@Desc    : pipeline 执行最小单元 
'''
import logging
from datetime import datetime, timedelta
from .step import PipelineStep
from .types import StepStatus, MetricsData, ProgressCallback
from typing import List, Dict, Optional, Any

class Pipeline:
    def __init__(self, name: str, description: str = "", logger: logging.Logger = None, 
                 callbacks: Optional[List[ProgressCallback]] = None):
        self.name = name
        self.description = description
        self.steps: List[PipelineStep] = []
        self.logger = logger if logger is not None else logging.getLogger(f"pipeline.{name}")
        self._validate_dependencies = True
        self.metrics = MetricsData()
        self.result = None
        self.intermediate_results = {}
        self.reference_count = {}
        self.callbacks = callbacks or []

    def add_step(self, step: PipelineStep) -> None:
        """Add a processing step to the pipeline"""
        if self._validate_dependencies:
            self._verify_dependencies(step)
        self.steps.append(step)

        # Initialize reference counts for dependencies
        for dep in step.dependencies:
            self.reference_count[dep] = self.reference_count.get(dep, 0) + 1

        self.logger.info(f"Added step: {step.name}")

    def logger_set(self, logger: logging.Logger):
        self.logger = logger
        
    def update_step(self, old_step_name: str, new_step: PipelineStep) -> None:
        """Update an existing step in the pipeline"""
        # Find the index of the old step
        old_step_index = next((i for i, step in enumerate(self.steps) if step.name == old_step_name), None)
        
        if old_step_index is None:
            raise ValueError(f"Step {old_step_name} not found in the pipeline")

        # Verify dependencies of the new step
        if self._validate_dependencies:
            self._verify_dependencies(new_step)

        # Replace the old step with the new step
        old_step = self.steps[old_step_index]
        self.steps[old_step_index] = new_step

        # Update reference counts for dependencies
        for dep in old_step.dependencies:
            self.reference_count[dep] -= 1
            if self.reference_count[dep] <= 0:
                del self.reference_count[dep]

        for dep in new_step.dependencies:
            self.reference_count[dep] = self.reference_count.get(dep, 0) + 1

        self.logger.info(f"Replaced step: {old_step_name} with {new_step.name}")
        
    def _verify_dependencies(self, step: PipelineStep) -> None:
        """Verify that all dependencies are met"""
        available_steps = {s.name for s in self.steps}
        missing_deps = set(step.dependencies) - available_steps
        if missing_deps:
            raise ValueError(f"Step {step.name} has unmet dependencies: {missing_deps}")

    def _cleanup_intermediate_result(self, step_name: str) -> None:
        """Decrement reference count and delete intermediate result if no longer needed"""
        if step_name in self.reference_count:
            self.reference_count[step_name] -= 1
            if self.reference_count[step_name] <= 0:
                self.logger.info(f"Deleting intermediate result for step: {step_name}")
                del self.intermediate_results[step_name]
                del self.reference_count[step_name]

    def log_pipeline_info(self) -> None:
        """Log pipeline information and metrics"""
        self.logger.info(f"Pipeline: {self.name}")
        self.logger.info(f"Description: {self.description}")
        self.logger.info(f"Total steps: {len(self.steps)}")
        if self.metrics.duration_seconds is not None:
            self.logger.info(f"Duration: {self.metrics.duration_seconds:.2f}s")

    def execute(self, data: Any, timeout: Optional[float] = None) -> Any:
        """Execute all steps in the pipeline with optional timeout and callbacks"""
        self.logger.info(f"Starting pipeline execution: {self.name}")
        self.metrics.start_time = datetime.now()
        current_data = data
        
        # Notify callbacks of pipeline start
        for callback in self.callbacks:
            try:
                callback.on_pipeline_start(self.name, data)
            except Exception as e:
                self.logger.warning(f"Callback error on pipeline start: {e}")

        # If timeout is specified, use it
        if timeout is not None:
            end_time = self.metrics.start_time + timedelta(seconds=timeout)
        else:
            end_time = None

        try:
            for step in self.steps:
                try:
                    self.logger.info(f"Executing step: {step.name}")
                    
                    # Notify callbacks of step start
                    for callback in self.callbacks:
                        try:
                            callback.on_step_start(self.name, step.name)
                        except Exception as e:
                            self.logger.warning(f"Callback error on step start: {e}")
                    
                    # Check for timeout before each step
                    if end_time is not None and datetime.now() > end_time:
                        raise TimeoutError(f"Pipeline execution timed out after {timeout} seconds")
                        
                    step_start_time = datetime.now()
                    current_data = step.execute(current_data)
                    step_duration = (datetime.now() - step_start_time).total_seconds()
                    
                    # Notify callbacks of step completion
                    for callback in self.callbacks:
                        try:
                            callback.on_step_complete(self.name, step.name, step_duration)
                        except Exception as e:
                            self.logger.warning(f"Callback error on step complete: {e}")
                            
                    self.intermediate_results[step.name] = current_data

                    # Cleanup dependencies of the current step
                    for dep in step.dependencies:
                        self._cleanup_intermediate_result(dep)

                except Exception as e:
                    self.logger.error(f"Pipeline failed at step {step.name}: {str(e)}")
                    
                    # Notify callbacks of step error
                    for callback in self.callbacks:
                        try:
                            callback.on_step_error(self.name, step.name, e)
                        except Exception as callback_e:
                            self.logger.warning(f"Callback error on step error: {callback_e}")
                            
                    raise
                    
            self.metrics.end_time = datetime.now()
            self.metrics.duration_seconds = (self.metrics.end_time - self.metrics.start_time).total_seconds()
            self.logger.info(f"{self.name} execution completed successfully in {self.metrics.duration_seconds:.2f} seconds")
            self.result = current_data
            
            # Notify callbacks of pipeline completion
            for callback in self.callbacks:
                try:
                    callback.on_pipeline_complete(self.name, current_data, self.metrics.duration_seconds)
                except Exception as e:
                    self.logger.warning(f"Callback error on pipeline complete: {e}")
                    
            return current_data
            
        except Exception as e:
            self.metrics.end_time = datetime.now()
            self.metrics.duration_seconds = (self.metrics.end_time - self.metrics.start_time).total_seconds()
            
            # Notify callbacks of pipeline error
            for callback in self.callbacks:
                try:
                    callback.on_pipeline_error(self.name, e, self.metrics.duration_seconds)
                except Exception as callback_e:
                    self.logger.warning(f"Callback error on pipeline error: {callback_e}")
                    
            raise

    def get_status(self) -> Dict:
        """Get the current status of all steps"""
        return {
            "pipeline_name": self.name,
            "total_steps": len(self.steps),
            "completed_steps": sum(
                1 for step in self.steps if step.status == StepStatus.COMPLETED
            ),
            "failed_steps": sum(
                1 for step in self.steps if step.status == StepStatus.FAILED
            ),
            "step_statuses": [
                {
                    "name": step.name,
                    "status": step.status.value,
                    "metrics": {
                        "duration": step.metrics.duration_seconds,
                        "custom_metrics": step.metrics.custom_metrics,
                    },
                }
                for step in self.steps
            ],
        }

    def get_result(self) -> Any:
        return self.result

    def get_intermediate_result(self, step_name: str) -> Any:
        """Get intermediate result"""
        return self.intermediate_results.get(step_name)

