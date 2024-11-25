# -*- coding: UTF-8 -*-
'''
@Author  : Zhiheng Liu
@Email   : visitorindark@gmail.com
@CreateDate    : 2024/11/11 15:35 
@ModifyDate    : 2024/11/11 15:35
@Desc    : pipeline 执行最小单元 
'''
import logging
from .step import PipelineStep
from .types import StepStatus
from typing import List, Dict, Optional, Any

class Pipeline:
    def __init__(self, name: str, description: str = "", logger: logging.Logger = None):
        self.name = name
        self.description = description
        self.steps: List[PipelineStep] = []
        self.logger = logger if logger is not None else logging.getLogger(f"pipeline.{name}")
        self._validate_dependencies = True
        self.result = None
        self.intermediate_results = {}

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

    def execute(self, data: Any) -> Any:
        """Execute all steps in the pipeline"""
        self.logger.info(f"Starting pipeline execution: {self.name}")
        current_data = data

        for step in self.steps:
            try:
                self.logger.info(f"Executing step: {step.name}")
                current_data = step.execute(current_data)
                self.intermediate_results[step.name] = current_data

                # Cleanup dependencies of the current step
                for dep in step.dependencies:
                    self._cleanup_intermediate_result(dep)

            except Exception as e:
                self.logger.error(f"Pipeline failed at step {step.name}: {str(e)}")
                raise

        self.logger.info("Pipeline execution completed successfully")
        self.result = current_data
        return current_data

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

