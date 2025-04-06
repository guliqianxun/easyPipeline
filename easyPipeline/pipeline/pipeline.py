from typing import Any, Dict, List, Optional
import logging
import time

from execution.step import Step
from commom.types import PipelineStatus
from commom.metrics import PipelineMetrics, StepMetrics
from pipeline.dag import DAG

class Pipeline:
    """
    A pipeline that orchestrates the execution of steps in a specific order.
    
    The pipeline uses a DAG to manage dependencies between steps and
    ensures they are executed in the correct order. This implementation
    is fully state-driven without callback dependencies.
    """
    
    def __init__(self, 
                 name: str, 
                 description: str = "",
                 logger: Optional[logging.Logger] = None):
        """
        Initialize a pipeline
        
        Args:
            name: Name of the pipeline
            description: Optional description
            logger: Optional logger instance
        """
        self.name = name
        self.description = description
        self.steps: Dict[str, Step] = {}
        self.dag = DAG()
        
        # State tracking
        self.status = PipelineStatus.NOT_STARTED
        self.metrics = PipelineMetrics()
        self.logger = logger or logging.getLogger(f"Pipeline.{self.name}")
        self.results: Dict[str, Any] = {}
        self.error = None
        self.execution_order: List[str] = []
        self.start_time = None
        self.end_time = None
        
        # Step tracking
        self.current_step = None
        self.completed_steps: List[str] = []
        self.failed_step = None
    
    def add_step(self, step: Step, depends_on: Optional[List[str]] = None) -> None:
        """
        Add a step to the pipeline
        
        Args:
            step: Step to add
            depends_on: Optional list of step names this step depends on
        """
        self.steps[step.name] = step
        self.dag.add_step(step.name, depends_on or [])
        self.metrics.step_count += 1
        self.logger.info(f"Added step: {step.name}")
    
    def run(self, data: Any) -> Any:
        """
        Run the pipeline with the given input data
        
        Args:
            data: Input data for the pipeline
            
        Returns:
            Result of the pipeline execution
        """
        # Initialize pipeline state
        self.status = PipelineStatus.RUNNING
        self.metrics.start()
        self.error = None
        self.start_time = time.time()
        self.completed_steps = []
        self.failed_step = None
        self.current_step = None
        self.logger.info(f"Starting pipeline: {self.name}")
        
        # Get the execution order from the DAG
        try:
            self.execution_order = self.dag.topological_sort()
        except Exception as e:
            self.logger.error(f"Failed to determine execution order: {e}")
            self.status = PipelineStatus.FAILED
            self.error = e
            self.end_time = time.time()
            raise
        
        self.logger.info(f"Execution order: {', '.join(self.execution_order)}")
        
        # Initialize results with input data
        current_data = data
        
        # Execute steps in order
        try:
            for step_name in self.execution_order:
                step = self.steps[step_name]
                self.current_step = step_name
                
                self.logger.info(f"Executing step: {step_name}")
                
                # Execute the step
                try:
                    # Update step state and execute
                    step_result = step.execute(current_data)
                    
                    # Update pipeline state with success
                    self.metrics.step_metrics.append(StepMetrics(step.name, step.metrics))
                    self.metrics.successful_steps += 1
                    self.completed_steps.append(step_name)
                    
                    # Store step result
                    self.results[step_name] = step_result

                    # Update current data for next step
                    current_data = step_result
                    
                except Exception as e:
                    # Update pipeline state with failure
                    self.logger.error(f"Step {step_name} failed: {e}")
                    self.metrics.failed_steps += 1
                    self.status = PipelineStatus.FAILED
                    self.error = e
                    self.failed_step = step_name
                    
                    # End timing and re-raise
                    self.end_time = time.time()
                    raise
            
            # Pipeline completed successfully
            self.status = PipelineStatus.COMPLETED
            self.metrics.stop()
            self.end_time = time.time()
            self.current_step = None
            self.logger.info(f"Pipeline {self.name} completed in {self.metrics.duration:.2f}s")
            
            return current_data
            
        except Exception as e:
            # Pipeline failed
            self.status = PipelineStatus.FAILED
            self.error = e
            self.metrics.stop()
            self.end_time = time.time()
            self.logger.error(f"Pipeline {self.name} failed after {self.metrics.duration:.2f}s: {e}")
            raise
    
    def get_metrics(self) -> Dict[str, Any]:
        """
        Get pipeline metrics
        
        Returns:
            Dictionary with pipeline metrics
        """
        return self.metrics.aggregate()
    
    def get_status(self) -> PipelineStatus:
        """
        Get pipeline status
        
        Returns:
            Current pipeline status
        """
        return self.status
    
    def get_error(self) -> Optional[Exception]:
        """
        Get pipeline error
        
        Returns:
            Error that occurred during pipeline execution, or None if no error
        """
        return self.error
    
    def get_progress(self) -> Dict[str, Any]:
        """
        Get current pipeline progress
        
        Returns:
            Dictionary with progress information
        """
        total_steps = len(self.execution_order)
        completed = len(self.completed_steps)
        
        return {
            "total_steps": total_steps,
            "completed_steps": completed,
            "current_step": self.current_step,
            "failed_step": self.failed_step,
            "progress_pct": (completed / total_steps * 100) if total_steps > 0 else 0,
            "elapsed_time": (time.time() - self.start_time) if self.start_time else 0,
            "status": self.status.name
        }
    
    def get_result(self, step_name: Optional[str] = None) -> Any:
        """
        Get the result of a specific step or the final pipeline result
        
        Args:
            step_name: Optional name of step to get result for
            
        Returns:
            Result of specified step or final pipeline result if no step specified
        """
        if step_name:
            return self.results.get(step_name)
        
        # Return the result of the last step in execution order
        if self.execution_order and self.execution_order[-1] in self.results:
            return self.results[self.execution_order[-1]]
        return None

    def get_state_snapshot(self) -> Dict[str, Any]:
        """
        Get a complete snapshot of the pipeline's current state
        
        Returns:
            Dictionary containing all pipeline state information
        """
        return {
            "name": self.name,
            "status": self.status.name,
            "progress": self.get_progress(),
            "execution_order": self.execution_order,
            "completed_steps": self.completed_steps,
            "current_step": self.current_step,
            "failed_step": self.failed_step,
            "metrics": self.get_metrics(),
            "error": str(self.error) if self.error else None,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "duration": (self.end_time - self.start_time) if self.end_time and self.start_time else None
        }

    def execute(self, data: Any, timeout: Optional[float] = None) -> Any:
        """
        Compatibility method for old API - executes the pipeline
        
        Args:
            data: Input data for the pipeline
            timeout: Optional timeout (ignored in this implementation)
            
        Returns:
            Result of the pipeline execution
        """
        self.logger.info(f"Using compatibility method 'execute' - consider migrating to 'run'")
        return self.run(data)
